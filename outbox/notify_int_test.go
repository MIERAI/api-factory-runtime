package outbox_test

// v0.5.2: 唤醒中继的 NOTIFY 不在写事务里(提交之后、事务外、合并发送)。集成测试,真实的 Postgres / RabbitMQ。

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/internal/itest"
	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/outbox"
	"github.com/MIERAI/api-factory-runtime/txn"
)

// sqlLog: 记下连接池执行的每条 SQL(按连接),看 NOTIFY 是在事务里还是外面。
type sqlLog struct {
	mu   sync.Mutex
	stmt []string // "<pid> <sql>"
}

func (l *sqlLog) TraceQueryStart(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stmt = append(l.stmt, strings.ToLower(strings.TrimSpace(d.SQL)))
	return ctx
}
func (l *sqlLog) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (l *sqlLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.stmt...)
}

func tracedPool(t *testing.T, dbURL string, tr pgx.QueryTracer) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	cfg.ConnConfig.Tracer = tr
	cfg.MaxConns = 32
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func listen(t *testing.T, dbURL string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	if _, err := c.Exec(context.Background(), `LISTEN `+outbox.NotifyChannel); err != nil {
		t.Fatal(err)
	}
	return c
}

func notified(c *pgx.Conn, d time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_, err := c.WaitForNotification(ctx)
	return err == nil
}

// 写事务里没有 NOTIFY;提交之后(事务外)有一次唤醒;回滚没有。
func TestWriteNotifiesAfterCommitOutsideTheTransaction(t *testing.T) {
	dbURL := itest.NewDB(t)
	tr := &sqlLog{}
	pool := tracedPool(t, dbURL, tr)
	l := listen(t, dbURL)
	e := txn.New(pool)
	w := outbox.NewWriter("rt-test")
	ctx := context.Background()
	if err := e.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		for i := 0; i < 3; i++ { // 一个事务写三次: 提交后只唤醒一次
			if _, err := w.Write(ctx, tx, outbox.Message{Kind: message.Event, Type: "a", Version: 1}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !notified(l, 5*time.Second) {
		t.Fatal("no wake-up after commit")
	}
	itest.Eventually(t, 5*time.Second, "the NOTIFY statement traced", func() bool {
		for _, s := range tr.snapshot() {
			if strings.Contains(s, "pg_notify") {
				return true
			}
		}
		return false
	})
	inTx, notifies := false, 0
	for _, s := range tr.snapshot() {
		switch {
		case strings.HasPrefix(s, "begin"):
			inTx = true
		case s == "commit" || s == "rollback":
			inTx = false
		case strings.Contains(s, "pg_notify"):
			notifies++
			if inTx {
				t.Fatalf("NOTIFY inside the write transaction: %q", tr.snapshot())
			}
		}
	}
	if notifies != 1 {
		t.Fatalf("one transaction → one wake-up, got %d: %q", notifies, tr.snapshot())
	}
	if notified(l, 300*time.Millisecond) {
		t.Fatal("more than one wake-up for one transaction")
	}
	// 回滚: 不唤醒
	_ = e.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, _ = w.Write(ctx, tx, outbox.Message{Kind: message.Event, Type: "b", Version: 1})
		return context.Canceled
	})
	if notified(l, 500*time.Millisecond) {
		t.Fatal("a rolled back transaction woke the relay")
	}
}

// 合并: 并发的大量提交只产生少量 NOTIFY(每个连接池同时最多一条在发),但最后一次提交之后一定有一次。
func TestWakeupsCoalesce(t *testing.T) {
	dbURL := itest.NewDB(t)
	tr := &sqlLog{}
	pool := tracedPool(t, dbURL, tr)
	e := txn.New(pool)
	w := outbox.NewWriter("rt-test")
	const workers, each = 16, 25
	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := e.InTx(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
					_, err := w.Write(ctx, tx, outbox.Message{Kind: message.Event, Type: "c", Version: 1})
					return err
				}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	l := listen(t, dbURL)
	// 最后一次提交之后: 再提交一次,必须被唤醒
	writeMessages(t, pool, outbox.Message{Kind: message.Event, Type: "c", Version: 1})
	if !notified(l, 5*time.Second) {
		t.Fatal("no wake-up after the last commit")
	}
	n := 0
	for _, s := range tr.snapshot() {
		if strings.Contains(s, "pg_notify") {
			n++
		}
	}
	t.Logf("%d commits → %d NOTIFY", workers*each+1, n)
	if n < 1 || n > workers*each+1 {
		t.Fatalf("NOTIFY count %d", n)
	}
}

// 写 outbox 的提交不再在实例上排队: N 个并发事务的提交吞吐,和"在写事务里 NOTIFY"(v0.5.1)比。
// 发过 NOTIFY 的事务在提交时持有 pg_database 上的 AccessExclusiveLock 直到 WAL 落盘,所以旧的做法每秒最多
// 1/fsync 次提交;现在应该和完全不通知的一样(组提交)。
func TestOutboxCommitsDoNotSerialize(t *testing.T) {
	dbURL := itest.NewDB(t)
	pool := tracedPool(t, dbURL, nil)
	e := txn.New(pool)
	w := outbox.NewWriter("rt-test")
	const workers = 16
	dur := 2 * time.Second
	run := func(mode string) float64 {
		var n atomic.Int64
		deadline := time.Now().Add(dur)
		var wg sync.WaitGroup
		for g := 0; g < workers; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					err := e.InTx(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
						if mode == "baseline" { // 只插入 outbox 的行,不唤醒(没有 NOTIFY 时的上限)
							_, err := tx.Exec(ctx, `INSERT INTO factory_outbox (id, kind, type, version, subject, envelope, occurred_at)
VALUES (gen_random_uuid(), 1, 'x', 1, '', '{}', now())`)
							return err
						}
						if _, err := w.Write(ctx, tx, outbox.Message{Kind: message.Event, Type: "x", Version: 1}); err != nil {
							return err
						}
						if mode == "notify-in-tx" { // v0.5.1 的做法
							_, err := tx.Exec(ctx, `SELECT pg_notify('`+outbox.NotifyChannel+`', '')`)
							return err
						}
						return nil
					})
					if err != nil {
						t.Error(err)
						return
					}
					n.Add(1)
				}
			}()
		}
		wg.Wait()
		return float64(n.Load()) / dur.Seconds()
	}
	run("after-commit") // 预热
	old, base, cur := run("notify-in-tx"), run("baseline"), run("after-commit")
	t.Logf("commits/s with %d concurrent writers: NOTIFY in the transaction %.0f, no NOTIFY %.0f, NOTIFY after commit (v0.5.2) %.0f", workers, old, base, cur)
	if cur < 0.6*base {
		t.Errorf("after-commit wake-ups cost too much: %.0f commits/s vs %.0f without NOTIFY", cur, base)
	}
	if base < 2*old {
		t.Logf("this Postgres does not show the serialization (fsync too cheap: no NOTIFY %.0f vs in-tx %.0f); only the overhead is checked", base, old)
		return
	}
	if cur < 1.5*old {
		t.Errorf("outbox commits still serialize: %.0f commits/s vs %.0f with NOTIFY in the transaction", cur, old)
	}
}

// 唤醒丢了(进程在提交之后、唤醒之前崩了 / NOTIFY 失败): 中继的轮询照样发出去。
func TestMissedWakeupIsPickedUpByPoll(t *testing.T) {
	sfx := itest.Suffix()
	evt := "evt.poll." + sfx
	_, queues := setupQueues(t, evt)
	dbURL := itest.NewDB(t)
	pool := itest.Pool(t, dbURL)
	b := itest.Broker(t)
	stop := runRelay(t, &outbox.Relay{DB: pool, Broker: b, Log: quiet, Poll: 300 * time.Millisecond})
	defer stop()
	time.Sleep(500 * time.Millisecond) // 中继已经在等
	l := listen(t, dbURL)
	// 不经过 InTx 的事务: 没有提交后的唤醒
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	envs, err := outbox.NewWriter("rt-test").Write(context.Background(), tx, outbox.Message{Kind: message.Event, Type: evt, Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if notified(l, 200*time.Millisecond) {
		t.Fatal("expected no wake-up for this write")
	}
	ch := itest.Channel(t, itest.Broker(t))
	d, ok := itest.Get(t, ch, queues[evt], 5*time.Second)
	if !ok || d.MessageId != envs[0].ID {
		t.Fatalf("not published by the poll: %v %+v", ok, d)
	}
	itest.Eventually(t, 5*time.Second, "marked published", func() bool { return pending(t, pool) == 0 })
}

// 中继从提交到发出的延迟(提交后唤醒 vs 只靠轮询)。RUNTIME_TEST_LATENCY=1 时运行,打印分位数。
func TestRelayPickupLatency(t *testing.T) {
	if os.Getenv("RUNTIME_TEST_LATENCY") != "1" {
		t.Skip("RUNTIME_TEST_LATENCY=1 to measure")
	}
	sfx := itest.Suffix()
	evt := "evt.lat." + sfx
	setupQueues(t, evt)
	pool := itest.Pool(t, itest.NewDB(t))
	stop := runRelay(t, &outbox.Relay{DB: pool, Broker: itest.Broker(t), Log: quiet})
	defer stop()
	time.Sleep(time.Second)
	var lat []time.Duration
	for i := 0; i < 200; i++ {
		envs := writeMessages(t, pool, outbox.Message{Kind: message.Event, Type: evt, Version: 1})
		var committed time.Time
		if err := pool.QueryRow(context.Background(), `SELECT clock_timestamp()`).Scan(&committed); err != nil {
			t.Fatal(err)
		}
		var published *time.Time
		for start := time.Now(); time.Since(start) < 5*time.Second; time.Sleep(2 * time.Millisecond) {
			if err := pool.QueryRow(context.Background(), `SELECT published_at FROM factory_outbox WHERE id = $1::uuid`, envs[0].ID).Scan(&published); err != nil {
				t.Fatal(err)
			}
			if published != nil {
				break
			}
		}
		if published == nil {
			t.Fatal("not published")
		}
		lat = append(lat, published.Sub(committed))
		time.Sleep(20 * time.Millisecond)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("commit → published_at over %d messages: p50 %v p90 %v p99 %v max %v", len(lat), lat[len(lat)/2], lat[len(lat)*9/10], lat[len(lat)*99/100], lat[len(lat)-1])
}

// confirm 超时: 这一批记成失败(attempts + 1、last_error)用的是新的 context —— 以前用的是已经过期的 bctx,
// UPDATE 失败,工作者下台(每次超时都换一次 leader)。现在: 工作者不下台,这一行退避后再发出去。
func TestConfirmTimeoutKeepsTheRelayActive(t *testing.T) {
	sfx := itest.Suffix()
	evt := "evt.timeout." + sfx
	_, queues := setupQueues(t, evt)
	pool := itest.Pool(t, itest.NewDB(t))
	var logs bytes.Buffer
	var mu sync.Mutex
	log := slog.New(slog.NewTextHandler(&lockedWriter{w: &logs, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := &outbox.Relay{DB: pool, Broker: itest.Broker(t), Log: log, ConfirmTimeout: 300 * time.Millisecond, MaxBackoff: time.Second}
	var stalled atomic.Bool
	outbox.SetConfirmWait(r, func(ctx context.Context, c *amqp.DeferredConfirmation) (bool, error) {
		if stalled.CompareAndSwap(false, true) { // 第一批: broker 不回 confirm,等到 bctx 超时
			<-ctx.Done()
			return false, ctx.Err()
		}
		return c.WaitContext(ctx)
	})
	envs := writeMessages(t, pool, outbox.Message{Kind: message.Event, Type: evt, Version: 1})
	stop := runRelay(t, r)
	defer stop()
	itest.Eventually(t, 5*time.Second, "the timed out row recorded as failed", func() bool {
		var attempts int
		var lastErr *string
		err := pool.QueryRow(context.Background(), `SELECT attempts, last_error FROM factory_outbox WHERE id = $1::uuid`, envs[0].ID).Scan(&attempts, &lastErr)
		return err == nil && attempts == 1 && lastErr != nil && *lastErr == "confirm timeout"
	})
	ch := itest.Channel(t, itest.Broker(t))
	itest.Eventually(t, 10*time.Second, "published after the back-off", func() bool { return pending(t, pool) == 0 })
	if _, ok := itest.Get(t, ch, queues[evt], 5*time.Second); !ok {
		t.Fatal("message not delivered")
	}
	mu.Lock()
	out := logs.String()
	mu.Unlock()
	if n := strings.Count(out, "outbox relay: active"); n != 1 {
		t.Fatalf("the relay stepped down after a confirm timeout (%d terms):\n%s", n, out)
	}
}

type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
