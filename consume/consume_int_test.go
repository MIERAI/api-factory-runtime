package consume_test

// 集成测试: 真实的 Postgres(RUNTIME_TEST_DB)和 RabbitMQ(RUNTIME_TEST_AMQP),没有设置时跳过。

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/broker"
	"github.com/MIERAI/api-factory-runtime/consume"
	"github.com/MIERAI/api-factory-runtime/internal/itest"
	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/metrics"
	"github.com/MIERAI/api-factory-runtime/outbox"
	"github.com/MIERAI/api-factory-runtime/topology"
	"github.com/MIERAI/api-factory-runtime/txn"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// 测试的重试策略: 和默认的同样的形状(3 级,最多 5 次),时间缩短。
var fastRetry = topology.RetryPolicy{Tiers: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}, MaxAttempts: 5}

type env struct {
	t      *testing.T
	dbURL  string
	pool   *pgxpool.Pool
	b      *broker.Conn
	ch     *amqp.Channel
	typ    string
	queue  string
	spec   topology.Spec
	writer *outbox.Writer
}

// setup: 新的数据库(平台表 + effects 表)、一个事件类型和它的队列(重试 / DLQ)。
func setup(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, dbURL: itest.NewDB(t)}
	e.pool = itest.Pool(t, e.dbURL)
	if _, err := e.pool.Exec(context.Background(), `CREATE TABLE effects (k text PRIMARY KEY, n int NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	e.b = itest.Broker(t)
	e.ch = itest.Channel(t, e.b)
	e.typ = "evt.test." + itest.Suffix()
	e.queue = topology.QueueName("rt-consumer", e.typ)
	e.spec = topology.Spec{Retry: fastRetry, Queues: []topology.Queue{{Name: e.queue, Subscriptions: []topology.Subscription{{Kind: message.Event, Type: e.typ}}}}}
	if err := topology.Declare(e.ch, e.spec); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if ch, err := e.b.Channel(context.Background()); err == nil {
			_ = topology.Delete(ch, e.spec)
			_ = ch.Close()
		}
	})
	e.writer = outbox.NewWriter("rt-test")
	return e
}

// effect: 处理器的效果(故意不幂等: 每执行一次 n + 1,去重失效就看得出来)。
func effect(ctx context.Context, tx pgx.Tx, k string) error {
	if _, err := tx.Exec(ctx, `INSERT INTO effects (k, n) VALUES ($1, 1) ON CONFLICT (k) DO UPDATE SET n = effects.n + 1`, k); err != nil {
		return err
	}
	return txn.AfterWrite(ctx, "effects") // 生成的代码用 txn.RunIn,写操作计入故障注入;这里手写的 SQL 照样计入
}

func (e *env) effects(k string) int {
	var n int
	err := e.pool.QueryRow(context.Background(), `SELECT n FROM effects WHERE k = $1`, k).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) envelope(subject string) message.Envelope {
	return message.Envelope{ID: message.NewID(), Type: e.typ, Version: 1, OccurredAt: time.Now(), Source: "rt-test", Subject: subject, Data: []byte(`{}`)}
}

// publish: 直接发到主队列(默认交换机)。
func (e *env) publish(body []byte, id string, headers amqp.Table) {
	e.t.Helper()
	if err := e.ch.PublishWithContext(context.Background(), "", e.queue, false, false, amqp.Publishing{
		ContentType: message.ContentType, DeliveryMode: amqp.Persistent, MessageId: id, Type: e.typ, Headers: headers, Body: body,
	}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) publishEnv(m message.Envelope, headers amqp.Table) {
	e.t.Helper()
	b, err := m.Encode()
	if err != nil {
		e.t.Fatal(err)
	}
	e.publish(b, m.ID, headers)
}

func (e *env) consumer(h consume.Handler) *consume.Consumer {
	return &consume.Consumer{Queue: e.queue, Accept: []consume.Accept{{Type: e.typ, Versions: []int{1}}}, Handler: h,
		Exec: txn.New(e.pool), Broker: e.b, Retry: fastRetry, Log: quiet, DLQCheckEvery: 100 * time.Millisecond}
}

func run(t *testing.T, c *consume.Consumer) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("consumer: %v", err)
				}
			case <-time.After(15 * time.Second):
				t.Error("consumer did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func (e *env) processed() (n int) {
	if err := e.pool.QueryRow(context.Background(), `SELECT count(*) FROM factory_processed_messages`).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// 端到端 + 去重: 写操作 → outbox → 中继 → 队列 → 处理器(它再发一条后续消息);同一条消息(同 id)再投两次 → 只生效一次。
func TestEndToEndAndDedup(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	follow := "evt.follow." + itest.Suffix()
	c := e.consumer(func(ctx context.Context, tx pgx.Tx, m message.Envelope) error {
		if err := effect(ctx, tx, m.Subject); err != nil {
			return err
		}
		_, err := e.writer.Write(ctx, tx, outbox.Message{Kind: message.Event, Type: follow, Version: 1, Subject: m.Subject})
		return err
	})
	run(t, c)
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var sent []message.Envelope
	if err := txn.New(e.pool).InTx(message.WithTraceparent(ctx, tp), func(ctx context.Context, tx pgx.Tx) error {
		var err error
		sent, err = e.writer.Write(ctx, tx, outbox.Message{Kind: message.Event, Type: e.typ, Version: 1, Subject: "k1"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	relayCtx, stopRelay := context.WithCancel(ctx)
	relayDone := make(chan struct{})
	go func() { _ = (&outbox.Relay{DB: e.pool, Broker: e.b, Log: quiet}).Run(relayCtx); close(relayDone) }()
	defer func() { stopRelay(); <-relayDone }()
	itest.Eventually(t, 10*time.Second, "message handled", func() bool { return e.effects("k1") == 1 })

	// 处理器发出的后续消息: 同一个事务里写进 outbox,traceparent 是收到的消息的子 span
	var followEnv string
	itest.Eventually(t, 5*time.Second, "follow-up message in the outbox", func() bool {
		return e.pool.QueryRow(ctx, `SELECT envelope::text FROM factory_outbox WHERE type = $1`, follow).Scan(&followEnv) == nil
	})
	fe, _ := message.Decode([]byte(followEnv))
	if fe.Traceparent == "" || fe.Traceparent[:36] != tp[:36] || fe.Traceparent == tp {
		t.Errorf("follow-up traceparent %q: want a child span of %q", fe.Traceparent, tp)
	}

	// 重复投递(同一个 id,例: 中继在标记之前崩溃)
	e.publishEnv(sent[0], nil)
	e.publishEnv(sent[0], amqp.Table{"x-extra": "redelivered by hand"})
	itest.Eventually(t, 10*time.Second, "duplicates skipped", func() bool { return metrics.MessageCount(e.queue, "duplicate") == 2 })
	if n := e.effects("k1"); n != 1 {
		t.Fatalf("duplicate delivery must have one effect, got %d", n)
	}
	var follows int
	_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM factory_outbox WHERE type = $1`, follow).Scan(&follows)
	if follows != 1 || e.processed() != 1 || metrics.MessageCount(e.queue, "ok") != 1 {
		t.Fatalf("duplicates must not emit messages: follows=%d processed=%d ok=%d", follows, e.processed(), metrics.MessageCount(e.queue, "ok"))
	}
	// 同内容、新 id = 新消息(去重只认 id;语义层的幂等是处理器的事,E8)
	again := e.envelope("k1")
	e.publishEnv(again, nil)
	itest.Eventually(t, 10*time.Second, "new id handled", func() bool { return e.effects("k1") == 2 })
}

// 重试: 暂时性错误按分级的 TTL 队列重试(间隔 100/200/400/400ms),第 5 次仍失败 → DLQ(带错误、attempt = 5);
// 中途恢复 → 只生效一次。
func TestRetryTiersAndDLQ(t *testing.T) {
	e := setup(t)
	var mu sync.Mutex
	calls := map[string][]time.Time{}
	c := e.consumer(func(ctx context.Context, tx pgx.Tx, m message.Envelope) error {
		mu.Lock()
		calls[m.Subject] = append(calls[m.Subject], time.Now())
		n := len(calls[m.Subject])
		mu.Unlock()
		if err := effect(ctx, tx, m.Subject); err != nil {
			return err
		}
		switch {
		case m.Subject == "always":
			return errors.New("downstream unavailable")
		case m.Subject == "twice" && n <= 2:
			return errors.New("flaky")
		case m.Subject == "panic" && n == 1:
			panic("bug in a pure function")
		}
		return nil
	})
	run(t, c)
	always := e.envelope("always")
	e.publishEnv(always, nil)
	e.publishEnv(e.envelope("twice"), nil)
	e.publishEnv(e.envelope("panic"), nil)

	dlq := topology.DLQ(e.queue)
	d, ok := itest.Get(t, e.ch, dlq, 15*time.Second)
	if !ok {
		t.Fatal("exhausted message not in the DLQ")
	}
	attempt, _ := message.HeaderInt(d.Headers, message.HeaderAttempt)
	if d.MessageId != always.ID || attempt != 5 || message.HeaderString(d.Headers, message.HeaderOutcome) != "exhausted" ||
		!strings.Contains(message.HeaderString(d.Headers, message.HeaderError), "downstream unavailable") {
		t.Fatalf("DLQ message: id=%s headers=%v", d.MessageId, d.Headers)
	}
	mu.Lock()
	ts := calls["always"]
	mu.Unlock()
	if len(ts) != 5 {
		t.Fatalf("want 5 attempts, got %d", len(ts))
	}
	for i, min := range []time.Duration{100, 200, 400, 400} {
		if gap := ts[i+1].Sub(ts[i]); gap < min*time.Millisecond {
			t.Errorf("gap before attempt %d = %s, want >= %dms (tier TTL)", i+2, gap, min)
		}
	}
	itest.Eventually(t, 10*time.Second, "flaky and panicking messages recovered", func() bool {
		return e.effects("twice") == 1 && e.effects("panic") == 1
	})
	if e.effects("always") != 0 {
		t.Error("failed attempts must roll back their writes")
	}
	if metrics.MessageCount(e.queue, "retry") != 4+2+1 || metrics.MessageCount(e.queue, "dlq") != 1 {
		t.Errorf("retry=%d dlq=%d", metrics.MessageCount(e.queue, "retry"), metrics.MessageCount(e.queue, "dlq"))
	}
	for _, tier := range []int{1, 2, 3} {
		if n := itest.Depth(t, e.ch, topology.RetryQueue(e.queue, tier)); n != 0 {
			t.Errorf("retry queue %d not empty: %d", tier, n)
		}
	}
}

// 毒消息: 解不开 / 不接受的类型或版本 / 处理器标成 Poison → 不重试,直接 DLQ。
func TestPoisonToDLQ(t *testing.T) {
	e := setup(t)
	var calls atomic.Int32
	c := e.consumer(func(ctx context.Context, tx pgx.Tx, m message.Envelope) error {
		calls.Add(1)
		if err := effect(ctx, tx, m.Subject); err != nil {
			return err
		}
		return consume.Poison(errors.New("missing required field qty"))
	})
	run(t, c)
	e.publish([]byte("{not json"), "", nil)
	wrongVersion := e.envelope("v2")
	wrongVersion.Version = 2
	e.publishEnv(wrongVersion, nil)
	e.publishEnv(e.envelope("bad-data"), nil)
	dlq := topology.DLQ(e.queue)
	itest.Eventually(t, 10*time.Second, "3 messages in the DLQ", func() bool { return itest.Depth(t, e.ch, dlq) == 3 })
	errs := []string{}
	for i := 0; i < 3; i++ {
		d, _ := itest.Get(t, e.ch, dlq, time.Second)
		if message.HeaderString(d.Headers, message.HeaderOutcome) != "poison" {
			t.Errorf("outcome: %v", d.Headers)
		}
		if a, _ := message.HeaderInt(d.Headers, message.HeaderAttempt); a != 1 {
			t.Errorf("poison must not be retried: attempt %d", a)
		}
		errs = append(errs, message.HeaderString(d.Headers, message.HeaderError))
	}
	all := strings.Join(errs, " | ")
	for _, want := range []string{"not a JSON envelope", "does not accept", "missing required field qty"} {
		if !strings.Contains(all, want) {
			t.Errorf("DLQ errors %q: missing %q", all, want)
		}
	}
	if calls.Load() != 1 || e.effects("bad-data") != 0 {
		t.Errorf("handler calls = %d (only the decodable, accepted one), effect = %d (rolled back)", calls.Load(), e.effects("bad-data"))
	}
	if metrics.MessageCount(e.queue, "poison") != 3 {
		t.Errorf("poison count %d", metrics.MessageCount(e.queue, "poison"))
	}
}

// 崩溃点(子进程真的 os.Exit(137)):
//
//	before_commit       → 没有写入;重投 → 处理一次
//	after_commit        → 写入已提交;重投 → duplicate → 只生效一次
//	after_retry_publish → 重试副本已发出、原消息没 ack → 两份都在 → 只生效一次
func TestCrashPoints(t *testing.T) {
	cases := []struct {
		point         string
		fail          bool // 子进程里处理器失败(走到重试)
		afterCrash    int  // 崩溃后的效果
		wantDuplicate uint64
	}{
		{consume.CrashBeforeCommit, false, 0, 0},
		{consume.CrashAfterCommit, false, 1, 1},
		{consume.CrashAfterRetryPublish, true, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.point, func(t *testing.T) {
			e := setup(t)
			m := e.envelope("crash")
			e.publishEnv(m, amqp.Table{message.HeaderCrash: tc.point})
			cmd := exec.Command(os.Args[0], "-test.run=^TestHelperConsumer$")
			extra := []string{"RUNTIME_HELPER_CONSUMER_DB=" + e.dbURL, "RUNTIME_HELPER_CONSUMER_TYPE=" + e.typ, txn.TestFaultsEnv + "=1"}
			if tc.fail {
				extra = append(extra, "RUNTIME_HELPER_CONSUMER_FAIL=1")
			}
			cmd.Env = itest.Env(extra...)
			out, err := cmd.CombinedOutput()
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != 137 {
				t.Fatalf("helper should exit(137) at %s: %v\n%s", tc.point, err, out)
			}
			if n := e.effects("crash"); n != tc.afterCrash {
				t.Fatalf("after the crash: effect %d, want %d", n, tc.afterCrash)
			}
			// 重启(这个进程,同样开着 FACTORY_TEST_FAULTS: 重投的消息不再崩溃)
			t.Setenv(txn.TestFaultsEnv, "1")
			run(t, e.consumer(func(ctx context.Context, tx pgx.Tx, m message.Envelope) error { return effect(ctx, tx, m.Subject) }))
			itest.Eventually(t, 10*time.Second, "redelivered and handled", func() bool {
				return e.effects("crash") == 1 && metrics.MessageCount(e.queue, "duplicate") == tc.wantDuplicate
			})
			time.Sleep(700 * time.Millisecond) // 重试副本(TTL 100ms)如果还在,也该到了
			if n := e.effects("crash"); n != 1 {
				t.Fatalf("effect %d, want exactly 1", n)
			}
			if d := itest.Depth(t, e.ch, e.queue) + itest.Depth(t, e.ch, topology.DLQ(e.queue)); d != 0 {
				t.Fatalf("%d messages left", d)
			}
		})
	}
}

// 子进程: 开着 FACTORY_TEST_FAULTS 的消费者(由 TestCrashPoints 启动),应该在崩溃点退出。
func TestHelperConsumer(t *testing.T) {
	dbURL := os.Getenv("RUNTIME_HELPER_CONSUMER_DB")
	if dbURL == "" {
		t.Skip("helper process")
	}
	typ := os.Getenv("RUNTIME_HELPER_CONSUMER_TYPE")
	fail := os.Getenv("RUNTIME_HELPER_CONSUMER_FAIL") == "1"
	pool := itest.Pool(t, dbURL)
	c := &consume.Consumer{Queue: topology.QueueName("rt-consumer", typ), Exec: txn.New(pool), Broker: itest.Broker(t), Retry: fastRetry, Log: quiet,
		Handler: func(ctx context.Context, tx pgx.Tx, m message.Envelope) error {
			if err := effect(ctx, tx, m.Subject); err != nil {
				return err
			}
			if fail {
				return errors.New("transient")
			}
			return nil
		}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = c.Run(ctx)
	t.Fatal("the consumer should have crashed")
}

// 优雅停机: 正在处理的做完、提交、ack;没开始的不处理,留在队列里(给别的副本)。
func TestGracefulShutdown(t *testing.T) {
	e := setup(t)
	started := make(chan string, 10)
	c := e.consumer(func(ctx context.Context, tx pgx.Tx, m message.Envelope) error {
		started <- m.Subject
		time.Sleep(500 * time.Millisecond)
		return effect(ctx, tx, m.Subject)
	})
	c.Prefetch = 2
	for i := 0; i < 5; i++ {
		e.publishEnv(e.envelope(fmt.Sprint("s", i)), nil)
	}
	stop := run(t, c)
	<-started
	time.Sleep(50 * time.Millisecond) // 第二个 worker 也可能已经开始
	t0 := time.Now()
	stop()
	if d := time.Since(t0); d > 3*time.Second {
		t.Fatalf("shutdown took %s", d)
	}
	close(started)
	n := 1
	for range started {
		n++
	}
	var done int
	_ = e.pool.QueryRow(context.Background(), `SELECT count(*) FROM effects`).Scan(&done)
	if done != n || e.processed() != n {
		t.Fatalf("in-flight messages must complete: started %d, committed %d, processed %d", n, done, e.processed())
	}
	itest.Eventually(t, 5*time.Second, "unstarted messages back in the queue", func() bool { return itest.Depth(t, e.ch, e.queue) == 5-n })
}

// DLQ 管理: list(不移除)/ requeue(同一个 id 回到主队列)/ drop。
func TestDLQAdmin(t *testing.T) {
	e := setup(t)
	c := e.consumer(func(ctx context.Context, tx pgx.Tx, m message.Envelope) error {
		return consume.Poison(errors.New("bad " + m.Subject))
	})
	stop := run(t, c)
	for i := 0; i < 3; i++ {
		e.publishEnv(e.envelope(fmt.Sprint("d", i)), nil)
	}
	dlq := topology.DLQ(e.queue)
	itest.Eventually(t, 10*time.Second, "3 in the DLQ", func() bool { return itest.Depth(t, e.ch, dlq) == 3 })
	itest.Eventually(t, 5*time.Second, "DLQ gauge", func() bool {
		var b strings.Builder
		metrics.WriteMessaging(&b)
		return strings.Contains(b.String(), fmt.Sprintf("factory_dlq_messages{queue=%q} 3", dlq))
	})
	stop() // 管理操作时处理器不在跑(否则 requeue 的消息又会进 DLQ)
	ctx := context.Background()
	list, err := consume.DLQList(ctx, e.b, e.queue, 10)
	if err != nil || len(list) != 3 {
		t.Fatalf("list: %v %d", err, len(list))
	}
	if list[0].Outcome != "poison" || !strings.HasPrefix(list[0].Error, "poison message: bad d") || list[0].Attempt != 1 {
		t.Errorf("list[0]: %+v", list[0])
	}
	itest.Eventually(t, 5*time.Second, "list does not remove", func() bool { return itest.Depth(t, e.ch, dlq) == 3 })
	if n, err := consume.DLQRequeue(ctx, e.b, e.queue, 1); err != nil || n != 1 {
		t.Fatalf("requeue: %v %d", err, n)
	}
	d, ok := itest.Get(t, e.ch, e.queue, 5*time.Second)
	if !ok || d.MessageId != list[0].ID || d.Headers[message.HeaderError] != nil || d.Headers[message.HeaderAttempt] != nil {
		t.Fatalf("requeued: %v %s %v", ok, d.MessageId, d.Headers)
	}
	if n, err := consume.DLQDrop(ctx, e.b, e.queue, 1); err != nil || n != 1 {
		t.Fatalf("drop: %v %d", err, n)
	}
	itest.Eventually(t, 5*time.Second, "1 left in the DLQ", func() bool { return itest.Depth(t, e.ch, dlq) == 1 })
	if n, _ := consume.QueueDepth(ctx, e.b, dlq); n != 1 {
		t.Errorf("QueueDepth = %d", n)
	}
}

// 进程内驱动(不经 RabbitMQ): Process 的判定。工厂的进程内差分测试这样调用生成的处理器。
func TestProcessInProcess(t *testing.T) {
	e := setup(t)
	c := e.consumer(func(ctx context.Context, tx pgx.Tx, m message.Envelope) error {
		if err := effect(ctx, tx, m.Subject); err != nil {
			return err
		}
		if m.Subject == "fail" {
			return errors.New("transient")
		}
		return nil
	})
	m := e.envelope("p")
	body, _ := m.Encode()
	ctx := context.Background()
	if r := c.Process(ctx, consume.Delivery{Body: body}); r.Outcome != consume.OK || r.Envelope.ID != m.ID || r.Attempt != 1 {
		t.Fatalf("first: %+v", r)
	}
	if r := c.Process(ctx, consume.Delivery{Body: body}); r.Outcome != consume.Duplicate {
		t.Fatalf("second: %+v", r)
	}
	f := e.envelope("fail")
	fb, _ := f.Encode()
	if r := c.Process(ctx, consume.Delivery{Body: fb}); r.Outcome != consume.Retry || r.Tier != 1 || r.Err == nil {
		t.Fatalf("fail #1: %+v", r)
	}
	if r := c.Process(ctx, consume.Delivery{Body: fb, Headers: map[string]any{message.HeaderAttempt: int32(4)}}); r.Outcome != consume.Retry || r.Tier != 3 {
		t.Fatalf("fail #4: %+v", r)
	}
	if r := c.Process(ctx, consume.Delivery{Body: fb, Headers: map[string]any{message.HeaderAttempt: int32(5)}}); r.Outcome != consume.Exhausted {
		t.Fatalf("fail #5: %+v", r)
	}
	// 业务结果: 照样提交,outcome = business
	cb := e.consumer(func(ctx context.Context, tx pgx.Tx, m message.Envelope) error {
		consume.Business(ctx, "INSUFFICIENT_STOCK")
		return effect(ctx, tx, m.Subject)
	})
	bm := e.envelope("b")
	bb, _ := bm.Encode()
	if r := cb.Process(ctx, consume.Delivery{Body: bb}); r.Outcome != consume.OK || r.Business != "INSUFFICIENT_STOCK" || e.effects("b") != 1 {
		t.Fatalf("business outcome: %+v", r)
	}
	if metrics.MessageCount(e.queue, "business") != 1 {
		t.Errorf("business count %d", metrics.MessageCount(e.queue, "business"))
	}
	if e.effects("p") != 1 || e.effects("fail") != 0 {
		t.Fatal("effects")
	}
	// 测试的故障注入 header(FACTORY_TEST_FAULTS=1): 第 1 个写之后失败 → 回滚,暂时性
	t.Setenv(txn.TestFaultsEnv, "1")
	g := e.envelope("g")
	gb, _ := g.Encode()
	r := c.Process(ctx, consume.Delivery{Body: gb, Headers: map[string]any{message.HeaderFailAfter: "1"}})
	if r.Outcome != consume.Retry || txn.ReasonOf(r.Err) != txn.Injected || e.effects("g") != 0 {
		t.Fatalf("fault header: %+v", r)
	}
}
