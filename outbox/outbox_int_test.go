package outbox_test

// 集成测试: 真实的 Postgres(RUNTIME_TEST_DB)和 RabbitMQ(RUNTIME_TEST_AMQP),没有设置时跳过。

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MIERAI/api-factory-runtime/clock"
	"github.com/MIERAI/api-factory-runtime/internal/itest"
	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/metrics"
	"github.com/MIERAI/api-factory-runtime/outbox"
	"github.com/MIERAI/api-factory-runtime/topology"
	"github.com/MIERAI/api-factory-runtime/txn"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func pending(t *testing.T, pool *pgxpool.Pool) (n int) {
	t.Helper()
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM factory_outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// 原子性: 事务回滚(处理器返回错误 / 故障注入在 outbox 写入之后)→ 没有消息;提交 → 有,envelope 的字段是平台定的。
func TestWriteAtomicity(t *testing.T) {
	pool := itest.Pool(t, itest.NewDB(t))
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE TABLE tasks (id bigint PRIMARY KEY, title text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	e := txn.New(pool)
	w := outbox.NewWriter("tasks")
	create := func(ctx context.Context, id int64) error {
		return e.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			if err := txn.RunIn(ctx, tx, txn.Op{Kind: txn.Insert, Table: "tasks", Values: []txn.Col{{Name: "id", Value: id}, {Name: "title", Value: "t"}}}); err != nil {
				return err
			}
			_, err := w.Write(ctx, tx, outbox.Message{Kind: message.Event, Type: "task.created", Version: 1, Subject: "1",
				Data: map[string]any{"task_id": id}})
			return err
		})
	}
	// 故障注入: 第 2 个写(= outbox 的行)之后失败 → 任务和消息都没有
	if got := txn.ReasonOf(create(txn.WithFault(ctx, 2), 1)); got != txn.Injected {
		t.Fatalf("fault: %s", got)
	}
	// 处理器在写完 outbox 之后失败
	boom := errors.New("boom")
	err := e.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := w.Write(ctx, tx, outbox.Message{Kind: message.Event, Type: "task.created", Version: 1}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM factory_outbox`).Scan(&n)
	if n != 0 {
		t.Fatalf("rolled back transactions left %d outbox rows", n)
	}
	// 提交
	at := time.Date(2026, 11, 22, 9, 0, 0, 0, time.UTC)
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if err := create(message.WithTraceparent(clock.With(ctx, at), tp), 2); err != nil {
		t.Fatal(err)
	}
	var id, typ, subject, env string
	var kind int16
	var occurred time.Time
	if err := pool.QueryRow(ctx, `SELECT id::text, kind, type, subject, envelope::text, occurred_at FROM factory_outbox`).Scan(&id, &kind, &typ, &subject, &env, &occurred); err != nil {
		t.Fatal(err)
	}
	got, err := message.Decode([]byte(env))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != id || kind != 1 || typ != "task.created" || subject != "1" || !occurred.Equal(at) || !got.OccurredAt.Equal(at) ||
		got.Source != "tasks" || got.Traceparent != tp || string(got.Data) != `{"task_id": 2}` {
		t.Fatalf("row: id=%s kind=%d type=%s subject=%s occurred=%s envelope=%s", id, kind, typ, subject, occurred, env)
	}
	if _, ok := message.IDTime(id); !ok {
		t.Errorf("id is not a UUIDv7: %s", id)
	}
}

func setupQueues(t *testing.T, types ...string) (topology.Spec, map[string]string) {
	t.Helper()
	b := itest.Broker(t)
	ch := itest.Channel(t, b)
	spec := topology.Spec{Retry: topology.DefaultRetry}
	queues := map[string]string{}
	for _, typ := range types {
		kind := message.Event
		if strings.HasPrefix(typ, "cmd.") {
			kind = message.Command
		}
		q := topology.QueueName("rt-test", typ)
		queues[typ] = q
		spec.Queues = append(spec.Queues, topology.Queue{Name: q, Subscriptions: []topology.Subscription{{Kind: kind, Type: typ}}})
	}
	if err := topology.Declare(ch, spec); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if ch, err := b.Channel(context.Background()); err == nil {
			_ = topology.Delete(ch, spec)
			_ = ch.Close()
		}
	})
	return spec, queues
}

func writeMessages(t *testing.T, pool *pgxpool.Pool, msgs ...outbox.Message) []message.Envelope {
	t.Helper()
	var envs []message.Envelope
	err := txn.New(pool).InTx(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
		var err error
		envs, err = outbox.NewWriter("rt-test").Write(ctx, tx, msgs...)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return envs
}

func runRelay(t *testing.T, r *outbox.Relay) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("relay: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("relay did not stop")
		}
	}
}

// 中继: 事件和命令发到各自的交换机,确认后标记;没有队列接收的命令留在 outbox 里(attempts、last_error)。
func TestRelayPublishes(t *testing.T) {
	sfx := itest.Suffix()
	evt, cmd, lost := "evt.created."+sfx, "cmd.do."+sfx, "cmd.nobody."+sfx
	_, queues := setupQueues(t, evt, cmd)
	pool := itest.Pool(t, itest.NewDB(t))
	b := itest.Broker(t)
	ch := itest.Channel(t, b)
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := message.WithTraceparent(context.Background(), tp)
	var envs []message.Envelope
	if err := txn.New(pool).InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		envs, err = outbox.NewWriter("rt-test").Write(ctx, tx,
			outbox.Message{Kind: message.Event, Type: evt, Version: 1, Subject: "a", Data: map[string]any{"n": 1}},
			outbox.Message{Kind: message.Command, Type: cmd, Version: 2, Subject: "b", Data: map[string]any{"n": 2}},
			outbox.Message{Kind: message.Command, Type: lost, Version: 1, Subject: "c"},
			outbox.Message{Kind: message.Event, Type: "evt.unsubscribed." + sfx, Version: 1, Subject: "d"}, // 没有订阅者的事件: 丢弃,但看得见
		)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	stop := runRelay(t, &outbox.Relay{DB: pool, Broker: b, Log: quiet})
	defer stop()
	d, ok := itest.Get(t, ch, queues[evt], 10*time.Second)
	if !ok {
		t.Fatal("event not delivered")
	}
	if d.MessageId != envs[0].ID || d.Type != evt || d.ContentType != "application/json" || d.DeliveryMode != 2 ||
		message.HeaderString(d.Headers, "traceparent") != tp || d.Exchange != "factory.events" {
		t.Errorf("event delivery: %+v", d)
	}
	if got, err := message.Decode(d.Body); err != nil || got.ID != envs[0].ID || got.Subject != "a" {
		t.Errorf("event body: %v %s", err, d.Body)
	}
	d, ok = itest.Get(t, ch, queues[cmd], 10*time.Second)
	if !ok || d.MessageId != envs[1].ID || d.Exchange != "factory.commands" {
		t.Fatalf("command delivery: %v %+v", ok, d)
	}
	if v, _ := message.HeaderInt(d.Headers, message.HeaderVersion); v != 2 {
		t.Errorf("version header: %v", d.Headers)
	}
	itest.Eventually(t, 10*time.Second, "events published, the unroutable command left in the outbox", func() bool {
		var lastErr *string
		var attempts int
		err := pool.QueryRow(context.Background(), `SELECT attempts, last_error FROM factory_outbox WHERE type = $1`, lost).Scan(&attempts, &lastErr)
		return err == nil && attempts >= 1 && lastErr != nil && strings.Contains(*lastErr, "unroutable") && pending(t, pool) == 1
	})
	// 没有订阅队列的事件: 标成已发送(不堵 outbox),但记下 last_error + 指标(不悄悄丢)
	var published bool
	var lastErr *string
	if err := pool.QueryRow(context.Background(), `SELECT published_at IS NOT NULL, last_error FROM factory_outbox WHERE type = $1`,
		"evt.unsubscribed."+sfx).Scan(&published, &lastErr); err != nil {
		t.Fatal(err)
	}
	if !published || lastErr == nil || !strings.Contains(*lastErr, "unroutable") {
		t.Errorf("unroutable event: published %v last_error %v", published, lastErr)
	}
	if metrics.UnroutableCount("event", "evt.unsubscribed."+sfx) != 1 || metrics.UnroutableCount("command", lost) < 1 ||
		metrics.UnroutableCount("event", evt) != 0 {
		t.Errorf("unroutable metric: event %d command %d", metrics.UnroutableCount("event", "evt.unsubscribed."+sfx), metrics.UnroutableCount("command", lost))
	}
}

// 崩溃(真的 os.Exit(137))在发布之后、标记之前 → 重启后同一条消息(同一个 id、逐字节相同)再发一次。
func TestRelayCrashRepublishesSameID(t *testing.T) {
	for _, point := range []string{outbox.CrashAfterPublish, outbox.CrashAfterConfirm} {
		t.Run(point, func(t *testing.T) {
			sfx := itest.Suffix()
			evt := "evt.crash." + sfx
			_, queues := setupQueues(t, evt)
			dbURL := itest.NewDB(t)
			pool := itest.Pool(t, dbURL)
			envs := writeMessages(t, pool, outbox.Message{Kind: message.Event, Type: evt, Version: 1, Subject: "x", Data: map[string]any{"v": "1.50"}})

			cmd := exec.Command(os.Args[0], "-test.run=^TestHelperRelay$")
			cmd.Env = itest.Env("RUNTIME_HELPER_RELAY_DB="+dbURL, txn.TestFaultsEnv+"=1", outbox.RelayCrashEnv+"="+point)
			out, err := cmd.CombinedOutput()
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != 137 {
				t.Fatalf("helper should exit(137) at %s: %v\n%s", point, err, out)
			}
			if pending(t, pool) != 1 {
				t.Fatal("the crashed relay must not have marked the message")
			}
			b := itest.Broker(t)
			ch := itest.Channel(t, b)
			stop := runRelay(t, &outbox.Relay{DB: pool, Broker: b, Log: quiet})
			itest.Eventually(t, 10*time.Second, "outbox drained", func() bool { return pending(t, pool) == 0 })
			stop()
			// after_confirm: 崩溃前的那次一定到了 broker → 正好两份。after_publish: 崩溃前的那次没等 confirm,
			// 可能没到(进程退出时还在发送缓冲里)→ 一份或两份;无论如何 id 和内容都相同
			var got []string
			var body string
			for {
				d, ok := itest.Get(t, ch, queues[evt], 500*time.Millisecond)
				if !ok {
					break
				}
				if d.MessageId != envs[0].ID || (body != "" && string(d.Body) != body) {
					t.Fatalf("re-publish must keep the id and the bytes: %s %s (first body %s)", d.MessageId, d.Body, body)
				}
				body = string(d.Body)
				got = append(got, d.MessageId)
			}
			if len(got) != 2 && !(point == outbox.CrashAfterPublish && len(got) == 1) {
				t.Fatalf("%s: got %d copies", point, len(got))
			}
		})
	}
}

// 子进程: 带崩溃点运行中继(由 TestRelayCrashRepublishesSameID 启动)。
func TestHelperRelay(t *testing.T) {
	dbURL := os.Getenv("RUNTIME_HELPER_RELAY_DB")
	if dbURL == "" {
		t.Skip("helper process")
	}
	pool := itest.Pool(t, dbURL)
	r := &outbox.Relay{DB: pool, Broker: itest.Broker(t), Log: quiet}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = r.Run(ctx)
	t.Fatal("the relay should have crashed")
}

// RabbitMQ 的连接断了: 中继重新连上,积压排空,没有丢失。
func TestRelayReconnects(t *testing.T) {
	sfx := itest.Suffix()
	evt := "evt.reconnect." + sfx
	_, queues := setupQueues(t, evt)
	pool := itest.Pool(t, itest.NewDB(t))
	b := itest.Broker(t)
	stop := runRelay(t, &outbox.Relay{DB: pool, Broker: b, Log: quiet, MaxBackoff: 2 * time.Second})
	defer stop()
	writeMessages(t, pool, outbox.Message{Kind: message.Event, Type: evt, Version: 1})
	itest.Eventually(t, 10*time.Second, "first message published", func() bool { return pending(t, pool) == 0 })
	b.Drop() // 断线
	writeMessages(t, pool, outbox.Message{Kind: message.Event, Type: evt, Version: 1}, outbox.Message{Kind: message.Event, Type: evt, Version: 1})
	itest.Eventually(t, 15*time.Second, "backlog drained after reconnect", func() bool { return pending(t, pool) == 0 })
	ch := itest.Channel(t, itest.Broker(t))
	itest.Eventually(t, 5*time.Second, "3 messages in the queue", func() bool { return itest.Depth(t, ch, queues[evt]) >= 3 })
}

// 清理: 发送超过保留期的行、超过保留期的去重记录被删除;未发送的不删。
func TestCleanup(t *testing.T) {
	pool := itest.Pool(t, itest.NewDB(t))
	ctx := context.Background()
	envs := writeMessages(t, pool, outbox.Message{Kind: message.Event, Type: "a", Version: 1}, outbox.Message{Kind: message.Event, Type: "b", Version: 1},
		outbox.Message{Kind: message.Event, Type: "c", Version: 1})
	if _, err := pool.Exec(ctx, `UPDATE factory_outbox SET published_at = now() - interval '8 days' WHERE type = 'a';
UPDATE factory_outbox SET published_at = now() WHERE type = 'b';
INSERT INTO factory_processed_messages VALUES ('h', '`+envs[0].ID+`', now() - interval '15 days'), ('h', '`+envs[1].ID+`', now())`); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Cleanup(ctx, pool, 7*24*time.Hour, 14*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	var types []string
	rows, _ := pool.Query(ctx, `SELECT type FROM factory_outbox ORDER BY type`)
	types, _ = pgx.CollectRows(rows, pgx.RowTo[string])
	var dedup int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM factory_processed_messages`).Scan(&dedup)
	if strings.Join(types, ",") != "b,c" || dedup != 1 {
		t.Fatalf("after cleanup: outbox %v, dedup %d", types, dedup)
	}
}
