package consume

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/metrics"
	"github.com/MIERAI/api-factory-runtime/txn"
)

// 生产环境(FACTORY_TEST_FAULTS 没有设置)消息的崩溃点 / 故障注入 header 无效。
func TestCrashHeaderIgnoredInProduction(t *testing.T) {
	t.Setenv(txn.TestFaultsEnv, "")
	c := &Consumer{Queue: "q"}
	d := Delivery{Headers: map[string]any{message.HeaderCrash: CrashAfterCommit, message.HeaderFailAfter: "1"}}
	if p := c.crashPoint(d); p != "" {
		t.Fatalf("production must ignore %s: %q", message.HeaderCrash, p)
	}
	env := message.Envelope{ID: message.NewID()}
	ctx, err := c.context(context.Background(), d, env, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.AfterWrite(ctx, "t"); err != nil {
		t.Fatalf("production must ignore %s: %v", message.HeaderFailAfter, err)
	}
	t.Setenv(txn.TestFaultsEnv, "1")
	if p := c.crashPoint(d); p != CrashAfterCommit {
		t.Fatalf("test faults: %q", p)
	}
	d.Redelivered = true
	if p := c.crashPoint(d); p != "" {
		t.Fatalf("a redelivery must not crash again: %q", p)
	}
	d.Headers[message.HeaderFailAfter] = "zero"
	if _, err := c.context(context.Background(), d, env, 1); err == nil {
		t.Fatal("malformed fail-after header must be an error in test mode")
	}
}

// x-factory-fail-attempts: 生产环境无效(有效时的行为: TestProcessInProcess)。
func TestFailAttemptsIgnoredInProduction(t *testing.T) {
	t.Setenv(txn.TestFaultsEnv, "")
	c := &Consumer{Queue: "q"}
	d := Delivery{Headers: map[string]any{message.HeaderFailAttempts: "2"}}
	ctx, err := c.context(context.Background(), d, message.Envelope{ID: message.NewID()}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.AfterWrite(ctx, "t"); err != nil {
		t.Fatalf("production must ignore %s: %v", message.HeaderFailAttempts, err)
	}
}

func TestProcessPoisonWithoutDB(t *testing.T) {
	c := &Consumer{Queue: "q.poison." + message.NewID(), Accept: []Accept{{Type: "task.created", Versions: []int{1}}}}
	res := c.Process(context.Background(), Delivery{Body: []byte("not json")})
	if res.Outcome != Poisoned || !IsPoison(res.Err) {
		t.Fatalf("undecodable: %+v", res)
	}
	body, _ := message.Envelope{ID: message.NewID(), Type: "task.created", Version: 2, OccurredAt: time.Now(), Source: "s"}.Encode()
	if res := c.Process(context.Background(), Delivery{Body: body}); res.Outcome != Poisoned {
		t.Fatalf("unknown version: %+v", res)
	}
	if n := metrics.MessageCount(c.Queue, "poison"); n != 2 {
		t.Errorf("poison count = %d", n)
	}
	if !IsPoison(Poison(errors.New("x"))) || IsPoison(errors.New("x")) {
		t.Error("IsPoison")
	}
}

func TestAccepts(t *testing.T) {
	c := &Consumer{Accept: []Accept{{Type: "a", Versions: []int{1, 2}}, {Type: "b"}}}
	for _, tc := range []struct {
		typ  string
		v    int
		want bool
	}{{"a", 1, true}, {"a", 2, true}, {"a", 3, false}, {"b", 9, true}, {"c", 1, false}} {
		if got := c.accepts(message.Envelope{Type: tc.typ, Version: tc.v}); got != tc.want {
			t.Errorf("%s v%d: %v", tc.typ, tc.v, got)
		}
	}
	if !(&Consumer{}).accepts(message.Envelope{Type: "x", Version: 1}) {
		t.Error("no Accept list: accept everything")
	}
}

func TestCopyHeaders(t *testing.T) {
	h := copyHeaders(amqp.Table{message.HeaderCrash: "after_commit", "x-death": []any{}, "x-first-death-queue": "q", "x-last-death-reason": "expired",
		message.HeaderAttempt: int32(2), "traceparent": "t"})
	if len(h) != 2 || h[message.HeaderAttempt] != int32(2) || h["traceparent"] != "t" {
		t.Errorf("copyHeaders: %v", h)
	}
}

func TestOutcomeString(t *testing.T) {
	for o, s := range map[Outcome]string{OK: "ok", Duplicate: "duplicate", Retry: "retry", Exhausted: "dlq", Poisoned: "poison"} {
		if o.String() != s {
			t.Errorf("%d: %s", o, o.String())
		}
	}
}
