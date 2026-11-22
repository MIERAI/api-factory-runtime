package topology

import (
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/message"
)

func TestTier(t *testing.T) {
	p := DefaultRetry
	// 第 n 次失败 → 第几级: 1s, 10s, 60s, 60s,第 5 次失败 → DLQ
	want := map[int]int{0: 1, 1: 1, 2: 2, 3: 3, 4: 3, 5: 0, 6: 0}
	for attempt, tier := range want {
		if got := p.Tier(attempt); got != tier {
			t.Errorf("Tier(%d) = %d, want %d", attempt, got, tier)
		}
	}
	if (RetryPolicy{MaxAttempts: 3}).Tier(1) != 0 {
		t.Error("no tiers: straight to the DLQ")
	}
}

type fakeCh struct {
	exchanges map[string]string
	queues    map[string]amqp.Table
	binds     [][3]string
	fail      string
}

func (f *fakeCh) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	if !durable || autoDelete || internal {
		return errors.New("exchanges must be durable")
	}
	f.exchanges[name] = kind
	return nil
}

func (f *fakeCh) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if name == f.fail {
		return amqp.Queue{}, errors.New("PRECONDITION_FAILED")
	}
	if !durable || autoDelete || exclusive {
		return amqp.Queue{}, errors.New("queues must be durable")
	}
	f.queues[name] = args
	return amqp.Queue{Name: name}, nil
}

func (f *fakeCh) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	f.binds = append(f.binds, [3]string{name, key, exchange})
	return nil
}

func TestDeclare(t *testing.T) {
	f := &fakeCh{exchanges: map[string]string{}, queues: map[string]amqp.Table{}}
	q := QueueName("notification", "order.confirmed")
	s := Spec{Retry: DefaultRetry, Queues: []Queue{
		{Name: q, Subscriptions: []Subscription{{Kind: message.Event, Type: "order.confirmed"}}},
		{Name: QueueName("inventory", "inventory.reserve"), Subscriptions: []Subscription{{Kind: message.Command, Type: "inventory.reserve"}}},
	}}
	if err := Declare(f, s); err != nil {
		t.Fatal(err)
	}
	if f.exchanges["factory.events"] != "topic" || f.exchanges["factory.commands"] != "direct" {
		t.Errorf("exchanges: %v", f.exchanges)
	}
	if q != "notification.order.confirmed" || len(f.queues) != 10 {
		t.Fatalf("queues: %v", f.queues)
	}
	main := f.queues[q]
	if main["x-dead-letter-exchange"] != "" || main["x-dead-letter-routing-key"] != q+".dlq" {
		t.Errorf("main queue args: %v", main)
	}
	for i, ttl := range []int64{1000, 10000, 60000} {
		r := f.queues[RetryQueue(q, i+1)]
		if r["x-message-ttl"] != ttl || r["x-dead-letter-exchange"] != "" || r["x-dead-letter-routing-key"] != q {
			t.Errorf("retry %d args: %v", i+1, r)
		}
	}
	if _, ok := f.queues[q+".dlq"]["x-message-ttl"]; ok {
		t.Error("the DLQ has no TTL")
	}
	want := [][3]string{{q, "order.confirmed", "factory.events"}, {"inventory.inventory.reserve", "inventory.reserve", "factory.commands"}}
	if len(f.binds) != 2 || f.binds[0] != want[0] || f.binds[1] != want[1] {
		t.Errorf("bindings: %v", f.binds)
	}
	// 参数不一致(服务器拒绝)→ 错误
	f.fail = q
	if err := Declare(f, s); err == nil {
		t.Error("declare error must be returned")
	}
	if err := Declare(f, Spec{Retry: RetryPolicy{MaxAttempts: 0}}); err == nil {
		t.Error("MaxAttempts 0 must be rejected")
	}
	if err := Declare(f, Spec{Retry: RetryPolicy{MaxAttempts: 2, Tiers: []time.Duration{0}}}); err == nil {
		t.Error("zero TTL must be rejected")
	}
	if err := Declare(f, Spec{Retry: DefaultRetry, Queues: []Queue{{Name: "x", Subscriptions: []Subscription{{Kind: 9, Type: "t"}}}}}); err == nil {
		t.Error("unknown kind must be rejected")
	}
}
