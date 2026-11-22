package topology_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/broker"
	"github.com/MIERAI/api-factory-runtime/internal/itest"
	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/topology"
)

// Ensure: 声明是幂等的;参数和已有的不一致 → 错误(启动失败);ctx 结束之前连不上 → nil。
func TestEnsureInt(t *testing.T) {
	b := itest.Broker(t)
	q := "rt-ensure-" + itest.Suffix()
	s := topology.Spec{Retry: topology.DefaultRetry, Queues: []topology.Queue{{Name: q, Subscriptions: []topology.Subscription{{Kind: message.Event, Type: q + ".evt"}}}}}
	t.Cleanup(func() { _ = topology.Delete(itest.Channel(t, b), s) })
	ctx := context.Background()
	if err := topology.Ensure(ctx, b, s); err != nil {
		t.Fatal(err)
	}
	if err := topology.Ensure(ctx, b, s); err != nil {
		t.Fatalf("idempotent: %v", err)
	}
	other := s
	other.Retry = topology.RetryPolicy{Tiers: []time.Duration{2 * time.Second}, MaxAttempts: 2}
	if err := topology.Ensure(ctx, b, other); err == nil {
		t.Error("a different retry TTL must fail (PRECONDITION_FAILED), not be changed silently")
	}
	cctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	if err := topology.Ensure(cctx, brokerDown(), s); err != nil {
		t.Errorf("unreachable broker until ctx ends: %v", err)
	}
}

func brokerDown() *broker.Conn { return broker.New("amqp://guest:guest@127.0.0.1:1/", "down") }

// Apply(系统级的清单): 声明全部队列 + 绑定,之后服务自己的 Ensure(同样的 Spec)是幂等的;
// 发到交换机的事件在订阅方启动之前就进了队列(G23);连不上 → ctx 结束时返回错误(一次性的 Job 失败)。
func TestApplyManifestInt(t *testing.T) {
	b := itest.Broker(t)
	sfx := itest.Suffix()
	typ := "rt.manifest." + sfx
	s := topology.Spec{Retry: topology.DefaultRetry, Queues: []topology.Queue{{Name: topology.QueueName("rt-sub", typ), Subscriptions: []topology.Subscription{{Kind: message.Event, Type: typ}}}}}
	t.Cleanup(func() { _ = topology.Delete(itest.Channel(t, b), s) })
	q, err := topology.QueueOf("rt-sub", s.Queues[0], s)
	if err != nil {
		t.Fatal(err)
	}
	m := topology.Manifest{Version: topology.ManifestVersion, Exchanges: topology.DefaultExchanges, Retry: topology.RetryOf(s.Retry), Queues: []topology.ManifestQueue{q}}
	ctx := context.Background()
	if err := topology.Apply(ctx, b, m); err != nil {
		t.Fatal(err)
	}
	ch := itest.Channel(t, b)
	for _, n := range append([]string{q.Name, q.DLQ}, q.Retry...) {
		if _, err := ch.QueueDeclarePassive(n, true, false, false, false, nil); err != nil {
			t.Fatalf("%s not declared: %v", n, err)
		}
	}
	if err := topology.Ensure(ctx, b, s); err != nil {
		t.Fatalf("the service's own Ensure after the manifest: %v", err)
	}
	pub := itest.Channel(t, b)
	if err := pub.PublishWithContext(ctx, topology.DefaultExchanges.Events, typ, true, false, amqpPublishing()); err != nil {
		t.Fatal(err)
	}
	if _, ok := itest.Get(t, pub, q.Name, 5*time.Second); !ok {
		t.Fatal("event published before any subscriber started is not in the queue")
	}
	cctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	if err := topology.Apply(cctx, brokerDown(), m); err == nil {
		t.Error("unreachable broker: Apply must fail when ctx ends")
	}
}

func amqpPublishing() amqp.Publishing {
	return amqp.Publishing{ContentType: message.ContentType, Body: []byte(`{}`), MessageId: message.NewID()}
}
