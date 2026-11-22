package topology_test

import (
	"context"
	"testing"
	"time"

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
