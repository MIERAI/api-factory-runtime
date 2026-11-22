package topology

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/MIERAI/api-factory-runtime/message"
)

// 清单 ↔ Spec: 写出来再读回来是同一个 Spec;手改推出来的名字 / 未知字段 / 版本不对 → 错误。
func TestManifestRoundTrip(t *testing.T) {
	s := Spec{Retry: DefaultRetry, Queues: []Queue{
		{Name: "inventory.inventory.reserve", Subscriptions: []Subscription{{Kind: message.Command, Type: "inventory.reserve"}}},
		{Name: "order.inventory.reserved", Subscriptions: []Subscription{{Kind: message.Event, Type: "inventory.reserved"}}},
	}}
	m := Manifest{Version: ManifestVersion, Exchanges: DefaultExchanges, Retry: RetryOf(s.Retry)}
	for i, svc := range []string{"inventory", "order"} {
		q, err := QueueOf(svc, s.Queues[i], s)
		if err != nil {
			t.Fatal(err)
		}
		m.Queues = append(m.Queues, q)
	}
	if m.Queues[0].DLQ != "inventory.inventory.reserve.dlq" || len(m.Queues[0].Retry) != 3 || m.Queues[1].Bindings[0].Exchange != "factory.events" {
		t.Fatalf("derived names: %+v", m.Queues)
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	got, err := back.Spec()
	if err != nil {
		t.Fatal(err)
	}
	got.Exchanges = Exchanges{} // 零值 = 默认;比较时去掉
	s2 := s
	if fmtSpec(got) != fmtSpec(s2) {
		t.Fatalf("round trip:\n%s\n%s", fmtSpec(got), fmtSpec(s2))
	}

	edited := strings.Replace(string(b), `"inventory.inventory.reserve.dlq"`, `"elsewhere.dlq"`, 1)
	if m2, err := ParseManifest([]byte(edited)); err != nil {
		t.Fatal(err)
	} else if _, err := m2.Spec(); err == nil || !strings.Contains(err.Error(), "do not edit") {
		t.Fatalf("hand-edited dlq: %v", err)
	}
	if _, err := ParseManifest([]byte(`{"version":1,"surprise":true}`)); err == nil {
		t.Fatal("unknown field accepted")
	}
	if _, err := (Manifest{Version: 2}).Spec(); err == nil {
		t.Fatal("wrong version accepted")
	}
	bad := m
	bad.Queues = append([]ManifestQueue(nil), m.Queues...)
	bad.Queues[1].Bindings = []ManifestBinding{{Kind: "signal", Type: "x", Exchange: "factory.events"}}
	if _, err := bad.Spec(); err == nil {
		t.Fatal("unknown kind accepted")
	}
}

func fmtSpec(s Spec) string { b, _ := json.Marshal(s); return string(b) }
