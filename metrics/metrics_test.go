package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestObserveAndWrite(t *testing.T) {
	reset()
	Observe("/x.v1.S/A", "ok", 3*time.Millisecond)
	Observe("/x.v1.S/A", "ok", 40*time.Millisecond)
	Observe("/x.v1.S/A", "internal", 2*time.Second)
	Observe("/x.v1.S/A", "ok", 9*time.Second) // > 最大的 bucket: 只在 +Inf
	var b strings.Builder
	Write(&b)
	out := b.String()
	for _, want := range []string{
		`rpc_requests_total{procedure="/x.v1.S/A",code="ok"} 3`,
		`rpc_requests_total{procedure="/x.v1.S/A",code="internal"} 1`,
		`rpc_request_duration_seconds_bucket{procedure="/x.v1.S/A",le="0.005"} 1`,
		`rpc_request_duration_seconds_bucket{procedure="/x.v1.S/A",le="0.05"} 2`,
		`rpc_request_duration_seconds_bucket{procedure="/x.v1.S/A",le="2.5"} 3`,
		`rpc_request_duration_seconds_bucket{procedure="/x.v1.S/A",le="5"} 3`,
		`rpc_request_duration_seconds_bucket{procedure="/x.v1.S/A",le="+Inf"} 4`,
		`rpc_request_duration_seconds_count{procedure="/x.v1.S/A"} 4`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s\n%s", want, out)
		}
	}
}
