package anomaly

import (
	"strings"
	"testing"
)

func TestReportAndMetrics(t *testing.T) {
	Report("jobs", "status", int32(7))
	Report("jobs", "status", int32(8))
	var b strings.Builder
	WriteMetrics(&b)
	if !strings.Contains(b.String(), `data_anomaly_total{table="jobs",column="status"} 2`) {
		t.Fatalf("unexpected metrics:\n%s", b.String())
	}
}
