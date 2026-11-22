package outbox

import (
	"encoding/json"
	"testing"

	"github.com/MIERAI/api-factory-runtime/txn"
)

// 生产环境(FACTORY_TEST_FAULTS 没有设置)中继的崩溃点无效。
func TestRelayCrashIgnoredInProduction(t *testing.T) {
	t.Setenv(txn.TestFaultsEnv, "")
	t.Setenv(RelayCrashEnv, CrashAfterPublish)
	p, err := relayCrashPlan()
	if err != nil || p.point != "" {
		t.Fatalf("production must ignore %s: %+v %v", RelayCrashEnv, p, err)
	}
	t.Setenv(txn.TestFaultsEnv, "1")
	if p, err := relayCrashPlan(); err != nil || p != (crashPlan{point: CrashAfterPublish, batch: 1}) {
		t.Fatalf("test faults: %+v %v", p, err)
	}
	if p, err := relayCrashFromEnv(true, "after_confirm:3"); err != nil || p != (crashPlan{point: CrashAfterConfirm, batch: 3}) {
		t.Fatalf("batch: %+v %v", p, err)
	}
	for _, bad := range []string{"later", "after_publish:0", "after_publish:x"} {
		if _, err := relayCrashFromEnv(true, bad); err == nil {
			t.Errorf("%q: want an error", bad)
		}
	}
}

func TestEncodeData(t *testing.T) {
	for _, ok := range []any{nil, map[string]any{"a": 1}, json.RawMessage(`{"a":"1"}`), []byte(`{}`), struct {
		A int `json:"a"`
	}{1}} {
		if _, err := encodeData(ok); err != nil {
			t.Errorf("%v: %v", ok, err)
		}
	}
	for _, bad := range []any{[]int{1}, "x", json.RawMessage(`null`), json.RawMessage(`{`), 3} {
		if _, err := encodeData(bad); err == nil {
			t.Errorf("%v: data must be a JSON object", bad)
		}
	}
}
