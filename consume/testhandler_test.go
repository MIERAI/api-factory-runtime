package consume

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MIERAI/api-factory-runtime/txn"
)

// 生产环境(FACTORY_TEST_FAULTS 没有设置): 进程内投递的入口不存在(发消息的人不能绕过 RabbitMQ 直接触发处理器)。
func TestTestHandlerOffInProduction(t *testing.T) {
	h := TestHandler(&Consumer{Queue: "svc.task.created"})
	post := func(body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, TestPath, strings.NewReader(body)))
		return w
	}
	t.Setenv(txn.TestFaultsEnv, "")
	if w := post(`{"handler": "svc.task.created", "body": "{}"}`); w.Code != http.StatusNotFound {
		t.Fatalf("production: %d", w.Code)
	}
	t.Setenv(txn.TestFaultsEnv, "1")
	if w := post(`{"handler": "nope", "body": "{}"}`); w.Code != http.StatusNotFound {
		t.Errorf("unknown handler: %d", w.Code)
	}
	w := post(`{"handler": "svc.task.created", "body": "not json"}`)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"outcome":"poison"`) {
		t.Errorf("undecodable envelope = poison (no database needed): %d %s", w.Code, w.Body)
	}
}
