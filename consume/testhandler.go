package consume

// 测试环境的进程内投递(R4 设计稿 §7.2 的"进程内"一层): 验收的驱动不经 RabbitMQ,把一条消息直接交给处理器
// (Process: 判定 + 数据库事务),拿回结果(ack / 重试 / DLQ 由驱动自己记)。只在 FACTORY_TEST_FAULTS=1 时挂载
// (和 txn 的故障注入同一个约定);生产环境 404,单元测试 TestTestHandlerOffInProduction 保证。
//
//	POST /factory.test/deliver
//	{"handler": "<处理器名 = Consumer.Name>", "body": "<envelope 的 JSON 文本>", "headers": {"x-factory-fail-after": "2", …},
//	 "redelivered": false}
//	→ 200 {"outcome": "ok|duplicate|retry|dlq|poison", "business": "INSUFFICIENT_STOCK", "attempt": 1, "tier": 0, "error": "…"}

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/MIERAI/api-factory-runtime/txn"
)

// TestPath: 进程内投递的路径(生成的 wiring 在测试环境挂载)。
const TestPath = "/factory.test/deliver"

// TestRequest / TestResult: 进程内投递的请求 / 结果。
type TestRequest struct {
	Handler     string         `json:"handler"`
	Body        string         `json:"body"`
	Headers     map[string]any `json:"headers,omitempty"` // 值: 文本(数字也写成文本)
	Redelivered bool           `json:"redelivered,omitempty"`
}

type TestResult struct {
	Outcome  string `json:"outcome"`
	Business string `json:"business,omitempty"`
	Attempt  int    `json:"attempt"`
	Tier     int    `json:"tier,omitempty"`
	Error    string `json:"error,omitempty"`
}

// TestHandler: 进程内投递的 HTTP handler(cs = 这个服务的全部处理器)。FACTORY_TEST_FAULTS 不是 1 → 一律 404。
func TestHandler(cs ...*Consumer) http.Handler {
	byName := map[string]*Consumer{}
	for _, c := range cs {
		c.defaults()
		byName[c.Name] = c
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !txn.TestFaultsEnabled() {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req TestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		c := byName[req.Handler]
		if c == nil {
			http.Error(w, fmt.Sprintf("no handler %q", req.Handler), http.StatusNotFound)
			return
		}
		h := map[string]any{}
		for k, v := range req.Headers {
			if s, ok := v.(string); ok {
				h[k] = s
			} else {
				h[k] = fmt.Sprint(v)
			}
		}
		res := c.Process(r.Context(), Delivery{Body: []byte(req.Body), Headers: h, Redelivered: req.Redelivered})
		out := TestResult{Outcome: res.Outcome.String(), Business: res.Business, Attempt: res.Attempt, Tier: res.Tier}
		if res.Err != nil {
			out.Error = res.Err.Error()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
}
