package message

// 追踪的传递(W3C traceparent)。运行时不依赖追踪库: 只把 traceparent 从入口(HTTP 请求头 / 收到的消息)带到
// 发出的消息里。收到消息时取它的 traceparent 生成一个子 span id(同一个 trace),处理中发出的消息带这个子 span。

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"connectrpc.com/connect"
)

type traceKey struct{}

// WithTraceparent: ctx 里放进当前的 traceparent(不合法的值忽略)。
func WithTraceparent(ctx context.Context, tp string) context.Context {
	if !ValidTraceparent(tp) {
		return ctx
	}
	return context.WithValue(ctx, traceKey{}, tp)
}

// Traceparent: ctx 里的 traceparent(没有 → "")。outbox.Write 把它写进 envelope。
func Traceparent(ctx context.Context) string {
	tp, _ := ctx.Value(traceKey{}).(string)
	return tp
}

// ChildTraceparent: 同一个 trace 里新的 span(父不合法 → "")。
func ChildTraceparent(parent string) string {
	if !ValidTraceparent(parent) {
		return ""
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	b[0] |= 1 // 全 0 的 span id 不合法
	return parent[:36] + hex.EncodeToString(b[:]) + parent[52:]
}

// ValidTraceparent: "00-<32 位十六进制 trace id>-<16 位十六进制 span id>-<2 位 flags>",id 不全为 0。
func ValidTraceparent(tp string) bool {
	if len(tp) != 55 || tp[2] != '-' || tp[35] != '-' || tp[52] != '-' || tp[:2] != "00" {
		return false
	}
	for _, part := range []string{tp[3:35], tp[36:52], tp[53:]} {
		if _, err := hex.DecodeString(part); err != nil {
			return false
		}
	}
	return tp[3:35] != "00000000000000000000000000000000" && tp[36:52] != "0000000000000000"
}

// NewTraceInterceptor: 把 HTTP 请求头 traceparent 放进 ctx(写操作发出的消息带上它)。
func NewTraceInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if tp := req.Header().Get("traceparent"); tp != "" {
				ctx = WithTraceparent(ctx, tp)
			}
			return next(ctx, req)
		}
	}
}
