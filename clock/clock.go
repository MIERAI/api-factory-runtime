// Package clock 是请求开始的时刻(查询里的 now())。生成的 handler 用 clock.Now(ctx) 取它,不直接读系统时钟:
// 同一个请求里的 now() 只有一个值,测试时可以固定。
//
// 测试时钟: 环境变量 FACTORY_TEST_CLOCK=1(只在测试环境设置)时,请求头 X-Factory-Now(RFC 3339)就是请求开始的时刻。
// 生产环境不设置这个变量,请求头被忽略(调用方不能改服务器的时间)。
package clock

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
)

// Header: 测试时固定请求开始时刻的请求头。
const Header = "X-Factory-Now"

// TestClockEnv: 这个环境变量为 "1" 时才认 Header。
const TestClockEnv = "FACTORY_TEST_CLOCK"

type key struct{}

// Now: 请求开始的时刻(UTC,微秒精度 = Postgres timestamptz 的精度)。拦截器没有设置时(例: 单元测试直接调用 handler)= 现在。
func Now(ctx context.Context) time.Time {
	if t, ok := ctx.Value(key{}).(time.Time); ok {
		return t
	}
	return time.Now().UTC().Truncate(time.Microsecond)
}

// With: ctx 里放进请求开始的时刻(测试 / 自己组装 handler 时用)。
func With(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, key{}, t.UTC().Truncate(time.Microsecond))
}

// FromHeader: 请求开始的时刻。testClock = false(生产)时不看请求头。请求头的格式不对 → 错误(测试写错了,不能悄悄用系统时钟)。
func FromHeader(h http.Header, testClock bool, now func() time.Time) (time.Time, error) {
	if testClock {
		if v := h.Get(Header); v != "" {
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return time.Time{}, errors.New(Header + ": not RFC 3339: " + v)
			}
			return t.UTC().Truncate(time.Microsecond), nil
		}
	}
	return now().UTC().Truncate(time.Microsecond), nil
}

// NewInterceptor: 每个请求开始时记下时刻(FACTORY_TEST_CLOCK=1 时可以由请求头 X-Factory-Now 指定)。
func NewInterceptor() connect.UnaryInterceptorFunc {
	testClock := os.Getenv(TestClockEnv) == "1"
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			t, err := FromHeader(req.Header(), testClock, time.Now)
			if err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
			return next(With(ctx, t), req)
		}
	}
}
