package clock

import (
	"context"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
)

func TestFromHeader(t *testing.T) {
	fixed := time.Date(2026, 11, 22, 3, 4, 5, 123456789, time.UTC)
	sys := func() time.Time { return fixed }
	h := http.Header{}
	h.Set(Header, "2026-01-01T09:00:00+09:00")
	// 生产(testClock = false): 请求头被忽略
	if got, _ := FromHeader(h, false, sys); !got.Equal(fixed.Truncate(time.Microsecond)) {
		t.Errorf("production must ignore %s: %v", Header, got)
	}
	// 测试: 请求头就是请求开始的时刻(UTC)
	if got, err := FromHeader(h, true, sys); err != nil || got.Format(time.RFC3339) != "2026-01-01T00:00:00Z" {
		t.Errorf("test clock: %v %v", got, err)
	}
	// 测试但没有请求头: 系统时钟
	if got, _ := FromHeader(http.Header{}, true, sys); !got.Equal(fixed.Truncate(time.Microsecond)) {
		t.Errorf("no header: %v", got)
	}
	h.Set(Header, "yesterday")
	if _, err := FromHeader(h, true, sys); err == nil {
		t.Errorf("bad header must be an error")
	}
}

func TestInterceptor(t *testing.T) {
	t.Setenv(TestClockEnv, "1")
	var seen time.Time
	next := connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		seen = Now(ctx)
		return nil, nil
	})
	req := connect.NewRequest(&struct{}{})
	req.Header().Set(Header, "2026-03-31T15:00:00.5Z")
	if _, err := NewInterceptor()(next)(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if seen.Format(time.RFC3339Nano) != "2026-03-31T15:00:00.5Z" {
		t.Errorf("Now(ctx) = %v", seen)
	}
	req.Header().Set(Header, "nope")
	if _, err := NewInterceptor()(next)(context.Background(), req); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("bad header: %v", err)
	}
	t.Setenv(TestClockEnv, "")
	req.Header().Set(Header, "nope") // 生产: 请求头被忽略(不报错)
	if _, err := NewInterceptor()(next)(context.Background(), req); err != nil {
		t.Errorf("production must ignore the header: %v", err)
	}
	if time.Since(seen) > time.Minute {
		t.Errorf("production Now(ctx) must be the system clock: %v", seen)
	}
}

func TestNowWithoutInterceptor(t *testing.T) {
	if d := time.Since(Now(context.Background())); d < 0 || d > time.Minute {
		t.Errorf("Now without interceptor: %v", d)
	}
}
