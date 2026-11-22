package txn

// 测试用的故障注入(命令工厂设计稿 §5.3)。
//
// ⚖️ D3(待负责人确认): 测试用的钩子放在运行时里,靠环境变量开关。
// 环境变量 FACTORY_TEST_FAULTS=1(只在测试环境设置)时,请求头 X-Factory-Fail-After: k 让执行器在第 k 个写操作
// 之后强制失败(事务回滚,响应是 INTERNAL)。生产环境不设置这个变量: 请求头被忽略(调用方不能让服务器失败),
// 单元测试 TestFaultHeaderIgnoredInProduction 保证这一点。

import (
	"context"
	"net/http"
	"os"
	"strconv"

	"connectrpc.com/connect"
)

// FaultHeader: 第 k 个写操作之后强制失败(k ≥ 1)。
const FaultHeader = "X-Factory-Fail-After"

// TestFaultsEnv: 这个环境变量为 "1" 时才认 FaultHeader。
const TestFaultsEnv = "FACTORY_TEST_FAULTS"

type faultKey struct{}

// WithFault: ctx 里放进"第 k 个写操作之后失败"(测试 / 自己组装 handler 时用)。
func WithFault(ctx context.Context, k int) context.Context {
	return context.WithValue(ctx, faultKey{}, k)
}

func faultFrom(ctx context.Context) int {
	k, _ := ctx.Value(faultKey{}).(int)
	return k
}

// FaultFromHeader: 请求头 → k。testFaults = false(生产)时总是 0;格式不对 → 错误(测试写错了,不能悄悄不注入)。
func FaultFromHeader(h http.Header, testFaults bool) (int, error) {
	if !testFaults {
		return 0, nil
	}
	v := h.Get(FaultHeader)
	if v == "" {
		return 0, nil
	}
	k, err := strconv.Atoi(v)
	if err != nil || k < 1 {
		return 0, connect.NewError(connect.CodeInvalidArgument, &Error{Msg: FaultHeader + ": want an integer >= 1, got " + strconv.Quote(v)})
	}
	return k, nil
}

// TestFaultsEnabled: 进程是不是在测试模式(FACTORY_TEST_FAULTS=1)。
func TestFaultsEnabled() bool { return os.Getenv(TestFaultsEnv) == "1" }

// NewInterceptor: FACTORY_TEST_FAULTS=1 时把请求头 X-Factory-Fail-After 放进 ctx;否则什么都不做。
func NewInterceptor() connect.UnaryInterceptorFunc {
	testFaults := TestFaultsEnabled()
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if !testFaults {
				return next(ctx, req)
			}
			k, err := FaultFromHeader(req.Header(), true)
			if err != nil {
				return nil, err
			}
			if k > 0 {
				ctx = WithFault(ctx, k)
			}
			return next(ctx, req)
		}
	}
}
