// Package apierr 是 handler 返回错误的唯一入口。
//
// handler 只需要:
//   - 业务上的错误 → 用 NotFound / InvalidArgument / FailedPrecondition 包装后返回
//   - 其他 error(DB 错误等) → 原样返回,平台拦截器会转成 UNAVAILABLE / INTERNAL,
//     并且不会把内部细节泄露给调用方
package apierr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// NotFound 表示请求的资源不存在。
func NotFound(format string, args ...any) error {
	return connect.NewError(connect.CodeNotFound, fmt.Errorf(format, args...))
}

// InvalidArgument 表示请求参数不合法(proto 校验规则无法表达的业务校验时使用)。
func InvalidArgument(format string, args ...any) error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(format, args...))
}

// FailedPrecondition 表示当前状态下无法执行该操作。
func FailedPrecondition(format string, args ...any) error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(format, args...))
}

// IsNoRows 判断 cardinality: one 的查询是否没有查到记录。
// handler 用它代替直接 import pgx。
func IsNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

// NewSanitizeInterceptor 把 handler 返回的非 connect 错误转换成安全的 gRPC code。
func NewSanitizeInterceptor(log *slog.Logger) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			res, err := next(ctx, req)
			if err == nil {
				return res, nil
			}
			var ce *connect.Error
			if errors.As(err, &ce) {
				return res, err
			}
			code := classify(err)
			log.ErrorContext(ctx, "handler error", "procedure", req.Spec().Procedure, "code", code.String(), "err", err)
			return nil, connect.NewError(code, errors.New(publicMessage(code)))
		}
	}
}

func classify(err error) connect.Code {
	var netErr net.Error
	var connectErr *pgconn.ConnectError
	switch {
	case errors.Is(err, context.DeadlineExceeded), pgconn.Timeout(err):
		return connect.CodeUnavailable
	case errors.As(err, &connectErr), errors.As(err, &netErr):
		return connect.CodeUnavailable
	case errors.Is(err, context.Canceled):
		return connect.CodeCanceled
	default:
		return connect.CodeInternal
	}
}

func publicMessage(code connect.Code) string {
	switch code {
	case connect.CodeUnavailable:
		return "service temporarily unavailable"
	case connect.CodeCanceled:
		return "request canceled"
	default:
		return "internal error"
	}
}
