// Package conv 是 proto 类型和查询类型之间的转换。生成的映射代码使用,AI 不需要直接调用。
package conv

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// TimePtr: 未指定(nil)→ nil(查询里 NULL = 不筛选)
func TimePtr(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}

// TS: time.Time → Timestamp
func TS(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

// TSPtr: 可空列 → Timestamp(NULL → 未设置)
func TSPtr(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

// 日期(DB 的 date)在 API 里是 "YYYY-MM-DD" 字符串。格式由 protovalidate 的正则先检查过。
const dateLayout = "2006-01-02"

// Date: "2026-11-01" → time.Time(UTC 0 点)
func Date(s string) time.Time {
	t, _ := time.Parse(dateLayout, s)
	return t
}

// DatePtr: 可选的日期(nil = 未指定)
func DatePtr(s *string) *time.Time {
	if s == nil {
		return nil
	}
	t := Date(*s)
	return &t
}

func DateString(t time.Time) string { return t.Format(dateLayout) }

func DateStringPtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := DateString(*t)
	return &s
}

// Dates: 日期的列表(格式由 protovalidate 先检查过)。
func Dates(ss []string) []time.Time {
	out := make([]time.Time, len(ss))
	for i, s := range ss {
		out[i] = Date(s)
	}
	return out
}

// Ptr: 值 → 指针(查询的列不为空、响应的字段是可选的时候)。
func Ptr[T any](v T) *T { return &v }
