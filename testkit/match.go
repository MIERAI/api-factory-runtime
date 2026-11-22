// Package testkit: 生成的测试用的工具(只在 *_test.go 里用)。
package testkit

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MatchField: 响应 msg 的字段 field(proto 的字段名)和期待值 wantJSON 做子集匹配:
// 对象只比较写出来的键;列表比较长度和每一项;数字按数值比较(int64 / 十进制字符串都可以);"$absent" = 没有设置。
func MatchField(t testing.TB, msg proto.Message, field, wantJSON string) {
	t.Helper()
	b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(msg)
	if err != nil {
		t.Fatalf("protojson: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("json: %v", err)
	}
	var want any
	if err := json.Unmarshal([]byte(wantJSON), &want); err != nil {
		t.Fatalf("want: %v", err)
	}
	if msg := match(field, want, got[field]); msg != "" {
		t.Error(msg)
	}
}

func match(path string, want, got any) string {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return fmt.Sprintf("%s: want object, got %v", path, got)
		}
		for k, wv := range w {
			if m := match(path+"."+k, wv, g[k]); m != "" {
				return m
			}
		}
		return ""
	case []any:
		g, _ := got.([]any)
		if len(g) != len(w) {
			return fmt.Sprintf("%s: want %d item(s), got %d", path, len(w), len(g))
		}
		for i := range w {
			if m := match(fmt.Sprintf("%s[%d]", path, i), w[i], g[i]); m != "" {
				return m
			}
		}
		return ""
	case string:
		if w == "$absent" {
			if got != nil {
				return fmt.Sprintf("%s: want unset, got %v", path, got)
			}
			return ""
		}
	}
	if wn, ok := num(want); ok {
		if gn, ok := num(got); ok && wn == gn {
			return ""
		}
	}
	if fmt.Sprint(want) != fmt.Sprint(got) {
		return fmt.Sprintf("%s: want %v, got %v", path, want, got)
	}
	return ""
}

func num(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	}
	return 0, false
}
