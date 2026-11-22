package server

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// 探针不碰数据库: 没有连接池也能回答;停机开始后 readyz 变 503,livez 不变。
func TestProbesWithoutDatabase(t *testing.T) {
	t.Parallel()
	var stopping atomic.Bool
	mux := http.NewServeMux()
	addProbes(mux, &stopping)
	get := func(path string) int {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		return w.Code
	}
	if get("/livez") != 200 || get("/readyz") != 200 {
		t.Fatalf("running: livez %d readyz %d", get("/livez"), get("/readyz"))
	}
	stopping.Store(true)
	if get("/livez") != 200 || get("/readyz") != 503 {
		t.Fatalf("stopping: livez %d readyz %d", get("/livez"), get("/readyz"))
	}
}
