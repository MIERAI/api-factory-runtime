package server

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// 探针不碰数据库: 没有连接池也能回答;停机开始后 readyz 变 503,livez 不变。
func TestProbesWithoutDatabase(t *testing.T) {
	t.Parallel()
	var stopping atomic.Bool
	mux := http.NewServeMux()
	addProbes(mux, &stopping, func() []string { return nil })
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

// G19: 登记过的"准备好了"的条件全部 Done 之前 readyz 是 503(列出还没满足的),livez 一直 200。
func TestReadyzWaitsForWorkerSignals(t *testing.T) {
	t.Parallel()
	var w Workers
	topo := w.Ready("topology")
	sub := w.Ready("consume a.b")
	var stopping atomic.Bool
	mux := http.NewServeMux()
	addProbes(mux, &stopping, w.Pending)
	get := func(path string) (int, string) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		return rec.Code, rec.Body.String()
	}
	if code, body := get("/readyz"); code != 503 || !strings.Contains(body, "consume a.b, topology") {
		t.Fatalf("nothing done: %d %q", code, body)
	}
	if code, _ := get("/livez"); code != 200 {
		t.Fatalf("livez %d", code)
	}
	topo.Done()
	topo.Done() // 多次调用无害
	if code, body := get("/readyz"); code != 503 || strings.Contains(body, "topology") {
		t.Fatalf("topology done: %d %q", code, body)
	}
	sub.Done()
	if code, _ := get("/readyz"); code != 200 {
		t.Fatalf("all done: %d", code)
	}
	select {
	case <-topo.C():
	default:
		t.Fatal("C must be closed after Done")
	}
}

// probe: 200 → 0;503 / 连不上 → 1(信息写到 stderr)。
func TestProbe(t *testing.T) {
	code := 200
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			w.WriteHeader(404)
			return
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte("not ready: topology"))
	})}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	t.Setenv("PORT", strings.TrimPrefix(ln.Addr().String(), "127.0.0.1:"))
	var stderr bytes.Buffer
	if rc := probe(&stderr); rc != 0 {
		t.Fatalf("200 → %d (%s)", rc, stderr.String())
	}
	code = 503
	if rc := probe(&stderr); rc != 1 || !strings.Contains(stderr.String(), "503 not ready: topology") {
		t.Fatalf("503 → %d (%s)", rc, stderr.String())
	}
	srv.Close()
	if rc := probe(&stderr); rc != 1 {
		t.Fatalf("no server → %d", rc)
	}
}
