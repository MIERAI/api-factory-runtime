// Package metrics: RPC 的请求数(按接口 × 结果码)和延迟的直方图,Prometheus 的文本格式(不引入依赖)。
// 金丝雀发布的分析、告警(错误率 / 延迟的 SLO)用这些指标。
package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// Buckets: 延迟直方图的上界(秒)
var Buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

type hist struct {
	counts []uint64 // 每个 bucket(累积之前)
	sum    float64
	n      uint64
}

var (
	mu       sync.Mutex
	requests = map[[2]string]uint64{} // {procedure, code}
	latency  = map[string]*hist{}     // procedure
)

// Observe: 一个 RPC 的结果(code = "ok" / connect 的错误码)和用时。
func Observe(procedure, code string, d time.Duration) {
	mu.Lock()
	defer mu.Unlock()
	requests[[2]string{procedure, code}]++
	h := latency[procedure]
	if h == nil {
		h = &hist{counts: make([]uint64, len(Buckets))}
		latency[procedure] = h
	}
	s := d.Seconds()
	for i, b := range Buckets {
		if s <= b {
			h.counts[i]++
			break
		}
	}
	h.sum += s
	h.n++
}

// Write: Prometheus 的文本格式。
func Write(w io.Writer) {
	mu.Lock()
	defer mu.Unlock()
	fmt.Fprintln(w, "# HELP rpc_requests_total RPC requests by procedure and result code (ok or the Connect error code).")
	fmt.Fprintln(w, "# TYPE rpc_requests_total counter")
	keys := make([][2]string, 0, len(requests))
	for k := range requests {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i][0]+"\x00"+keys[i][1] < keys[j][0]+"\x00"+keys[j][1] })
	for _, k := range keys {
		fmt.Fprintf(w, "rpc_requests_total{procedure=%q,code=%q} %d\n", k[0], k[1], requests[k])
	}
	fmt.Fprintln(w, "# HELP rpc_request_duration_seconds RPC latency.")
	fmt.Fprintln(w, "# TYPE rpc_request_duration_seconds histogram")
	procs := make([]string, 0, len(latency))
	for p := range latency {
		procs = append(procs, p)
	}
	sort.Strings(procs)
	for _, p := range procs {
		h := latency[p]
		var cum uint64
		for i, b := range Buckets {
			cum += h.counts[i]
			fmt.Fprintf(w, "rpc_request_duration_seconds_bucket{procedure=%q,le=%q} %d\n", p, fmt.Sprint(b), cum)
		}
		fmt.Fprintf(w, "rpc_request_duration_seconds_bucket{procedure=%q,le=\"+Inf\"} %d\n", p, h.n)
		fmt.Fprintf(w, "rpc_request_duration_seconds_sum{procedure=%q} %g\n", p, h.sum)
		fmt.Fprintf(w, "rpc_request_duration_seconds_count{procedure=%q} %d\n", p, h.n)
	}
}

func reset() { // 测试用
	mu.Lock()
	defer mu.Unlock()
	requests, latency = map[[2]string]uint64{}, map[string]*hist{}
}
