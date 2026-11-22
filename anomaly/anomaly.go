// Package anomaly 记录"读到了领域层不认识的数据"(读取宽容: 不报错,但必须能被发现)。
// 生成的映射代码调用 Report;服务器在 /metrics 以 Prometheus 文本格式输出 data_anomaly_total。
package anomaly

import (
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
)

var (
	mu     sync.Mutex
	counts = map[[2]string]uint64{}
	writes = map[[2]string]uint64{} // {table, constraint}
)

// Report: table.column 出现了映射表以外的值。
func Report(table, column string, value any) {
	mu.Lock()
	counts[[2]string{table, column}]++
	mu.Unlock()
	slog.Warn("data anomaly: value not in domain mapping", "table", table, "column", column, "value", value)
}

// ReportWrite: 写入被数据库的规则(CHECK / NOT NULL / 值不合法)拒绝了。请求的校验应该已经拦住,
// 到这里说明设计稿漏了规则: 调用方得到 INVALID_ARGUMENT,同时计入 write_rejected_total(要能被发现)。
func ReportWrite(table, constraint string) {
	mu.Lock()
	writes[[2]string{table, constraint}]++
	mu.Unlock()
	slog.Warn("write rejected by a database rule the request validation did not check", "table", table, "constraint", constraint)
}

// WriteMetrics 以 Prometheus 文本格式输出。
func WriteMetrics(w io.Writer) {
	mu.Lock()
	defer mu.Unlock()
	fmt.Fprintln(w, "# HELP data_anomaly_total Values read from the DB that are not in the domain mapping.")
	fmt.Fprintln(w, "# TYPE data_anomaly_total counter")
	keys := make([][2]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i][0]+keys[i][1] < keys[j][0]+keys[j][1] })
	for _, k := range keys {
		fmt.Fprintf(w, "data_anomaly_total{table=%q,column=%q} %d\n", k[0], k[1], counts[k])
	}
	fmt.Fprintln(w, "# HELP write_rejected_total Writes rejected by a database rule (CHECK / NOT NULL) that request validation did not check.")
	fmt.Fprintln(w, "# TYPE write_rejected_total counter")
	keys = keys[:0]
	for k := range writes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i][0]+keys[i][1] < keys[j][0]+keys[j][1] })
	for _, k := range keys {
		fmt.Fprintf(w, "write_rejected_total{table=%q,constraint=%q} %d\n", k[0], k[1], writes[k])
	}
}
