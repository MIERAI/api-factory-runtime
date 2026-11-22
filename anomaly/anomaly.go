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
)

// Report: table.column 出现了映射表以外的值。
func Report(table, column string, value any) {
	mu.Lock()
	counts[[2]string{table, column}]++
	mu.Unlock()
	slog.Warn("data anomaly: value not in domain mapping", "table", table, "column", column, "value", value)
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
}
