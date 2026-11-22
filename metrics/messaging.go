package metrics

// 消息(R4)的指标: outbox 的积压、中继的发送、消费者的结果 / 延迟 / 耗时、DLQ 的长度。和 RPC 的指标一样是 Prometheus 的
// 文本格式、不引入依赖;server 的 /metrics 用 WriteMessaging 一起输出(没有任何消息活动时什么都不输出)。
//
//	factory_outbox_pending                       gauge     未发送的行数(只有工作中的中继导出)
//	factory_outbox_oldest_pending_age_seconds    gauge     最老的未发送消息的年龄(同上)
//	factory_relay_leader                         gauge     这个进程的中继是不是工作者(1 / 0)
//	factory_outbox_published_total               counter   发出并确认的消息数
//	factory_outbox_publish_errors_total{reason}  counter   发送失败(nack / timeout / unroutable / broker)
//	factory_outbox_unroutable_total{kind,type}   counter   没有队列接收的消息(命令: 留在 outbox 重试;事件: 丢弃)
//	factory_messages_total{handler,outcome}      counter   ok / business / duplicate / retry / dlq / poison
//	factory_handler_panics_total{handler}        counter   处理器 panic(当暂时性错误重试)
//	factory_message_lag_seconds{handler}         histogram 提交时刻 − occurred_at
//	factory_handler_duration_seconds{handler}    histogram 一次处理(事务)的耗时
//	factory_dlq_messages{queue}                  gauge     DLQ 里的消息数(消费循环定期查)

import (
	"fmt"
	"io"
	"sort"
	"time"
)

// LagBuckets: 消息延迟直方图的上界(秒)。重试链最长约 2 分钟。
var LagBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

type outboxState struct {
	known   bool // 这个进程是工作中的中继(待命的副本不导出积压)
	pending int64
	oldest  float64
	leader  bool
	seen    bool // 有过中继
}

var (
	outbox        outboxState
	published     uint64
	publishErrors = map[string]uint64{}
	unroutable    = map[[2]string]uint64{} // {kind, type}
	messages      = map[[2]string]uint64{} // {handler, outcome}
	panics        = map[string]uint64{}
	lagHist       = map[string]*hist{}
	durHist       = map[string]*hist{}
	dlqSize       = map[string]int64{}
)

// OutboxBacklog: 中继每轮算一次的积压(行数、最老的年龄)。
func OutboxBacklog(pending int64, oldest time.Duration) {
	mu.Lock()
	defer mu.Unlock()
	outbox.known, outbox.pending, outbox.oldest, outbox.seen = true, pending, max(oldest.Seconds(), 0), true
}

// RelayLeader: 这个进程的中继成为 / 不再是工作者。不再是时积压的 gauge 不再导出(由新的工作者导出)。
func RelayLeader(leading bool) {
	mu.Lock()
	defer mu.Unlock()
	outbox.leader, outbox.seen = leading, true
	if !leading {
		outbox.known = false
	}
}

// OutboxPublished: n 条消息发出并确认。
func OutboxPublished(n int) {
	mu.Lock()
	defer mu.Unlock()
	published += uint64(n)
}

// OutboxPublishError: n 条消息发送失败。
func OutboxPublishError(reason string, n int) {
	mu.Lock()
	defer mu.Unlock()
	publishErrors[reason] += uint64(n)
}

// OutboxUnroutable: 一条消息被 RabbitMQ 退回(mandatory: 没有队列接收)。kind = event / command。
func OutboxUnroutable(kind, typ string) {
	mu.Lock()
	defer mu.Unlock()
	unroutable[[2]string{kind, typ}]++
}

// Message: 一条消息的处理结果。
func Message(handler, outcome string) {
	mu.Lock()
	defer mu.Unlock()
	messages[[2]string{handler, outcome}]++
}

// HandlerPanic: 处理器 panic 了。
func HandlerPanic(handler string) {
	mu.Lock()
	defer mu.Unlock()
	panics[handler]++
}

// MessageLag: 提交时刻 − occurred_at。
func MessageLag(handler string, d time.Duration) { observe(lagHist, LagBuckets, handler, d) }

// HandlerDuration: 一次处理的耗时。
func HandlerDuration(handler string, d time.Duration) { observe(durHist, Buckets, handler, d) }

// DLQSize: DLQ 的长度。
func DLQSize(queue string, n int64) {
	mu.Lock()
	defer mu.Unlock()
	dlqSize[queue] = n
}

func observe(m map[string]*hist, buckets []float64, key string, d time.Duration) {
	mu.Lock()
	defer mu.Unlock()
	h := m[key]
	if h == nil {
		h = &hist{counts: make([]uint64, len(buckets))}
		m[key] = h
	}
	s := d.Seconds()
	for i, b := range buckets {
		if s <= b {
			h.counts[i]++
			break
		}
	}
	h.sum += s
	h.n++
}

// WriteMessaging: 消息的指标(Prometheus 文本格式)。
func WriteMessaging(w io.Writer) {
	mu.Lock()
	defer mu.Unlock()
	if outbox.seen {
		if outbox.known {
			fmt.Fprintln(w, "# HELP factory_outbox_pending Unpublished outbox rows (exported by the active relay only).")
			fmt.Fprintln(w, "# TYPE factory_outbox_pending gauge")
			fmt.Fprintf(w, "factory_outbox_pending %d\n", outbox.pending)
			fmt.Fprintln(w, "# HELP factory_outbox_oldest_pending_age_seconds Age of the oldest unpublished outbox message (active relay only).")
			fmt.Fprintln(w, "# TYPE factory_outbox_oldest_pending_age_seconds gauge")
			fmt.Fprintf(w, "factory_outbox_oldest_pending_age_seconds %g\n", outbox.oldest)
		}
		fmt.Fprintln(w, "# HELP factory_relay_leader Whether this process runs the active outbox relay of its database.")
		fmt.Fprintln(w, "# TYPE factory_relay_leader gauge")
		fmt.Fprintf(w, "factory_relay_leader %d\n", b2i(outbox.leader))
		fmt.Fprintln(w, "# HELP factory_outbox_published_total Outbox messages published and confirmed by the broker.")
		fmt.Fprintln(w, "# TYPE factory_outbox_published_total counter")
		fmt.Fprintf(w, "factory_outbox_published_total %d\n", published)
	}
	if len(publishErrors) > 0 {
		fmt.Fprintln(w, "# HELP factory_outbox_publish_errors_total Outbox messages whose publish failed (left in the outbox and retried).")
		fmt.Fprintln(w, "# TYPE factory_outbox_publish_errors_total counter")
		for _, k := range sortedKeys(publishErrors) {
			fmt.Fprintf(w, "factory_outbox_publish_errors_total{reason=%q} %d\n", k, publishErrors[k])
		}
	}
	if len(unroutable) > 0 {
		fmt.Fprintln(w, "# HELP factory_outbox_unroutable_total Messages returned by the broker because no queue is bound (commands stay in the outbox; events are dropped).")
		fmt.Fprintln(w, "# TYPE factory_outbox_unroutable_total counter")
		for _, k := range sorted2(unroutable) {
			fmt.Fprintf(w, "factory_outbox_unroutable_total{kind=%q,type=%q} %d\n", k[0], k[1], unroutable[k])
		}
	}
	if len(messages) > 0 {
		fmt.Fprintln(w, "# HELP factory_messages_total Messages handled, by handler and outcome (ok, business, duplicate, retry, dlq, poison).")
		fmt.Fprintln(w, "# TYPE factory_messages_total counter")
		keys := make([][2]string, 0, len(messages))
		for k := range messages {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i][0]+"\x00"+keys[i][1] < keys[j][0]+"\x00"+keys[j][1] })
		for _, k := range keys {
			fmt.Fprintf(w, "factory_messages_total{handler=%q,outcome=%q} %d\n", k[0], k[1], messages[k])
		}
	}
	if len(panics) > 0 {
		fmt.Fprintln(w, "# HELP factory_handler_panics_total Message handler panics (retried like transient errors).")
		fmt.Fprintln(w, "# TYPE factory_handler_panics_total counter")
		for _, k := range sortedKeys(panics) {
			fmt.Fprintf(w, "factory_handler_panics_total{handler=%q} %d\n", k, panics[k])
		}
	}
	writeHist(w, "factory_message_lag_seconds", "Commit time minus the message's occurred_at.", "handler", LagBuckets, lagHist)
	writeHist(w, "factory_handler_duration_seconds", "Duration of one message handling (one transaction).", "handler", Buckets, durHist)
	if len(dlqSize) > 0 {
		fmt.Fprintln(w, "# HELP factory_dlq_messages Messages in a dead letter queue (checked periodically by the consumer).")
		fmt.Fprintln(w, "# TYPE factory_dlq_messages gauge")
		for _, k := range sortedKeys(dlqSize) {
			fmt.Fprintf(w, "factory_dlq_messages{queue=%q} %d\n", k, dlqSize[k])
		}
	}
}

func writeHist(w io.Writer, name, help, label string, buckets []float64, m map[string]*hist) {
	if len(m) == 0 {
		return
	}
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	for _, k := range sortedKeys(m) {
		h := m[k]
		var cum uint64
		for i, b := range buckets {
			cum += h.counts[i]
			fmt.Fprintf(w, "%s_bucket{%s=%q,le=%q} %d\n", name, label, k, fmt.Sprint(b), cum)
		}
		fmt.Fprintf(w, "%s_bucket{%s=%q,le=\"+Inf\"} %d\n", name, label, k, h.n)
		fmt.Fprintf(w, "%s_sum{%s=%q} %g\n", name, label, k, h.sum)
		fmt.Fprintf(w, "%s_count{%s=%q} %d\n", name, label, k, h.n)
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func resetMessaging() { // 测试用
	mu.Lock()
	defer mu.Unlock()
	outbox, published = outboxState{}, 0
	publishErrors, messages, panics = map[string]uint64{}, map[[2]string]uint64{}, map[string]uint64{}
	unroutable = map[[2]string]uint64{}
	lagHist, durHist, dlqSize = map[string]*hist{}, map[string]*hist{}, map[string]int64{}
}

// UnroutableCount: 测试用的读数(kind × type 被退回的次数)。
func UnroutableCount(kind, typ string) uint64 {
	mu.Lock()
	defer mu.Unlock()
	return unroutable[[2]string{kind, typ}]
}

// MessageCount: 测试用的读数(某个 handler × outcome 的计数)。
func MessageCount(handler, outcome string) uint64 {
	mu.Lock()
	defer mu.Unlock()
	return messages[[2]string{handler, outcome}]
}
