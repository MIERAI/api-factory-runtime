package metrics

// 状态机(R5)的指标。和消息的指标一样是 Prometheus 的文本格式;没有状态机的服务什么都不输出。
//
//	factory_sm_transitions_total{machine,from,to,trigger}  counter   每条边的流量;trigger = 消息类型 / timeout(重发) / deadline
//	factory_sm_messages_ignored_total{machine,reason}      counter   迟到 / 重复的消息(late)
//	factory_sm_messages_rejected_total{machine,reason}     counter   守卫为假(guard)/ 不可能的消息(impossible)/ 实体不存在(not_found)→ DLQ
//	factory_sm_instances{machine,state}                    gauge     每个等待状态的实体数(超时扫描顺带算)
//	factory_sm_oldest_in_state_seconds{machine,state}      gauge     停留最久的实体的停留时间("订单 N 分钟内到达终态"的 SLO 数据源)
//	factory_sm_timeout_backlog{machine}                    gauge     一轮满了上限的个数(没扫完)
//	factory_sm_completion_seconds{machine,state}           histogram 创建到进入终态的耗时

import (
	"fmt"
	"io"
	"sort"
	"time"
)

// CompletionBuckets: 创建到终态的耗时(秒)。正常几秒;依赖停机时到几分钟。
var CompletionBuckets = []float64{0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800}

var (
	smTransitions = map[[4]string]uint64{}
	smIgnored     = map[[2]string]uint64{}
	smRejected    = map[[2]string]uint64{}
	smInstances   = map[[2]string]int64{}
	smOldest      = map[[2]string]float64{}
	smBacklog     = map[string]int{}
	smCompletion  = map[string]*hist{} // machine \x00 state
)

// MachineTransition: 一次转移(或重发: from = to,trigger = timeout)。
func MachineTransition(machine, from, to, trigger string) {
	mu.Lock()
	defer mu.Unlock()
	smTransitions[[4]string{machine, from, to, trigger}]++
}

// MachineIgnored / MachineRejected: 不能转移的消息。
func MachineIgnored(machine, reason string) {
	mu.Lock()
	defer mu.Unlock()
	smIgnored[[2]string{machine, reason}]++
}

func MachineRejected(machine, reason string) {
	mu.Lock()
	defer mu.Unlock()
	smRejected[[2]string{machine, reason}]++
}

// MachineState: 一个等待状态的实体数 / 停留最久的秒数(超时扫描每轮)。
func MachineState(machine, state string, n int64, oldest float64) {
	mu.Lock()
	defer mu.Unlock()
	smInstances[[2]string{machine, state}] = n
	smOldest[[2]string{machine, state}] = oldest
}

// MachineBacklog: 一轮没扫完的个数。
func MachineBacklog(machine string, n int) {
	mu.Lock()
	defer mu.Unlock()
	smBacklog[machine] = n
}

// MachineCompleted: 进入终态。
func MachineCompleted(machine, state string, d time.Duration) {
	observe(smCompletion, CompletionBuckets, machine+"\x00"+state, d)
}

// MachineTransitions: 测试用的读数。
func MachineTransitions(machine, from, to, trigger string) uint64 {
	mu.Lock()
	defer mu.Unlock()
	return smTransitions[[4]string{machine, from, to, trigger}]
}

// WriteMachines: 状态机的指标(server 的 /metrics)。
func WriteMachines(w io.Writer) {
	mu.Lock()
	defer mu.Unlock()
	if len(smTransitions) > 0 {
		fmt.Fprintln(w, "# HELP factory_sm_transitions_total State machine transitions by edge; trigger = message type, timeout (resend) or deadline.")
		fmt.Fprintln(w, "# TYPE factory_sm_transitions_total counter")
		keys := make([][4]string, 0, len(smTransitions))
		for k := range smTransitions {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
		for _, k := range keys {
			fmt.Fprintf(w, "factory_sm_transitions_total{machine=%q,from=%q,to=%q,trigger=%q} %d\n", k[0], k[1], k[2], k[3], smTransitions[k])
		}
	}
	write2 := func(name, help, typ string, m map[[2]string]uint64, label string) {
		if len(m) == 0 {
			return
		}
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
		for _, k := range sorted2(m) {
			fmt.Fprintf(w, "%s{machine=%q,%s=%q} %d\n", name, k[0], label, k[1], m[k])
		}
	}
	write2("factory_sm_messages_ignored_total", "Late or duplicate messages a state machine ignored.", "counter", smIgnored, "reason")
	write2("factory_sm_messages_rejected_total", "Messages a state machine rejected to the DLQ (guard false, impossible, not found).", "counter", smRejected, "reason")
	if len(smInstances) > 0 {
		fmt.Fprintln(w, "# HELP factory_sm_instances Instances per waiting state (computed by the timeout scanner).")
		fmt.Fprintln(w, "# TYPE factory_sm_instances gauge")
		for _, k := range sorted2(smInstances) {
			fmt.Fprintf(w, "factory_sm_instances{machine=%q,state=%q} %d\n", k[0], k[1], smInstances[k])
		}
		fmt.Fprintln(w, "# HELP factory_sm_oldest_in_state_seconds How long the oldest instance has been in a waiting state.")
		fmt.Fprintln(w, "# TYPE factory_sm_oldest_in_state_seconds gauge")
		for _, k := range sorted2(smOldest) {
			fmt.Fprintf(w, "factory_sm_oldest_in_state_seconds{machine=%q,state=%q} %g\n", k[0], k[1], smOldest[k])
		}
	}
	if len(smBacklog) > 0 {
		fmt.Fprintln(w, "# HELP factory_sm_timeout_backlog Due instances left after a full timeout round.")
		fmt.Fprintln(w, "# TYPE factory_sm_timeout_backlog gauge")
		for _, k := range sortedKeys(smBacklog) {
			fmt.Fprintf(w, "factory_sm_timeout_backlog{machine=%q} %d\n", k, smBacklog[k])
		}
	}
	if len(smCompletion) > 0 {
		name := "factory_sm_completion_seconds"
		fmt.Fprintf(w, "# HELP %s Time from creation to a final state.\n# TYPE %s histogram\n", name, name)
		for _, k := range sortedKeys(smCompletion) {
			h := smCompletion[k]
			machine, state := k, ""
			for i := range k {
				if k[i] == 0 {
					machine, state = k[:i], k[i+1:]
					break
				}
			}
			lbl := fmt.Sprintf("machine=%q,state=%q", machine, state)
			var cum uint64
			for i, b := range CompletionBuckets {
				cum += h.counts[i]
				fmt.Fprintf(w, "%s_bucket{%s,le=%q} %d\n", name, lbl, fmt.Sprint(b), cum)
			}
			fmt.Fprintf(w, "%s_bucket{%s,le=\"+Inf\"} %d\n", name, lbl, h.n)
			fmt.Fprintf(w, "%s_sum{%s} %g\n", name, lbl, h.sum)
			fmt.Fprintf(w, "%s_count{%s} %d\n", name, lbl, h.n)
		}
	}
}

func sorted2[V any](m map[[2]string]V) [][2]string {
	keys := make([][2]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i][0]+"\x00"+keys[i][1] < keys[j][0]+"\x00"+keys[j][1] })
	return keys
}
