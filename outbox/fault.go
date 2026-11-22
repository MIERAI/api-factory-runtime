package outbox

// 测试用的崩溃点(R4 设计稿 §7.3)。和 txn 的故障注入同一个约定: 只在 FACTORY_TEST_FAULTS=1 时生效,
// 生产环境不设置这个变量 → 这里的环境变量被忽略(单元测试 TestRelayCrashIgnoredInProduction 保证)。
//
//	FACTORY_TEST_RELAY_CRASH=after_publish[:n]   第 n 批(默认 1)发布之后、等 confirm 之前 → os.Exit(137)(模拟 kill -9)
//	FACTORY_TEST_RELAY_CRASH=after_confirm[:n]   第 n 批 confirm 之后、标记 published_at 之前 → os.Exit(137)
//
// 两种都应该得到: 重启后同一条消息(同一个 id)再发一次,下游只生效一次。

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/MIERAI/api-factory-runtime/txn"
)

// RelayCrashEnv: 中继的崩溃点。
const RelayCrashEnv = "FACTORY_TEST_RELAY_CRASH"

// 崩溃点。
const (
	CrashAfterPublish = "after_publish"
	CrashAfterConfirm = "after_confirm"
)

type crashPlan struct {
	point string
	batch int // 第几批(非空的批,从 1 开始)
}

// relayCrashFromEnv: testFaults = false(生产)→ 没有崩溃点。写错了 → 错误(测试写错了,不能悄悄不注入)。
func relayCrashFromEnv(testFaults bool, v string) (crashPlan, error) {
	if !testFaults || v == "" {
		return crashPlan{}, nil
	}
	point, n, hasN := strings.Cut(v, ":")
	p := crashPlan{point: point, batch: 1}
	if point != CrashAfterPublish && point != CrashAfterConfirm {
		return crashPlan{}, fmt.Errorf("%s: unknown crash point %q (want %s or %s)", RelayCrashEnv, point, CrashAfterPublish, CrashAfterConfirm)
	}
	if hasN {
		k, err := strconv.Atoi(n)
		if err != nil || k < 1 {
			return crashPlan{}, fmt.Errorf("%s: batch must be an integer >= 1, got %q", RelayCrashEnv, n)
		}
		p.batch = k
	}
	return p, nil
}

func relayCrashPlan() (crashPlan, error) {
	return relayCrashFromEnv(txn.TestFaultsEnabled(), os.Getenv(RelayCrashEnv))
}

// exit: 崩溃(测试里换掉)。137 = 被 SIGKILL 杀掉的退出码。
var exit = func() { os.Exit(137) }
