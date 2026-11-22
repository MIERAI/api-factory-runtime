package consume

// 测试用的崩溃点(R4 设计稿 §7.3)。和 txn 的故障注入同一个约定: 只在 FACTORY_TEST_FAULTS=1 时生效,
// 生产环境不设置这个变量 → 消息的 header 被忽略(发消息的人不能让服务器崩溃;单元测试 TestCrashHeaderIgnoredInProduction)。
//
// 消息的 AMQP header x-factory-crash 指定崩溃点,崩溃 = os.Exit(137)(模拟 kill -9):
//
//	before_commit        处理器执行完、提交之前   → 没有任何写入;重启后重投、处理一次
//	after_commit         提交之后、ack 之前       → 重启后重投 → duplicate → 状态不变、没有新消息
//	after_retry_publish  重试副本发出之后、ack 原消息之前(处理器要失败才会走到) → 两份都在 → 只生效一次
//
// 只在第一次投递时崩溃(重投的 Redelivered = true 时不再崩溃,否则会一直崩);发到重试队列 / DLQ 的副本不带这个 header。

import (
	"net/http"
	"os"

	"github.com/MIERAI/api-factory-runtime/clock"
	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/txn"
)

// 崩溃点。
const (
	CrashBeforeCommit      = "before_commit"
	CrashAfterCommit       = "after_commit"
	CrashAfterRetryPublish = "after_retry_publish"
)

// crashPoint: 这条消息的崩溃点(生产环境 / 重投 → "")。
func (c *Consumer) crashPoint(d Delivery) string {
	if !txn.TestFaultsEnabled() || d.Redelivered {
		return ""
	}
	return message.HeaderString(d.Headers, message.HeaderCrash)
}

// exit: 崩溃(测试里换掉)。137 = 被 SIGKILL 杀掉的退出码。
var exit = func() { os.Exit(137) }

func testClock() bool { return os.Getenv(clock.TestClockEnv) == "1" }

// headerView: AMQP header 的一个值 → HTTP 头(复用 clock / txn 解析 HTTP 头的函数)。
func headerView(h map[string]any, amqpKey, httpKey string) http.Header {
	out := http.Header{}
	if v := message.HeaderString(h, amqpKey); v != "" {
		out.Set(httpKey, v)
	}
	return out
}
