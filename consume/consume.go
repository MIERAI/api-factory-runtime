// Package consume: 消息处理器的消费循环(R4 设计稿 §4.2 / §4.3)。
//
// 一条消息:
//
//	解 envelope + 检查类型 / 版本      失败 → 毒消息: 副本发到 <队列>.dlq,ack
//	BEGIN(txn.Executor.InTx)
//	  INSERT factory_processed_messages (handler, id) ON CONFLICT DO NOTHING
//	  └─ 冲突 = 处理过了 → COMMIT,ack,计 duplicate                  (传输层去重)
//	  Handler(ctx, tx, envelope)        生成的代码: 解码 data、执行操作(txn.RunIn / txn.Savepoint)、写 outbox
//	COMMIT
//	ack                                                              (先提交,再确认)
//
// 处理器返回错误:
//   - Poison(err) 包起来的 → 毒消息,不重试,直接 DLQ(生成的解码器: 缺必填字段 / 类型不对)
//   - 其他错误、panic、超时(Timeout,默认 10s)→ 暂时性: 副本发到分级的重试队列(header x-factory-attempt = 下一次是第几次),
//     publisher confirm 之后再 ack 原消息;第 MaxAttempts 次仍失败 → DLQ(带最后的错误)
//
// 业务结果(库存不足等)不是错误: 处理器自己走声明的分支、提交(txn.Savepoint 回滚已做的操作)。
//
// 测试的进程内驱动: Process(ctx, Delivery) 只做判定和数据库事务,不碰 RabbitMQ;Run 把判定变成 ack / 重试 / DLQ。
package consume

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/MIERAI/api-factory-runtime/broker"
	"github.com/MIERAI/api-factory-runtime/clock"
	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/metrics"
	"github.com/MIERAI/api-factory-runtime/topology"
	"github.com/MIERAI/api-factory-runtime/txn"
)

// Handler: 处理一条消息(生成的代码)。在 tx 里做全部写入(操作、后续消息的 outbox);返回 nil = 提交。
// 可能被调用多次(序列化失败的重来、重试、重投): 除了 tx 以外不能有副作用。
type Handler func(ctx context.Context, tx pgx.Tx, env message.Envelope) error

// Accept: 处理器接受的消息类型和版本(契约)。
type Accept struct {
	Type     string
	Versions []int // 空 = 任何版本
}

// Consumer: 一个队列的处理器。零值的可选字段用默认值。
type Consumer struct {
	Queue   string   // 主队列(topology.QueueName(服务, 类型))
	Name    string   // 去重的处理器名(factory_processed_messages.handler);默认 = Queue
	Accept  []Accept // 空 = 不检查类型 / 版本
	Handler Handler
	Exec    *txn.Executor
	Broker  *broker.Conn

	Retry         topology.RetryPolicy // 零值 = topology.DefaultRetry(必须和声明队列时的一致)
	Prefetch      int                  // 同时处理几条(默认 16)
	Timeout       time.Duration        // 一条消息的事务上限(默认 10s;超时 = 暂时性错误)
	DLQCheckEvery time.Duration        // 多久查一次 DLQ 的长度(默认 30s;< 0 = 不查)
	Log           *slog.Logger

	once sync.Once
}

// Outcome: 一条消息的处理结果。
type Outcome uint8

const (
	OK        Outcome = iota + 1 // 处理并提交了
	Duplicate                    // 处理过了(同一个 id),跳过
	Retry                        // 暂时性错误: 副本进重试队列
	Exhausted                    // 重试用尽: 进 DLQ
	Poisoned                     // 毒消息: 不重试,进 DLQ
)

func (o Outcome) String() string {
	switch o {
	case OK:
		return "ok"
	case Duplicate:
		return "duplicate"
	case Retry:
		return "retry"
	case Exhausted:
		return "dlq"
	case Poisoned:
		return "poison"
	}
	return fmt.Sprintf("outcome(%d)", o)
}

// Delivery: 收到的一条消息(和传输无关: Run 从 AMQP 的 delivery 转换;进程内的测试驱动直接构造)。
type Delivery struct {
	Body        []byte
	MessageID   string
	Headers     map[string]any
	Redelivered bool
}

// Result: Process 的判定。
type Result struct {
	Outcome  Outcome
	Envelope message.Envelope // 解不开时是零值
	Attempt  int              // 这是第几次处理(从 1 开始)
	Tier     int              // Outcome = Retry 时: 第几级重试队列(从 1 开始)
	Err      error            // Retry / Exhausted / Poisoned 的原因
	Business string           // Outcome = OK 且处理器调用了 Business(ctx, 结果) 时: 声明过的业务结果(例 INSUFFICIENT_STOCK)
}

type businessKey struct{}

// Business: 处理器走了声明过的业务结果分支(不是错误: 照样提交、ack)。只影响指标(outcome = "business")和 Result.Business;
// 在 Handler 里调用,序列化失败重来时以最后一次为准。
func Business(ctx context.Context, result string) {
	if p, ok := ctx.Value(businessKey{}).(*string); ok {
		*p = result
	}
}

// Poison: 把 err 标成毒消息(不重试,直接 DLQ)。生成的解码器 / 契约校验失败时用。
func Poison(err error) error { return &poisonError{err} }

// IsPoison: err 是不是毒消息。
func IsPoison(err error) bool {
	var p *poisonError
	return errors.As(err, &p) || message.IsDecodeError(err)
}

type poisonError struct{ err error }

func (p *poisonError) Error() string { return "poison message: " + p.err.Error() }
func (p *poisonError) Unwrap() error { return p.err }

func (c *Consumer) defaults() { c.once.Do(c.setDefaults) }

func (c *Consumer) setDefaults() {
	if c.Name == "" {
		c.Name = c.Queue
	}
	if c.Retry.MaxAttempts == 0 && c.Retry.Tiers == nil {
		c.Retry = topology.DefaultRetry
	}
	if c.Prefetch <= 0 {
		c.Prefetch = 16
	}
	if c.Timeout <= 0 {
		c.Timeout = 10 * time.Second
	}
	if c.DLQCheckEvery == 0 {
		c.DLQCheckEvery = 30 * time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

func (c *Consumer) accepts(env message.Envelope) bool {
	if len(c.Accept) == 0 {
		return true
	}
	for _, a := range c.Accept {
		if a.Type != env.Type {
			continue
		}
		if len(a.Versions) == 0 {
			return true
		}
		for _, v := range a.Versions {
			if v == env.Version {
				return true
			}
		}
	}
	return false
}

// Process: 一条消息的判定 + 数据库事务(去重 + 处理器),不碰 RabbitMQ。Run 按结果 ack / 重试 / 进 DLQ;
// 进程内的测试驱动直接调用它(ack / nack 自己记)。结果计入 factory_messages_total。
func (c *Consumer) Process(ctx context.Context, d Delivery) Result {
	c.defaults()
	res := c.process(ctx, d)
	outcome := res.Outcome.String()
	if res.Outcome == OK && res.Business != "" {
		outcome = "business"
	}
	metrics.Message(c.Name, outcome)
	return res
}

func (c *Consumer) process(ctx context.Context, d Delivery) Result {
	env, err := message.Decode(d.Body)
	if err != nil {
		return Result{Outcome: Poisoned, Attempt: 1, Err: err}
	}
	attempt, ok := message.HeaderInt(d.Headers, message.HeaderAttempt)
	if !ok || attempt < 1 {
		attempt = 1
	}
	res := Result{Envelope: env, Attempt: attempt}
	if !c.accepts(env) {
		res.Outcome, res.Err = Poisoned, fmt.Errorf("handler %s does not accept %s v%d", c.Name, env.Type, env.Version)
		return res
	}
	ctx, err = c.context(ctx, d, env, attempt)
	if err != nil {
		res.Outcome, res.Err = Poisoned, err
		return res
	}
	crash := c.crashPoint(d)
	start := time.Now()
	tctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	var dup bool
	var business string
	tctx = context.WithValue(tctx, businessKey{}, &business)
	err = c.Exec.InTx(tctx, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO factory_processed_messages (handler, message_id, processed_at)
VALUES ($1::text, $2::uuid, now()) ON CONFLICT DO NOTHING`, c.Name, env.ID)
		if err != nil {
			return err
		}
		business = ""
		if dup = tag.RowsAffected() == 0; dup {
			return nil // 处理过了: 提交(什么都没写),ack
		}
		if err := c.call(ctx, tx, env); err != nil {
			return err
		}
		if crash == CrashBeforeCommit {
			exit()
		}
		return nil
	})
	metrics.HandlerDuration(c.Name, time.Since(start))
	switch {
	case err == nil && dup:
		res.Outcome = Duplicate
	case err == nil:
		res.Outcome, res.Business = OK, business
		metrics.MessageLag(c.Name, time.Since(env.OccurredAt))
	case IsPoison(err):
		res.Outcome, res.Err = Poisoned, err
	default:
		res.Err = err
		if res.Tier = c.Retry.Tier(attempt); res.Tier == 0 {
			res.Outcome = Exhausted
		} else {
			res.Outcome = Retry
		}
	}
	return res
}

// call: 处理器;panic → 暂时性错误(生成代码 / 业务纯函数的 bug: 重试几次后进 DLQ,并计 panic 的指标)。
func (c *Consumer) call(ctx context.Context, tx pgx.Tx, env message.Envelope) (err error) {
	defer func() {
		if p := recover(); p != nil {
			metrics.HandlerPanic(c.Name)
			c.Log.Error("message handler panicked", "handler", c.Name, "id", env.ID, "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panicked: %v", p)
		}
	}()
	return c.Handler(ctx, tx, env)
}

// context: 处理器的 ctx: 平台时钟(处理开始的时刻;测试时 header x-factory-now)、追踪(收到的 traceparent 的子 span)、
// 测试的故障注入(header x-factory-fail-after)。测试 header 写错了 → 错误(当毒消息: 测试写错了,不能悄悄不注入)。
func (c *Consumer) context(ctx context.Context, d Delivery, env message.Envelope, attempt int) (context.Context, error) {
	now, err := clock.FromHeader(headerView(d.Headers, message.HeaderNow, clock.Header), testClock(), time.Now)
	if err != nil {
		return ctx, err
	}
	ctx = clock.With(ctx, now)
	if tp := message.ChildTraceparent(env.Traceparent); tp != "" {
		ctx = message.WithTraceparent(ctx, tp)
	}
	k, err := txn.FaultFromHeader(headerView(d.Headers, message.HeaderFailAfter, txn.FaultHeader), txn.TestFaultsEnabled())
	if err != nil {
		return ctx, err
	}
	if n, ok := message.HeaderInt(d.Headers, message.HeaderFailAttempts); ok && txn.TestFaultsEnabled() && k == 0 && attempt <= n {
		k = 1 // 前 n 次: 第一个写操作之后失败
	}
	if k > 0 {
		ctx = txn.WithFault(ctx, k)
	}
	return ctx, nil
}
