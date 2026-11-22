// Package topology 声明 RabbitMQ 上的交换机、队列、重试队列和死信队列(R4 设计稿 §4.1)。
//
// 生成的服务启动时按生成的清单(Spec)调用 Declare: 幂等;已有的同名对象参数不一致 → RabbitMQ 报
// PRECONDITION_FAILED → Declare 返回错误 → 启动失败(不悄悄改)。
//
//	交换机(E5)  factory.events    topic   路由键 = 消息类型   每个订阅者一个队列,按类型绑定(0..N 个订阅者)
//	            factory.commands  direct  路由键 = 消息类型   只有拥有这个命令契约的服务绑定(恰好一个处理器)
//	队列        <服务>.<消息类型>          classic、持久化;x-dead-letter-exchange = "" → <队列>.dlq
//	重试(E6)    <队列>.retry.1 / .2 / .3  队列级 TTL 1s / 10s / 60s,无消费者,到期 → 回到 <队列>
//	死信        <队列>.dlq                无 TTL,人来处理(consume 包的 DLQ 管理)
//
// vhost 每个环境一个(dev / staging / prod): 写在连接串里(AMQP_URL=amqp://user:pass@host:5672/prod),
// 服务不感知环境。
package topology

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/broker"
	"github.com/MIERAI/api-factory-runtime/message"
)

// Exchanges: 两个交换机的名字。
type Exchanges struct {
	Events   string // topic
	Commands string // direct
}

// DefaultExchanges: factory.events / factory.commands。
var DefaultExchanges = Exchanges{Events: "factory.events", Commands: "factory.commands"}

// For: 这种消息发到哪个交换机。
func (x Exchanges) For(k message.Kind) (string, error) {
	switch k {
	case message.Event:
		return x.Events, nil
	case message.Command:
		return x.Commands, nil
	}
	return "", fmt.Errorf("topology: unknown message kind %d", k)
}

// RetryPolicy: 重试的分级(E6)。第 n 次处理失败(n < MaxAttempts)→ 副本发到第 min(n, len(Tiers)) 级的重试队列;
// 第 MaxAttempts 次仍失败 → DLQ。默认: 1s / 10s / 60s,最多 5 次(延迟 1s, 10s, 60s, 60s)。
// 生产者(Declare 的队列 TTL)和消费者(consume)必须用同一个策略。
type RetryPolicy struct {
	Tiers       []time.Duration
	MaxAttempts int
}

// DefaultRetry: 设计稿 §4.1 / §4.3 的值。
var DefaultRetry = RetryPolicy{Tiers: []time.Duration{time.Second, 10 * time.Second, 60 * time.Second}, MaxAttempts: 5}

// Tier: 第 attempt 次失败之后用第几级重试队列(从 1 开始);0 = 用尽,进 DLQ。
func (p RetryPolicy) Tier(attempt int) int {
	if attempt >= p.MaxAttempts || len(p.Tiers) == 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	return min(attempt, len(p.Tiers))
}

func (p RetryPolicy) validate() error {
	if p.MaxAttempts < 1 {
		return errors.New("topology: RetryPolicy.MaxAttempts must be >= 1")
	}
	for i, d := range p.Tiers {
		if d < time.Millisecond {
			return fmt.Errorf("topology: retry tier %d: TTL %s is shorter than 1ms", i+1, d)
		}
	}
	return nil
}

// Subscription: 队列接收的一种消息。
type Subscription struct {
	Kind message.Kind
	Type string // 消息类型 = 路由键(事件可以用 topic 的通配符,命令不行)
}

// Queue: 一个处理器的队列。
type Queue struct {
	Name          string // QueueName(服务, 消息类型)
	Subscriptions []Subscription
}

// Spec: 一个服务要声明的全部东西(工厂生成)。
type Spec struct {
	Exchanges Exchanges // 零值 = DefaultExchanges
	Retry     RetryPolicy
	Queues    []Queue
}

// QueueName: <服务>.<消息类型>(例 notification.order.confirmed)。
func QueueName(service, msgType string) string { return service + "." + msgType }

// RetryQueue: 第 tier 级(从 1 开始)的重试队列。
func RetryQueue(queue string, tier int) string { return queue + ".retry." + strconv.Itoa(tier) }

// DLQ: 死信队列。
func DLQ(queue string) string { return queue + ".dlq" }

// Declarer: Declare 用到的 channel 方法(*amqp.Channel)。
type Declarer interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
}

func (s Spec) exchanges() Exchanges {
	if s.Exchanges == (Exchanges{}) {
		return DefaultExchanges
	}
	return s.Exchanges
}

// DeclareExchanges: 只声明两个交换机(只发不收的服务 / 中继启动时)。
func DeclareExchanges(ch Declarer, x Exchanges) error {
	if x == (Exchanges{}) {
		x = DefaultExchanges
	}
	if err := ch.ExchangeDeclare(x.Events, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", x.Events, err)
	}
	if err := ch.ExchangeDeclare(x.Commands, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", x.Commands, err)
	}
	return nil
}

// Declare: 交换机 + 每个队列的主队列 / 重试队列 / DLQ + 绑定。幂等;参数和已有的不一致 → 错误(channel 被服务器关掉,
// 调用方换一个 channel)。
func Declare(ch Declarer, s Spec) error {
	if err := s.Retry.validate(); err != nil {
		return err
	}
	x := s.exchanges()
	if err := DeclareExchanges(ch, x); err != nil {
		return err
	}
	for _, q := range s.Queues {
		if q.Name == "" {
			return errors.New("topology: queue without a name")
		}
		if _, err := ch.QueueDeclare(DLQ(q.Name), true, false, false, false, amqp.Table{"x-queue-type": "classic"}); err != nil {
			return fmt.Errorf("declare queue %s: %w", DLQ(q.Name), err)
		}
		for i, ttl := range s.Retry.Tiers {
			args := amqp.Table{
				"x-queue-type":              "classic",
				"x-message-ttl":             ttl.Milliseconds(),
				"x-dead-letter-exchange":    "",
				"x-dead-letter-routing-key": q.Name,
			}
			if _, err := ch.QueueDeclare(RetryQueue(q.Name, i+1), true, false, false, false, args); err != nil {
				return fmt.Errorf("declare queue %s: %w", RetryQueue(q.Name, i+1), err)
			}
		}
		args := amqp.Table{
			"x-queue-type":              "classic",
			"x-dead-letter-exchange":    "", // 被 reject(requeue=false)的消息 → DLQ(consume 发不出 DLQ 副本时的退路)
			"x-dead-letter-routing-key": DLQ(q.Name),
		}
		if _, err := ch.QueueDeclare(q.Name, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare queue %s: %w", q.Name, err)
		}
		for _, sub := range q.Subscriptions {
			ex, err := x.For(sub.Kind)
			if err != nil {
				return err
			}
			if sub.Type == "" {
				return fmt.Errorf("topology: queue %s: subscription without a message type", q.Name)
			}
			if err := ch.QueueBind(q.Name, sub.Type, ex, false, nil); err != nil {
				return fmt.Errorf("bind %s to %s (%s): %w", q.Name, ex, sub.Type, err)
			}
		}
	}
	return nil
}

// Deleter: Delete 用到的 channel 方法。
type Deleter interface {
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)
}

// Delete: 删除 Spec 的全部队列(不删交换机;测试的清理用)。
func Delete(ch Deleter, s Spec) error {
	var errs []error
	for _, q := range s.Queues {
		names := []string{q.Name, DLQ(q.Name)}
		for i := range s.Retry.Tiers {
			names = append(names, RetryQueue(q.Name, i+1))
		}
		for _, n := range names {
			if _, err := ch.QueueDelete(n, false, false, false); err != nil {
				errs = append(errs, fmt.Errorf("delete %s: %w", n, err))
			}
		}
	}
	return errors.Join(errs...)
}

// Ensure: 服务启动时声明 s(生成的 wiring 的后台任务调用)。RabbitMQ 连不上 → 退避(1s → 30s)重试,直到 ctx 结束
// (返回 nil);连上之后声明的错误(参数和已有的不一致 = PRECONDITION_FAILED 等)→ 返回错误 = 启动失败(不悄悄改)。
// 消费循环在队列声明好之前订阅失败也会自己重试,所以不用等 Ensure。
func Ensure(ctx context.Context, conn *broker.Conn, s Spec) error {
	var backoff time.Duration
	for {
		ch, err := conn.Channel(ctx)
		if err == nil {
			derr := Declare(ch, s)
			_ = ch.Close()
			var ae *amqp.Error
			if derr == nil || !errors.As(derr, &ae) || ae.Code != amqp.ChannelError {
				return derr // 成功,或者服务器拒绝了声明(参数不一致等)
			}
			err = derr // 声明途中连接断了: 重试
		}
		if ctx.Err() != nil {
			return nil
		}
		backoff = min(max(backoff*2, time.Second), 30*time.Second)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
	}
}
