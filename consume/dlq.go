package consume

// DLQ 的管理(R4 设计稿 §4.3): 看(List)、放回主队列(Requeue)、丢弃(Drop)。cmd/factory-runtime 的
// `dlq list|requeue|drop` 子命令用这些函数。Requeue 保留原 id: 已经处理过的会被去重跳过。

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/broker"
	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/topology"
)

// DeadLetter: DLQ 里的一条消息。
type DeadLetter struct {
	ID      string // message_id(= envelope.id)
	Type    string
	Outcome string // poison / exhausted(reject 退路进来的没有)
	Error   string // 最后的错误
	Attempt int
	Body    []byte
	Headers map[string]any
}

// QueueDepth: 队列里的消息数(被动声明;队列不存在 → 错误)。
func QueueDepth(ctx context.Context, b *broker.Conn, queue string) (int, error) {
	ch, err := b.Channel(ctx)
	if err != nil {
		return 0, err
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}

// DLQList: 看 <queue>.dlq 的前 limit 条(不移除: 取出后不 ack,关 channel 时放回)。
func DLQList(ctx context.Context, b *broker.Conn, queue string, limit int) ([]DeadLetter, error) {
	var out []DeadLetter
	err := dlqEach(ctx, b, queue, limit, func(_ *amqp.Channel, d amqp.Delivery) (bool, error) {
		out = append(out, deadLetter(d))
		return false, nil
	})
	return out, err
}

// DLQRequeue: <queue>.dlq 的前 limit 条放回主队列(同一个 id、同样的 body;重试次数从头开始),返回条数。
func DLQRequeue(ctx context.Context, b *broker.Conn, queue string, limit int) (int, error) {
	pub, err := b.ConfirmChannel(ctx)
	if err != nil {
		return 0, err
	}
	defer pub.Close()
	n := 0
	err = dlqEach(ctx, b, queue, limit, func(_ *amqp.Channel, d amqp.Delivery) (bool, error) {
		h := copyHeaders(d.Headers)
		delete(h, message.HeaderAttempt)
		delete(h, message.HeaderError)
		delete(h, message.HeaderOutcome)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		conf, err := pub.PublishWithDeferredConfirmWithContext(cctx, "", queue, false, false, amqp.Publishing{
			ContentType: d.ContentType, DeliveryMode: amqp.Persistent, MessageId: d.MessageId, Type: d.Type,
			Timestamp: d.Timestamp, AppId: d.AppId, Headers: h, Body: d.Body,
		})
		if err != nil {
			return false, err
		}
		if ok, err := conf.WaitContext(cctx); err != nil || !ok {
			return false, errors.Join(errors.New("requeue not confirmed"), err)
		}
		n++
		return true, nil
	})
	return n, err
}

// DLQDrop: 丢弃 <queue>.dlq 的前 limit 条,返回条数。
func DLQDrop(ctx context.Context, b *broker.Conn, queue string, limit int) (int, error) {
	n := 0
	err := dlqEach(ctx, b, queue, limit, func(_ *amqp.Channel, _ amqp.Delivery) (bool, error) {
		n++
		return true, nil
	})
	return n, err
}

// dlqEach: 从 DLQ 逐条取(basic.get),fn 返回 true = ack(移除);false = 留着(关 channel 时放回)。
func dlqEach(ctx context.Context, b *broker.Conn, queue string, limit int, fn func(*amqp.Channel, amqp.Delivery) (bool, error)) error {
	if limit <= 0 {
		return fmt.Errorf("dlq: limit must be >= 1")
	}
	ch, err := b.Channel(ctx)
	if err != nil {
		return err
	}
	defer ch.Close() // 没 ack 的放回 DLQ
	for i := 0; i < limit; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		d, ok, err := ch.Get(topology.DLQ(queue), false)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		ack, err := fn(ch, d)
		if err != nil {
			return err
		}
		if ack {
			if err := d.Ack(false); err != nil {
				return err
			}
		}
	}
	return nil
}

func deadLetter(d amqp.Delivery) DeadLetter {
	attempt, _ := message.HeaderInt(d.Headers, message.HeaderAttempt)
	return DeadLetter{
		ID: d.MessageId, Type: d.Type, Body: d.Body, Headers: d.Headers, Attempt: attempt,
		Outcome: message.HeaderString(d.Headers, message.HeaderOutcome),
		Error:   message.HeaderString(d.Headers, message.HeaderError),
	}
}
