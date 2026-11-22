package consume

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/metrics"
	"github.com/MIERAI/api-factory-runtime/topology"
)

// Run: 消费 Queue,直到 ctx 结束(返回 nil)。连接 / channel 断了 → 退避(1s → 30s)后重新订阅。
//
// 停机(ctx 结束): 取消订阅,不再开始新的消息;正在处理的消息做完(事务上限 Timeout)并 ack / 发重试副本;
// 已经预取但没开始的消息不 ack,channel 关闭后由 RabbitMQ 重新投递给别的副本。所以 server 的停机等待要 ≥ Timeout。
func (c *Consumer) Run(ctx context.Context) error {
	c.defaults()
	switch {
	case c.Queue == "":
		return errors.New("consume: Queue is required")
	case c.Handler == nil:
		return fmt.Errorf("consume %s: Handler is required", c.Queue)
	case c.Exec == nil:
		return fmt.Errorf("consume %s: Exec is required", c.Queue)
	case c.Broker == nil:
		return fmt.Errorf("consume %s: Broker is required", c.Queue)
	}
	if c.WaitFor != nil { // 先等拓扑(冷启动时不再每个队列一条 NOT_FOUND + 退避)
		select {
		case <-c.WaitFor:
		case <-ctx.Done():
			return nil
		case <-time.After(c.WaitLimit):
			c.Log.Warn("consumer: topology not ready yet; subscribing anyway (retries while the queue does not exist)", "queue", c.Queue, "waited", c.WaitLimit)
		}
	}
	if c.DLQCheckEvery > 0 {
		go c.watchDLQ(ctx)
	}
	var backoff time.Duration
	for {
		started := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(started) > time.Minute {
			backoff = 0 // 正常工作了一阵之后断开: 从头退避
		}
		backoff = min(max(backoff*2, time.Second), 30*time.Second)
		c.Log.Warn("consumer: session ended", "queue", c.Queue, "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
	}
}

func (c *Consumer) session(ctx context.Context) error {
	ch, err := c.Broker.Channel(ctx)
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.Qos(c.Prefetch, 0, false); err != nil {
		return err
	}
	pch, err := c.Broker.ConfirmChannel(ctx) // 重试 / DLQ 的副本(和消费的 channel 分开)
	if err != nil {
		return err
	}
	defer pch.Close()
	pub := &publisher{ch: pch, returns: pch.NotifyReturn(make(chan amqp.Return, 4))}
	tag := "factory-" + c.Name + "-" + message.NewID()[24:]
	deliveries, err := ch.Consume(c.Queue, tag, false, false, false, false, nil)
	if err != nil {
		return err
	}
	c.Log.Info("consumer: subscribed", "queue", c.Queue, "prefetch", c.Prefetch)
	if c.Subscribed != nil {
		c.Subscribed()
	}

	// 正在处理的消息不跟着 ctx 取消: 停机时做完(上限 Timeout)
	pctx := context.WithoutCancel(ctx)
	work := make(chan amqp.Delivery)
	var wg sync.WaitGroup
	for i := 0; i < c.Prefetch; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range work {
				c.deliver(pctx, pub, d)
			}
		}()
	}
	defer func() {
		close(work)
		wg.Wait() // 做完正在处理的,再关 channel(defer 的顺序: 先这里,再 pub / ch 的 Close)
	}()
	for {
		select {
		case <-ctx.Done():
			_ = ch.Cancel(tag, false)
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed (connection or channel lost)")
			}
			select {
			case work <- d:
			case <-ctx.Done(): // 停机: 这条不开始,不 ack(关 channel 后重新投递)
				_ = ch.Cancel(tag, false)
				return nil
			}
		}
	}
}

// deliver: 处理一条,并按结果 ack / 发副本。
func (c *Consumer) deliver(ctx context.Context, pub *publisher, d amqp.Delivery) {
	in := Delivery{Body: d.Body, MessageID: d.MessageId, Headers: d.Headers, Redelivered: d.Redelivered}
	res := c.Process(ctx, in)
	crash := c.crashPoint(in)
	switch res.Outcome {
	case OK, Duplicate:
		if crash == CrashAfterCommit {
			exit()
		}
		c.ack(d)
	case Retry:
		h := copyHeaders(d.Headers)
		h[message.HeaderAttempt] = int32(res.Attempt + 1)
		h[message.HeaderError] = truncate(errText(res.Err), 1000)
		q := topology.RetryQueue(c.Queue, res.Tier)
		if err := pub.copy(ctx, q, d, h); err != nil {
			// 副本发不出(RabbitMQ 有问题): 放回队列,稍等一下免得热循环。attempt 不变,之后照常重试
			c.Log.Warn("consumer: cannot publish retry copy; requeueing", "queue", c.Queue, "id", res.Envelope.ID, "err", err)
			time.Sleep(time.Second)
			_ = d.Nack(false, true)
			return
		}
		if crash == CrashAfterRetryPublish {
			exit()
		}
		c.Log.Warn("consumer: transient failure, retrying", "queue", c.Queue, "id", res.Envelope.ID, "attempt", res.Attempt, "tier", res.Tier, "err", res.Err)
		c.ack(d)
	case Exhausted, Poisoned:
		h := copyHeaders(d.Headers)
		outcome := "exhausted"
		if res.Outcome == Poisoned {
			outcome = "poison"
		}
		h[message.HeaderOutcome] = outcome
		h[message.HeaderError] = truncate(errText(res.Err), 1000)
		h[message.HeaderAttempt] = int32(res.Attempt)
		c.Log.Error("consumer: message dead-lettered", "queue", c.Queue, "id", res.Envelope.ID, "outcome", outcome, "attempt", res.Attempt, "err", res.Err)
		if err := pub.copy(ctx, topology.DLQ(c.Queue), d, h); err != nil {
			// 发不出带错误的副本: reject → 队列的 x-dead-letter-exchange 把原消息送进 DLQ(没有错误的 header)
			_ = d.Reject(false)
			return
		}
		c.ack(d)
	}
}

func (c *Consumer) ack(d amqp.Delivery) {
	if err := d.Ack(false); err != nil {
		// channel 断了: 消息会被重新投递,去重表保证只生效一次
		c.Log.Warn("consumer: ack failed (the message will be redelivered and deduplicated)", "queue", c.Queue, "err", err)
	}
}

// publisher: 发副本的 channel(confirm 模式)。副本只在失败时才发,一次一条(mu)就够了,
// 这样 mandatory 的退回能对应到这一次发送。
type publisher struct {
	mu      sync.Mutex
	ch      *amqp.Channel
	returns chan amqp.Return
}

// copy: 原消息的副本发到 queue(默认交换机,路由键 = 队列名,mandatory: 队列不存在 = 失败),等 publisher confirm。
func (p *publisher) copy(ctx context.Context, queue string, d amqp.Delivery, h amqp.Table) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(cctx, "", queue, true, false, amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Type:         d.Type,
		Timestamp:    d.Timestamp,
		AppId:        d.AppId,
		Headers:      h,
		Body:         d.Body,
	})
	if err != nil {
		return err
	}
	acked, err := conf.WaitContext(cctx)
	if err != nil {
		return err
	}
	if !acked {
		return errors.New("nack from broker")
	}
	select { // basic.return 在 basic.ack 之前送达
	case ret := <-p.returns:
		return fmt.Errorf("queue %s does not exist (%d %s)", ret.RoutingKey, ret.ReplyCode, ret.ReplyText)
	default:
		return nil
	}
}

// copyHeaders: 副本的 header: 去掉测试的崩溃点(副本不再崩溃)和 RabbitMQ 的死信记录(重试的历史看 x-factory-attempt)。
func copyHeaders(h amqp.Table) amqp.Table {
	out := amqp.Table{}
	for k, v := range h {
		if k == message.HeaderCrash || k == "x-death" || strings.HasPrefix(k, "x-first-death-") || strings.HasPrefix(k, "x-last-death-") {
			continue
		}
		out[k] = v
	}
	return out
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// watchDLQ: 定期查 DLQ 的长度(factory_dlq_messages)。
func (c *Consumer) watchDLQ(ctx context.Context) {
	t := time.NewTicker(c.DLQCheckEvery)
	defer t.Stop()
	for {
		if n, err := QueueDepth(ctx, c.Broker, topology.DLQ(c.Queue)); err == nil {
			metrics.DLQSize(topology.DLQ(c.Queue), int64(n))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
