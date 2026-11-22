// Package broker 是到 RabbitMQ 的连接: 一个进程一个 TCP 连接,需要时(第一次 / 断开之后)才连,
// 中继和每个消费循环各自从这里开 channel。断线重连的策略在使用者(中继 / 消费循环)里: 它们的 channel 关了就
// 退避之后再要一个,这里只负责"连接断了就重新连"。
//
// 配置: AMQP_URL(例 amqp://app:secret@rabbitmq:5672/prod)。vhost 每个环境一个,写在连接串里(R4 §4.1)。
package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Conn: 可以多个 goroutine 共用。
type Conn struct {
	url  string
	name string

	mu     sync.Mutex
	conn   *amqp.Connection
	closed bool
}

// New: 不连接(第一次 Channel 时才连)。name 显示在 RabbitMQ 管理界面的连接属性里(例: 服务名)。
func New(url, name string) *Conn { return &Conn{url: url, name: name} }

// ErrClosed: Close 之后。
var ErrClosed = errors.New("broker: closed")

// Channel: 开一个新的 channel(连接断了就先重新连)。用完由调用方 Close。
func (c *Conn) Channel(ctx context.Context) (*amqp.Channel, error) {
	conn, err := c.connection(ctx)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		// 连接还在但开不了 channel(例: channel 数到上限)或者刚好断开: 下次重新连
		c.mu.Lock()
		if c.conn == conn && conn.IsClosed() {
			c.conn = nil
		}
		c.mu.Unlock()
		return nil, fmt.Errorf("broker: open channel: %w", err)
	}
	return ch, nil
}

// ConfirmChannel: 开了 publisher confirm 的 channel(发送方用: 中继、重试 / DLQ 的副本)。
func (c *Conn) ConfirmChannel(ctx context.Context) (*amqp.Channel, error) {
	ch, err := c.Channel(ctx)
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("broker: confirm mode: %w", err)
	}
	return ch, nil
}

func (c *Conn) connection(ctx context.Context) (*amqp.Connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn, nil
	}
	if c.url == "" {
		return nil, errors.New("broker: AMQP_URL is not set")
	}
	timeout := 10 * time.Second
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < timeout {
		timeout = max(time.Until(dl), time.Millisecond)
	}
	props := amqp.NewConnectionProperties()
	if c.name != "" {
		props.SetClientConnectionName(c.name)
	}
	conn, err := amqp.DialConfig(c.url, amqp.Config{
		Heartbeat:  10 * time.Second,
		Properties: props,
		Dial:       amqp.DefaultDial(timeout),
	})
	if err != nil {
		return nil, fmt.Errorf("broker: dial: %w", err)
	}
	c.conn = conn
	return conn, nil
}

// Close: 关掉连接;之后的 Channel 返回 ErrClosed。
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		if errors.Is(err, amqp.ErrClosed) {
			return nil
		}
		return err
	}
	return nil
}

// Drop: 关掉当前的 TCP 连接(不像 Close 那样不再连接): 下一次 Channel 重新连。测试里模拟断线用。
func (c *Conn) Drop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}
