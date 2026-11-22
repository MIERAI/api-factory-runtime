package topology

// 系统级的拓扑清单(G23): 一个系统(共用一个契约目录的全部服务)的全部交换机、队列、重试队列、DLQ 和绑定。
//
// 工厂从系统的全部项目的设计生成它(<契约目录>/topology.json,和跨项目的契约检查同一次读),平台在部署服务之前
// 声明它(factory-runtime topology apply --file topology.json;k8s 的 Job / compose 的一次性容器)。这样订阅方
// 第一次启动之前发出的事件也有队列接收(否则 factory.events 上没有绑定,事件被退回 = 丢弃)。
//
// 每个服务启动时照样用自己的 Spec 声明自己的队列(Ensure;幂等),所以清单是"提前声明",不是唯一的声明者。
// 参数和已有的不一致(例: 重试的 TTL 改了)→ PRECONDITION_FAILED → apply 失败(不悄悄改)。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/broker"
	"github.com/MIERAI/api-factory-runtime/message"
)

// ManifestVersion: 清单格式的版本(格式不兼容地改变时 + 1)。
const ManifestVersion = 1

// Manifest: topology.json。Queues 是声明的对象;Messages 是说明(谁发、谁收;apply 不用)。
type Manifest struct {
	Version   int               `json:"version"`
	Comment   string            `json:"comment,omitempty"`
	Exchanges Exchanges         `json:"exchanges"`
	Retry     ManifestRetry     `json:"retry"`
	Queues    []ManifestQueue   `json:"queues"`
	Messages  []ManifestMessage `json:"messages,omitempty"`
}

// ManifestRetry: RetryPolicy 的 JSON(毫秒)。
type ManifestRetry struct {
	TiersMs     []int64 `json:"tiers_ms"`
	MaxAttempts int     `json:"max_attempts"`
}

// ManifestQueue: 一个处理器的队列。Retry / DLQ 是由 Name 推出来的名字(写出来给人看;Spec 检查它们一致)。
type ManifestQueue struct {
	Name     string            `json:"name"`
	Service  string            `json:"service"` // 消费它的服务
	Bindings []ManifestBinding `json:"bindings"`
	Retry    []string          `json:"retry_queues"`
	DLQ      string            `json:"dlq"`
}

// ManifestBinding: 队列绑定的一种消息。Exchange 由 Kind 决定(写出来给人看)。
type ManifestBinding struct {
	Kind     string `json:"kind"` // event / command
	Type     string `json:"type"`
	Exchange string `json:"exchange"`
}

// ManifestMessage: 一种消息在系统里的发送方和接收方(工厂的跨项目检查的结果)。External = 系统外也有发送方 / 接收方。
type ManifestMessage struct {
	Type      string   `json:"type"`
	Kind      string   `json:"kind"`
	Version   int      `json:"version"`
	Owner     string   `json:"owner"`
	Producers []string `json:"producers"`
	Consumers []string `json:"consumers"`
	External  bool     `json:"external,omitempty"`
}

func kindName(k message.Kind) (string, error) {
	switch k {
	case message.Event:
		return "event", nil
	case message.Command:
		return "command", nil
	}
	return "", fmt.Errorf("topology: unknown message kind %d", k)
}

func kindOf(s string) (message.Kind, error) {
	switch s {
	case "event":
		return message.Event, nil
	case "command":
		return message.Command, nil
	}
	return 0, fmt.Errorf("topology: kind %q (event / command)", s)
}

// QueueOf: 一个队列的清单项(Spec 的 Queue + 消费它的服务)。
func QueueOf(service string, q Queue, s Spec) (ManifestQueue, error) {
	x := s.exchanges()
	mq := ManifestQueue{Name: q.Name, Service: service, Bindings: []ManifestBinding{}, Retry: []string{}, DLQ: DLQ(q.Name)}
	for _, sub := range q.Subscriptions {
		k, err := kindName(sub.Kind)
		if err != nil {
			return mq, err
		}
		ex, _ := x.For(sub.Kind)
		mq.Bindings = append(mq.Bindings, ManifestBinding{Kind: k, Type: sub.Type, Exchange: ex})
	}
	for i := range s.Retry.Tiers {
		mq.Retry = append(mq.Retry, RetryQueue(q.Name, i+1))
	}
	return mq, nil
}

// RetryOf: RetryPolicy → 清单的写法。
func RetryOf(p RetryPolicy) ManifestRetry {
	r := ManifestRetry{TiersMs: []int64{}, MaxAttempts: p.MaxAttempts}
	for _, d := range p.Tiers {
		r.TiersMs = append(r.TiersMs, d.Milliseconds())
	}
	return r
}

// Spec: 清单 → 要声明的 Spec。检查格式版本、kind、推出来的名字(手改了 retry_queues / dlq / exchange 就报错)。
func (m Manifest) Spec() (Spec, error) {
	if m.Version != ManifestVersion {
		return Spec{}, fmt.Errorf("topology manifest: version %d (this runtime reads %d)", m.Version, ManifestVersion)
	}
	s := Spec{Exchanges: m.Exchanges, Retry: RetryPolicy{MaxAttempts: m.Retry.MaxAttempts}}
	for _, ms := range m.Retry.TiersMs {
		s.Retry.Tiers = append(s.Retry.Tiers, time.Duration(ms)*time.Millisecond)
	}
	if err := s.Retry.validate(); err != nil {
		return Spec{}, err
	}
	seen := map[string]bool{}
	for _, mq := range m.Queues {
		if mq.Name == "" || seen[mq.Name] {
			return Spec{}, fmt.Errorf("topology manifest: queue name %q empty or repeated", mq.Name)
		}
		seen[mq.Name] = true
		q := Queue{Name: mq.Name}
		for _, b := range mq.Bindings {
			k, err := kindOf(b.Kind)
			if err != nil {
				return Spec{}, fmt.Errorf("topology manifest: queue %s: %w", mq.Name, err)
			}
			q.Subscriptions = append(q.Subscriptions, Subscription{Kind: k, Type: b.Type})
		}
		want, err := QueueOf(mq.Service, q, s)
		if err != nil {
			return Spec{}, err
		}
		if fmt.Sprint(want) != fmt.Sprint(mq) {
			return Spec{}, fmt.Errorf("topology manifest: queue %s: the derived names do not match (retry %v dlq %s; exchanges %v): regenerate the manifest, do not edit it",
				mq.Name, want.Retry, want.DLQ, want.Bindings)
		}
		s.Queues = append(s.Queues, q)
	}
	return s, nil
}

// ParseManifest: topology.json → Manifest(未知的字段 = 错误: 格式和运行时的版本对不上)。
func ParseManifest(b []byte) (Manifest, error) {
	var m Manifest
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return m, fmt.Errorf("topology manifest: %w", err)
	}
	return m, nil
}

// Apply: 声明清单(平台在部署服务之前)。RabbitMQ 连不上 → 退避(1s → 10s)重试到 ctx 结束(返回最后的错误);
// 连上之后声明被拒绝(参数和已有的不一致)→ 立刻返回错误。
func Apply(ctx context.Context, conn *broker.Conn, m Manifest) error {
	s, err := m.Spec()
	if err != nil {
		return err
	}
	var backoff time.Duration
	for {
		ch, err := conn.Channel(ctx)
		if err == nil {
			derr := Declare(ch, s)
			_ = ch.Close()
			var ae *amqp.Error
			if derr == nil || !errors.As(derr, &ae) || ae.Code != amqp.ChannelError {
				return derr
			}
			err = derr
		}
		backoff = min(max(backoff*2, time.Second), 10*time.Second)
		select {
		case <-ctx.Done():
			return fmt.Errorf("topology manifest: broker not reachable: %w", err)
		case <-time.After(backoff):
		}
	}
}
