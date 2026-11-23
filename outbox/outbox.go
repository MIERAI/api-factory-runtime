// Package outbox: 写操作发出的消息先在同一个事务里写进 outbox 表,再由中继(Relay)发到 RabbitMQ(R4 设计稿 §3)。
//
//	写入   Writer.Write(ctx, tx, msgs…): 在调用方的事务(txn.Executor.InTx 的 tx)里 INSERT factory_outbox;
//	       提交之后在事务外 NOTIFY 唤醒中继(合并发送,见 notify.go;NOTIFY 不在写事务里)。
//	       事务回滚 → 没有消息;提交 → 消息一定会被发出(至少一次,id 不变)。
//	中继   Relay.Run(ctx): 每个数据库同时只有一个在工作(advisory lock,E3),按 seq 批量发布、等 publisher confirm、
//	       确认了才标记 published_at。崩在发布之后、标记之前 → 重启后同一条消息(同一个 id、逐字节相同)再发一次,
//	       下游靠 id 去重(consume 包)。
//	平台表 PlatformSQL(zz_factory_platform.sql): 工厂把它原样复制到项目的 db/schema/(E1)。
package outbox

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/MIERAI/api-factory-runtime/clock"
	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/txn"
)

// PlatformSQL: 平台表(factory_outbox、factory_processed_messages)的 DDL。工厂原样写到项目的
// db/schema/zz_factory_platform.sql;内容随运行时的版本变化(升级运行时 = 重新生成这个文件)。
//
//go:embed zz_factory_platform.sql
var PlatformSQL string

// PlatformFile: 工厂写出 PlatformSQL 时用的文件名(排在项目的 schema 文件之后)。
const PlatformFile = "zz_factory_platform.sql"

// NotifyChannel: 写入提交后唤醒中继的 NOTIFY 频道(只是唤醒,丢了也没关系: 中继另有轮询)。
// 提交之后在事务外发(v0.5.2 起;以前在写事务里发,会让所有写 outbox 的提交在实例上串行)。
const NotifyChannel = "factory_outbox"

// Message: 要发出的一条消息(生成的代码按设计稿的 `发出:` 组装)。
type Message struct {
	Kind    message.Kind
	Type    string // 消息类型(= 路由键)
	Version int    // 契约版本(≥ 1)
	Subject string // 契约声明的主体字段的值
	// Data: 字段 → 值。json.RawMessage / []byte 原样使用(必须是 JSON 对象);其他值用 encoding/json 序列化
	Data any
}

// Writer: 每个服务一个(Source = 服务名,写进 envelope.source)。
type Writer struct {
	Source string
}

// NewWriter: source = 服务名(项目 factory.yaml 的 proto 包名前缀)。
func NewWriter(source string) *Writer { return &Writer{Source: source} }

// Write: 在 tx 里把 msgs 写进 outbox(按参数的顺序),返回写下的 envelope(id 是新生成的 UUIDv7)。
//
//   - occurred_at = clock.Now(ctx)(请求开始的时刻;测试时可以固定)
//   - traceparent = message.Traceparent(ctx)(HTTP 请求头 / 正在处理的消息)
//   - 每一行计入测试的故障注入(txn.AfterWrite): "outbox 写入之后、提交之前"失败 → 没有消息
//   - 提交成功之后(txn.AfterCommit)在事务外 NOTIFY factory_outbox 唤醒中继,按连接池合并;回滚就没有。
//     写事务本身不发 NOTIFY(见 notify.go: 那会让所有写 outbox 的提交在实例上串行)
//
// 必须在调用方的事务里调用(txn.Executor.InTx 的 tx);事务的提交 / 回滚由调用方决定。不在 InTx 里调用时
// 没有提交后的唤醒: 消息照样会发出,由中继的轮询(Poll)取走。
func (w *Writer) Write(ctx context.Context, tx pgx.Tx, msgs ...Message) ([]message.Envelope, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	if w.Source == "" {
		return nil, errors.New("outbox: Writer.Source (the service name) is empty")
	}
	now := clock.Now(ctx)
	tp := message.Traceparent(ctx)
	envs := make([]message.Envelope, 0, len(msgs))
	for i, m := range msgs {
		if m.Kind != message.Event && m.Kind != message.Command {
			return nil, fmt.Errorf("outbox: message %d (%s): unknown kind %d", i, m.Type, m.Kind)
		}
		if m.Type == "" || m.Version < 1 {
			return nil, fmt.Errorf("outbox: message %d: type and version (>= 1) are required", i)
		}
		data, err := encodeData(m.Data)
		if err != nil {
			return nil, fmt.Errorf("outbox: message %d (%s): %w", i, m.Type, err)
		}
		env := message.Envelope{
			ID: message.NewID(), Type: m.Type, Version: m.Version, OccurredAt: now,
			Source: w.Source, Subject: m.Subject, Traceparent: tp, Data: data,
		}
		body, err := env.Encode()
		if err != nil {
			return nil, fmt.Errorf("outbox: message %d (%s): %w", i, m.Type, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO factory_outbox (id, kind, type, version, subject, envelope, occurred_at)
VALUES ($1::uuid, $2::smallint, $3::text, $4::integer, $5::text, $6::jsonb, $7::timestamptz)`,
			env.ID, int16(m.Kind), m.Type, int32(m.Version), m.Subject, string(body), now); err != nil {
			return nil, fmt.Errorf("outbox: insert %s: %w", m.Type, err)
		}
		if err := txn.AfterWrite(ctx, "factory_outbox"); err != nil {
			return nil, err
		}
		envs = append(envs, env)
	}
	txn.AfterCommit(ctx, wakeupKey{}, kick)
	return envs, nil
}

type wakeupKey struct{} // 一个事务里不管写了几次 outbox,提交后只唤醒一次

func encodeData(v any) (json.RawMessage, error) {
	var b []byte
	switch d := v.(type) {
	case nil:
		return json.RawMessage("{}"), nil
	case json.RawMessage:
		b = d
	case []byte:
		b = d
	default:
		var err error
		if b, err = json.Marshal(v); err != nil {
			return nil, err
		}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(b, &obj); err != nil || obj == nil {
		return nil, errors.New("data must be a JSON object")
	}
	return json.RawMessage(b), nil
}
