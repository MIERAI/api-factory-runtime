// Package message 是消息(事件 / 命令)的外层(envelope)和它在 RabbitMQ 上的表示,outbox(发送)和 consume(接收)共用。
//
// R4 设计稿 §2.3: 所有消息共用一个 envelope:
//
//	{"id": UUIDv7, "type": "task.created", "version": 1, "occurred_at": RFC 3339 (UTC),
//	 "source": 服务名, "subject": 主体字段的值, "traceparent": W3C, "data": {字段: 值}}
//
// data 的内容由生成的代码决定(数值的表示和查询工厂的 API 一致: 十进制 → 字符串,时间 → UTC RFC 3339,
// 日期 → YYYY-MM-DD);这个包只把它当作一个 JSON 对象。
package message

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Kind: 消息的种类。事件 = 广播"发生了什么"(0..N 个订阅者);命令 = 让某个服务做某件事(恰好一个处理器)。
type Kind int16

const (
	Event   Kind = 1
	Command Kind = 2
)

func (k Kind) String() string {
	switch k {
	case Event:
		return "event"
	case Command:
		return "command"
	}
	return "kind(" + strconv.Itoa(int(k)) + ")"
}

// Envelope: 消息的外层。Data 是 JSON 对象(原样保留,不认识的字段不丢)。
type Envelope struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Version     int             `json:"version"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Source      string          `json:"source"`
	Subject     string          `json:"subject"`
	Traceparent string          `json:"traceparent,omitempty"`
	Data        json.RawMessage `json:"data"`
}

// Encode: envelope → JSON(occurred_at 是 UTC、微秒精度,和 Postgres 的 timestamptz 一致)。
func (e Envelope) Encode() ([]byte, error) {
	e.OccurredAt = e.OccurredAt.UTC().Truncate(time.Microsecond)
	if len(e.Data) == 0 {
		e.Data = json.RawMessage("{}")
	}
	return json.Marshal(e)
}

// DecodeError: 外层解不开(毒消息: 重试也不会好)。
type DecodeError struct{ Msg string }

func (e *DecodeError) Error() string { return "message: " + e.Msg }

// Decode: JSON → envelope,并检查外层: id 是 UUID、type / source 非空、version ≥ 1、occurred_at 有值、data 是对象。
// 不认识的字段忽略(契约 §2.4: 消费者忽略不认识的字段)。失败 → *DecodeError。
func Decode(body []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(body, &e); err != nil {
		return e, &DecodeError{Msg: "not a JSON envelope: " + err.Error()}
	}
	switch {
	case !ValidID(e.ID):
		return e, &DecodeError{Msg: "id is not a UUID: " + strconv.Quote(e.ID)}
	case e.Type == "":
		return e, &DecodeError{Msg: "type is missing"}
	case e.Version < 1:
		return e, &DecodeError{Msg: "version must be >= 1"}
	case e.OccurredAt.IsZero():
		return e, &DecodeError{Msg: "occurred_at is missing"}
	case e.Source == "":
		return e, &DecodeError{Msg: "source is missing"}
	}
	d := bytes.TrimSpace(e.Data)
	if len(d) == 0 || d[0] != '{' {
		return e, &DecodeError{Msg: "data is not a JSON object"}
	}
	return e, nil
}

// IsDecodeError: err 是不是外层解不开。
func IsDecodeError(err error) bool {
	var d *DecodeError
	return errors.As(err, &d)
}

// NewID: UUIDv7(RFC 9562: 前 48 位是毫秒时间戳,其余随机)的文本。按时间大致有序,便于排查;不用来排序消息(E4)。
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[6:]); err != nil {
		panic("message: crypto/rand: " + err.Error())
	}
	ms := uint64(time.Now().UnixMilli())
	for i := 0; i < 6; i++ {
		b[i] = byte(ms >> (40 - 8*i))
	}
	b[6] = 0x70 | (b[6] & 0x0f) // version 7
	b[8] = 0x80 | (b[8] & 0x3f) // variant 10
	return formatUUID(b)
}

func formatUUID(b [16]byte) string {
	var s [36]byte
	hex.Encode(s[0:8], b[0:4])
	s[8] = '-'
	hex.Encode(s[9:13], b[4:6])
	s[13] = '-'
	hex.Encode(s[14:18], b[6:8])
	s[18] = '-'
	hex.Encode(s[19:23], b[8:10])
	s[23] = '-'
	hex.Encode(s[24:], b[10:])
	return string(s[:])
}

// ValidID: s 是 UUID 的文本(8-4-4-4-12 位十六进制;不限版本: 别的系统发来的 UUID 也收)。
func ValidID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
				return false
			}
		}
	}
	return true
}

// IDTime: UUIDv7 里的时间戳(不是 v7 → false)。
func IDTime(id string) (time.Time, bool) {
	if !ValidID(id) || id[14] != '7' {
		return time.Time{}, false
	}
	h, err := hex.DecodeString(id[0:8] + id[9:13])
	if err != nil {
		return time.Time{}, false
	}
	var ms int64
	for _, c := range h {
		ms = ms<<8 | int64(c)
	}
	return time.UnixMilli(ms).UTC(), true
}

// AMQP 上的表示(outbox 的中继发送、consume 的接收、DLQ 管理共用)。
const (
	ContentType = "application/json"
	// HeaderTraceparent: W3C traceparent(envelope 里也有;放进 header 让不解 body 的工具也能看到)
	HeaderTraceparent = "traceparent"
	// HeaderVersion: 契约版本
	HeaderVersion = "x-factory-version"
	// HeaderAttempt: 这是第几次处理(重试的副本带上;没有 = 1)
	HeaderAttempt = "x-factory-attempt"
	// HeaderError / HeaderOutcome: 进 DLQ 的消息带上最后的错误和原因(poison / exhausted)
	HeaderError   = "x-factory-error"
	HeaderOutcome = "x-factory-outcome"
	// HeaderCrash: 测试用的崩溃点(只在 FACTORY_TEST_FAULTS=1 时生效,见 consume)
	HeaderCrash = "x-factory-crash"
	// HeaderFailAfter / HeaderNow: 测试用(只在 FACTORY_TEST_FAULTS=1 / FACTORY_TEST_CLOCK=1 时生效),
	// 和 HTTP 的 X-Factory-Fail-After / X-Factory-Now 同义
	HeaderFailAfter = "x-factory-fail-after"
	// HeaderFailAttempts: 测试用(FACTORY_TEST_FAULTS=1): 前 n 次处理在第一个写操作之后失败(暂时性错误 → 重试队列),
	// 第 n+1 次正常处理。重试的副本保留 header,所以 x-factory-fail-after 会让每一次都失败(→ 用尽 → DLQ);
	// 这个 header 用来验证"重试之后成功"的路径
	HeaderFailAttempts = "x-factory-fail-attempts"
	HeaderNow          = "x-factory-now"
)

// HeaderInt: AMQP header 里的整数(各种整数类型、数字字符串都收)。没有 / 不是整数 → ok = false。
func HeaderInt(h map[string]any, key string) (int, bool) {
	switch v := h[key].(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		return int(v), true
	case string:
		n, err := strconv.Atoi(v)
		return n, err == nil
	}
	return 0, false
}

// HeaderString: AMQP header 里的文本(string / []byte)。
func HeaderString(h map[string]any, key string) string {
	switch v := h[key].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}
