package message

// data 的编解码(生成的代码用): 契约的 data = 一个 proto 消息(工厂从契约生成),表示和查询工厂的 API 一致
// (protojson、proto 的字段名: 十进制 → 字符串,64 位整数 → 字符串,时间 → UTC RFC 3339)。

import (
	"encoding/json"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// DecodeData: env.Data → m,并按 m 的 protovalidate 规则(契约的 必填 / 范围 / 个数 / 唯一 …)检查。
// 不认识的字段忽略(契约 §2.4: 消费者忽略不认识的字段)。解不开 / 规则不满足 → *DecodeError(毒消息: 重试也不会好)。
func DecodeData(env Envelope, m proto.Message) error {
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(env.Data, m); err != nil {
		return &DecodeError{Msg: env.Type + " v" + itoa(env.Version) + ": data: " + err.Error()}
	}
	if err := protovalidate.Validate(m); err != nil {
		return &DecodeError{Msg: env.Type + " v" + itoa(env.Version) + ": data: " + err.Error()}
	}
	return nil
}

// EncodeData: m → data(JSON 对象;proto 的字段名;零值也写出: 消费者看得到 0 / 空字符串 / false)。
func EncodeData(m proto.Message) (json.RawMessage, error) {
	b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(m)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
