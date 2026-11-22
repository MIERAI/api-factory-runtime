// Package paging 是 list_keyset 模式的运行时部分: page_size 处理、多取一行判断下一页、
// 以及带签名且与筛选条件绑定的 page_token。
//
// token = base64url( json{fp, cursor} + HMAC-SHA256 )
//   - 签名: 调用方无法伪造或篡改游标
//   - fp(筛选条件指纹): 翻页途中改变筛选条件时,旧 token 失效 → INVALID_ARGUMENT
package paging

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var ErrInvalidToken = errors.New("invalid page_token")

type Pager struct {
	key []byte
}

// New: key 为空时生成随机 key(只适合单实例开发环境,重启后旧 token 失效)。
func New(key []byte) *Pager {
	if len(key) == 0 {
		key = make([]byte, 32)
		_, _ = rand.Read(key)
	}
	return &Pager{key: key}
}

type envelope struct {
	FP     string          `json:"f"`
	Cursor json.RawMessage `json:"c"`
}

func (p *Pager) Encode(fp string, cursor any) (string, error) {
	c, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(envelope{FP: fp, Cursor: c})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(append(body, p.sign(body)...)), nil
}

func (p *Pager) Decode(token, fp string, cursor any) error {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) <= sha256.Size {
		return ErrInvalidToken
	}
	body, sig := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	if !hmac.Equal(sig, p.sign(body)) {
		return ErrInvalidToken
	}
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil || env.FP != fp {
		return ErrInvalidToken
	}
	if err := json.Unmarshal(env.Cursor, cursor); err != nil {
		return ErrInvalidToken
	}
	return nil
}

func (p *Pager) sign(body []byte) []byte {
	m := hmac.New(sha256.New, p.key)
	m.Write(body)
	return m.Sum(nil)
}

// Fingerprint 计算请求里"筛选条件"的指纹: 去掉 exclude 字段(page_token / page_size)后确定性序列化。
func Fingerprint(msg proto.Message, exclude ...string) string {
	c := proto.Clone(msg)
	r := c.ProtoReflect()
	for _, name := range exclude {
		if fd := r.Descriptor().Fields().ByName(protoreflect.Name(name)); fd != nil {
			r.Clear(fd)
		}
	}
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// Size: 0 → def,超过 max → max(protovalidate 已经拒绝越界值,这里是兜底)。
func Size(requested, def, max int32) int32 {
	switch {
	case requested <= 0:
		return def
	case requested > max:
		return max
	}
	return requested
}

// Trim: 查询取 size+1 行,多出来的 1 行只用于判断是否有下一页。
func Trim[T any](rows []T, size int32) ([]T, bool) {
	if len(rows) > int(size) {
		return rows[:size], true
	}
	return rows, false
}
