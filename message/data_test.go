package message

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/descriptorpb"
)

func TestData(t *testing.T) {
	m := &descriptorpb.FieldDescriptorProto{}
	env := Envelope{Type: "t.x", Version: 2, Data: []byte(`{"name": "sku", "number": 3, "json_name": "skuX", "unknown_field": [1, 2]}`)}
	if err := DecodeData(env, m); err != nil || m.GetName() != "sku" || m.GetNumber() != 3 || m.GetJsonName() != "skuX" {
		t.Fatalf("decode (unknown fields ignored): %v %v", err, m)
	}
	env.Data = []byte(`{"number": "not a number"}`)
	if err := DecodeData(env, m); !IsDecodeError(err) || !strings.Contains(err.Error(), "t.x v2: data:") {
		t.Errorf("bad data must be a DecodeError (poison): %v", err)
	}
	b, err := EncodeData(&descriptorpb.FieldDescriptorProto{Name: new(string)})
	if err != nil || !strings.Contains(string(b), `"name":""`) {
		t.Errorf("encode: %s %v (want proto names, zero values written)", b, err)
	}
}
