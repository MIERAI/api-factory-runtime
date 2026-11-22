package message

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNewID(t *testing.T) {
	seen := map[string]bool{}
	before := time.Now().Add(-time.Millisecond)
	for i := 0; i < 1000; i++ {
		id := NewID()
		if !ValidID(id) || id[14] != '7' || !strings.ContainsRune("89ab", rune(id[19])) {
			t.Fatalf("not a UUIDv7: %s", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
	}
	ts, ok := IDTime(NewID())
	if !ok || ts.Before(before.Truncate(time.Millisecond)) || ts.After(time.Now().Add(time.Millisecond)) {
		t.Fatalf("IDTime: %v %v", ts, ok)
	}
	if _, ok := IDTime("6ba7b810-9dad-11d1-80b4-00c04fd430c8"); ok {
		t.Error("IDTime of a v1 UUID")
	}
	for _, bad := range []string{"", "x", "6ba7b810-9dad-11d1-80b4-00c04fd430c", "6ba7b810x9dad-11d1-80b4-00c04fd430c8", "gba7b810-9dad-11d1-80b4-00c04fd430c8"} {
		if ValidID(bad) {
			t.Errorf("ValidID(%q)", bad)
		}
	}
}

func TestEncodeDecode(t *testing.T) {
	at := time.Date(2026, 11, 22, 1, 2, 3, 456789123, time.FixedZone("x", 9*3600))
	e := Envelope{ID: NewID(), Type: "task.created", Version: 1, OccurredAt: at, Source: "tasks", Subject: "7",
		Data: json.RawMessage(`{"task_id":"7","extra":{"a":1}}`)}
	b, err := e.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"occurred_at":"2026-11-21T16:02:03.456789Z"`) {
		t.Errorf("occurred_at must be UTC with microseconds: %s", b)
	}
	if strings.Contains(string(b), "traceparent") {
		t.Errorf("empty traceparent is omitted: %s", b)
	}
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != e.ID || got.Type != e.Type || got.Subject != "7" || !got.OccurredAt.Equal(at.Truncate(time.Microsecond)) || string(got.Data) != string(e.Data) {
		t.Errorf("roundtrip: %+v", got)
	}
	// 不认识的字段忽略
	if _, err := Decode([]byte(`{"id":"` + e.ID + `","type":"t","version":2,"occurred_at":"2026-01-01T00:00:00Z","source":"s","subject":"","data":{},"new_field":1}`)); err != nil {
		t.Errorf("unknown fields must be ignored: %v", err)
	}
	bad := map[string]string{
		"not json":      `{`,
		"bad id":        `{"id":"x","type":"t","version":1,"occurred_at":"2026-01-01T00:00:00Z","source":"s","data":{}}`,
		"no type":       `{"id":"` + e.ID + `","version":1,"occurred_at":"2026-01-01T00:00:00Z","source":"s","data":{}}`,
		"version 0":     `{"id":"` + e.ID + `","type":"t","version":0,"occurred_at":"2026-01-01T00:00:00Z","source":"s","data":{}}`,
		"no time":       `{"id":"` + e.ID + `","type":"t","version":1,"source":"s","data":{}}`,
		"no source":     `{"id":"` + e.ID + `","type":"t","version":1,"occurred_at":"2026-01-01T00:00:00Z","data":{}}`,
		"data not obj":  `{"id":"` + e.ID + `","type":"t","version":1,"occurred_at":"2026-01-01T00:00:00Z","source":"s","data":[1]}`,
		"data missing":  `{"id":"` + e.ID + `","type":"t","version":1,"occurred_at":"2026-01-01T00:00:00Z","source":"s"}`,
		"version float": `{"id":"` + e.ID + `","type":"t","version":1.5,"occurred_at":"2026-01-01T00:00:00Z","source":"s","data":{}}`,
	}
	for name, body := range bad {
		if _, err := Decode([]byte(body)); !IsDecodeError(err) {
			t.Errorf("%s: want a decode error, got %v", name, err)
		}
	}
}

func TestTraceparent(t *testing.T) {
	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if !ValidTraceparent(parent) {
		t.Fatal("valid traceparent rejected")
	}
	child := ChildTraceparent(parent)
	if !ValidTraceparent(child) || child[:36] != parent[:36] || child[52:] != parent[52:] || child[36:52] == parent[36:52] {
		t.Fatalf("child %s of %s", child, parent)
	}
	for _, bad := range []string{"", "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", "00-4bf92f3577b34da6a3ce929d0e0e473z-00f067aa0ba902b7-01"} {
		if ValidTraceparent(bad) || ChildTraceparent(bad) != "" {
			t.Errorf("invalid traceparent accepted: %q", bad)
		}
	}
	ctx := WithTraceparent(context.Background(), "garbage")
	if Traceparent(ctx) != "" {
		t.Error("invalid traceparent stored")
	}
	if Traceparent(WithTraceparent(ctx, parent)) != parent {
		t.Error("traceparent not stored")
	}
}

func TestHeaders(t *testing.T) {
	h := map[string]any{"a": int32(3), "b": int64(4), "c": "5", "d": "x", "e": []byte("y"), "f": int16(6), "g": uint8(7)}
	for k, want := range map[string]int{"a": 3, "b": 4, "c": 5, "f": 6, "g": 7} {
		if n, ok := HeaderInt(h, k); !ok || n != want {
			t.Errorf("HeaderInt(%s) = %d %v", k, n, ok)
		}
	}
	if _, ok := HeaderInt(h, "d"); ok {
		t.Error("HeaderInt of a non-number")
	}
	if _, ok := HeaderInt(h, "missing"); ok {
		t.Error("HeaderInt of a missing key")
	}
	if HeaderString(h, "d") != "x" || HeaderString(h, "e") != "y" || HeaderString(h, "missing") != "" {
		t.Error("HeaderString")
	}
}
