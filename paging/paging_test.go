package paging

import (
	"testing"
	"time"
)

type cur struct {
	CreatedAt time.Time `json:"created_at"`
	ID        int64     `json:"id"`
}

func TestRoundTrip(t *testing.T) {
	p := New([]byte("k"))
	in := cur{CreatedAt: time.Date(2026, 11, 6, 0, 0, 0, 123456000, time.UTC), ID: 7}
	tok, err := p.Encode("fp1", in)
	if err != nil {
		t.Fatal(err)
	}
	var out cur
	if err := p.Decode(tok, "fp1", &out); err != nil {
		t.Fatal(err)
	}
	if !out.CreatedAt.Equal(in.CreatedAt) || out.ID != in.ID {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestRejects(t *testing.T) {
	p := New([]byte("k"))
	tok, _ := p.Encode("fp1", cur{ID: 1})
	var out cur
	cases := map[string]error{
		"filter changed": p.Decode(tok, "fp2", &out),
		"other key":      New([]byte("other")).Decode(tok, "fp1", &out),
		"garbage":        p.Decode("not-a-valid-token", "fp1", &out),
		"tampered":       p.Decode(tok[:len(tok)-2]+"AA", "fp1", &out),
	}
	for name, err := range cases {
		if err != ErrInvalidToken {
			t.Errorf("%s: want ErrInvalidToken, got %v", name, err)
		}
	}
}

func TestTrimAndSize(t *testing.T) {
	if r, next := Trim([]int{1, 2, 3, 4}, 3); len(r) != 3 || !next {
		t.Fatal("want 3 rows and next")
	}
	if r, next := Trim([]int{1, 2, 3}, 3); len(r) != 3 || next {
		t.Fatal("want 3 rows and no next")
	}
	if Size(0, 20, 100) != 20 || Size(500, 20, 100) != 100 || Size(5, 20, 100) != 5 {
		t.Fatal("size")
	}
}
