package server

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuth(t *testing.T) {
	sum := func(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
	for _, c := range []struct {
		env     map[string]string
		wantErr bool
	}{
		{map[string]string{}, true},
		{map[string]string{"AUTH_MODE": "token"}, true},
		{map[string]string{"AUTH_MODE": "token", "AUTH_TOKEN_SHA256": "abc"}, true},
		{map[string]string{"AUTH_MODE": "maybe"}, true},
		{map[string]string{"AUTH_MODE": "none"}, false},
	} {
		t.Setenv("AUTH_MODE", c.env["AUTH_MODE"])
		t.Setenv("AUTH_TOKEN_SHA256", c.env["AUTH_TOKEN_SHA256"])
		if _, err := loadAuth(); (err != nil) != c.wantErr {
			t.Errorf("%v: err = %v", c.env, err)
		}
	}
	t.Setenv("AUTH_MODE", "token")
	t.Setenv("AUTH_TOKEN_SHA256", sum("old")+", "+sum("new")) // 轮换中: 两个都有效
	a, err := loadAuth()
	if err != nil {
		t.Fatal(err)
	}
	h := a.wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	for _, c := range []struct {
		path, header string
		want         int
	}{
		{"/x.v1.S/M", "", 401},
		{"/x.v1.S/M", "Bearer wrong", 401},
		{"/x.v1.S/M", "Basic new", 401},
		{"/x.v1.S/M", "Bearer ", 401},
		{"/x.v1.S/M", "Bearer new", 200},
		{"/x.v1.S/M", "Bearer old", 200},
		{"/healthz", "", 200},
		{"/livez", "", 200},
		{"/readyz", "", 200},
		{"/metrics", "", 401},
	} {
		r := httptest.NewRequest("POST", c.path, nil)
		if c.header != "" {
			r.Header.Set("Authorization", c.header)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != c.want {
			t.Errorf("%s %q: %d, want %d", c.path, c.header, w.Code, c.want)
		}
	}
}
