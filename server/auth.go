package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// 认证: AUTH_MODE 必须显式指定(不写就不启动 —— 不会因为忘了配置而对外裸奔)。
//
//	AUTH_MODE=token  请求要带 Authorization: Bearer <令牌>。服务器只保存令牌的 SHA-256(AUTH_TOKEN_SHA256,逗号隔开,可以多个: 轮换用)
//	AUTH_MODE=none   服务器不认证: 由平台负责(Cloud Run 的 IAM「需要认证」/ 网关),或者本地测试
//
// 探针(/livez /readyz /healthz)不需要认证(平台的健康检查)。
type auth struct {
	mode   string
	hashes [][]byte
}

func loadAuth() (*auth, error) {
	a := &auth{mode: os.Getenv("AUTH_MODE")}
	switch a.mode {
	case "none":
		return a, nil
	case "token":
		for _, h := range strings.Split(os.Getenv("AUTH_TOKEN_SHA256"), ",") {
			h = strings.TrimSpace(h)
			if h == "" {
				continue
			}
			b, err := hex.DecodeString(h)
			if err != nil || len(b) != sha256.Size {
				return nil, fmt.Errorf("AUTH_TOKEN_SHA256: %q is not a sha256 hex", h)
			}
			a.hashes = append(a.hashes, b)
		}
		if len(a.hashes) == 0 {
			return nil, errors.New("AUTH_MODE=token needs AUTH_TOKEN_SHA256 (sha256 hex of each token, comma separated)")
		}
		return a, nil
	case "":
		return nil, errors.New("AUTH_MODE is required: token (Bearer token checked by the server) or none (the platform authenticates, e.g. Cloud Run IAM)")
	}
	return nil, fmt.Errorf("AUTH_MODE=%q: must be token or none", a.mode)
}

var probePaths = map[string]bool{"/livez": true, "/readyz": true, "/healthz": true}

// wrap: 除了探针(/livez /readyz /healthz),都检查令牌。
func (a *auth) wrap(next http.Handler) http.Handler {
	if a.mode == "none" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if probePaths[r.URL.Path] || a.ok(r.Header.Get("Authorization")) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("WWW-Authenticate", `Bearer realm="api"`)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"missing or invalid bearer token"}`))
	})
}

func (a *auth) ok(header string) bool {
	token, found := strings.CutPrefix(header, "Bearer ")
	if !found || token == "" {
		return false
	}
	sum := sha256.Sum256([]byte(token))
	match := 0
	for _, h := range a.hashes { // 全部比较(不提前返回): 时间不泄露是第几个
		match |= subtle.ConstantTimeCompare(sum[:], h)
	}
	return match == 1
}
