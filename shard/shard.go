// Package shard: 分库(按店铺等)的连接路由。
//
// 一个请求只访问一个库(分库键由必填的「等于」条件固定,工厂编译时检查),所以查询的语义不变,
// 只是"用哪个连接"由分库键决定。配置:
//
//	FACTORY_SHARDS="1-5=postgres://…/business0;6-10=postgres://…/business1;*=postgres://…/business9"
//
// 规则从左到右匹配;没有配置(或没有匹配的规则且没有 *)时用主库。
package shard

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Rule: lo~hi(含)→ URL;Any = "*"。
type Rule struct {
	Lo, Hi int64
	Any    bool
	URL    string
}

// Parse: FACTORY_SHARDS 的写法 → 规则。
func Parse(spec string) ([]Rule, error) {
	var out []Rule
	for _, part := range strings.Split(spec, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, url, ok := strings.Cut(part, "=")
		if !ok || url == "" {
			return nil, fmt.Errorf("FACTORY_SHARDS: %q 要写成 lo-hi=URL 或 *=URL", part)
		}
		key = strings.TrimSpace(key)
		if key == "*" {
			out = append(out, Rule{Any: true, URL: url})
			continue
		}
		a, b, isRange := strings.Cut(key, "-")
		if !isRange {
			b = a
		}
		lo, err1 := strconv.ParseInt(strings.TrimSpace(a), 10, 64)
		hi, err2 := strconv.ParseInt(strings.TrimSpace(b), 10, 64)
		if err1 != nil || err2 != nil || lo > hi {
			return nil, fmt.Errorf("FACTORY_SHARDS: %q 的范围不对", part)
		}
		out = append(out, Rule{Lo: lo, Hi: hi, URL: url})
	}
	return out, nil
}

// Pick: 分库键 → URL。没有匹配 → ""(用主库)。
func Pick(rules []Rule, key int64) string {
	for _, r := range rules {
		if r.Any || (r.Lo <= key && key <= r.Hi) {
			return r.URL
		}
	}
	return ""
}

// DBTX: 和 gen/db.DBTX 相同的接口(平台不依赖生成的代码)。
type DBTX interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

// Router: 分库键 → 连接池(第一次用到时创建)。
type Router struct {
	rules   []Rule
	main    DBTX
	newPool func(ctx context.Context, url string) (*pgxpool.Pool, error)
	mu      sync.Mutex
	pools   map[string]*pgxpool.Pool
}

func New(rules []Rule, main DBTX, newPool func(ctx context.Context, url string) (*pgxpool.Pool, error)) *Router {
	return &Router{rules: rules, main: main, newPool: newPool, pools: map[string]*pgxpool.Pool{}}
}

// For: 分库键对应的连接。连接池创建失败时返回一个"每次执行都报这个错"的连接
// (handler 不需要额外的错误处理,查询的错误照常返回)。
func (r *Router) For(key int64) DBTX {
	url := Pick(r.rules, key)
	if url == "" {
		return r.main
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.pools[url]; ok {
		return p
	}
	p, err := r.newPool(context.Background(), url)
	if err != nil {
		return errDB{fmt.Errorf("shard for key %d: %w", key, err)}
	}
	r.pools[url] = p
	return p
}

func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.pools {
		p.Close()
	}
}

type errDB struct{ err error }

func (e errDB) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, e.err
}
func (e errDB) Query(context.Context, string, ...interface{}) (pgx.Rows, error) { return nil, e.err }
func (e errDB) QueryRow(context.Context, string, ...interface{}) pgx.Row        { return errRow{e.err} }

type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }
