// Package itest: 集成测试的共用部分(真实的 Postgres / RabbitMQ)。环境变量没有设置时跳过:
//
//	RUNTIME_TEST_DB=postgres://user:pass@localhost:55460/postgres?sslmode=disable   (能 CREATE DATABASE 的用户)
//	RUNTIME_TEST_AMQP=amqp://guest:guest@localhost:55672/
//
// 每个测试一个新的数据库(平台表已建好;测试结束删除),RabbitMQ 上的名字带随机后缀(并行的包 / 测试互不干扰)。
package itest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/broker"
	"github.com/MIERAI/api-factory-runtime/outbox"
)

// Suffix: 随机的名字后缀(小写十六进制)。
func Suffix() string {
	var b [5]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// DBURL: RUNTIME_TEST_DB(没有 → 跳过)。
func DBURL(t testing.TB) string {
	u := os.Getenv("RUNTIME_TEST_DB")
	if u == "" {
		t.Skip("RUNTIME_TEST_DB not set")
	}
	return u
}

// AMQPURL: RUNTIME_TEST_AMQP(没有 → 跳过)。
func AMQPURL(t testing.TB) string {
	u := os.Getenv("RUNTIME_TEST_AMQP")
	if u == "" {
		t.Skip("RUNTIME_TEST_AMQP not set")
	}
	return u
}

// NewDB: 新建一个数据库并建好平台表(PlatformSQL),返回它的 URL。测试结束删除。
func NewDB(t testing.TB) string {
	t.Helper()
	base := DBURL(t)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	name := "rt_" + Suffix()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), base)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	dbURL := u.String()
	c, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, outbox.PlatformSQL); err != nil {
		t.Fatal(err)
	}
	return dbURL
}

// Pool: url 的连接池(和 server 一样的设置: QueryExecModeExec)。测试结束关闭。
func Pool(t testing.TB, dbURL string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

// Broker: RUNTIME_TEST_AMQP 的连接。测试结束关闭。
func Broker(t testing.TB) *broker.Conn {
	t.Helper()
	b := broker.New(AMQPURL(t), "runtime-test")
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// Channel: 测试自己用的 channel。
func Channel(t testing.TB, b *broker.Conn) *amqp.Channel {
	t.Helper()
	ch, err := b.Channel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	return ch
}

// Get: 从 queue 取一条(ack),最多等 timeout。没有 → ok = false。
func Get(t testing.TB, ch *amqp.Channel, queue string, timeout time.Duration) (amqp.Delivery, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		d, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("get %s: %v", queue, err)
		}
		if ok || time.Now().After(deadline) {
			return d, ok
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Depth: 队列里的消息数(被动声明)。
func Depth(t testing.TB, ch *amqp.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("inspect %s: %v", queue, err)
	}
	return q.Messages
}

// Eventually: cond 在 timeout 内变成 true。
func Eventually(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Env: 子进程的环境变量(去掉 FACTORY_TEST_* 以后加上 extra)。
func Env(extra ...string) []string {
	var out []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "FACTORY_TEST_") || strings.HasPrefix(kv, "RUNTIME_HELPER") {
			continue
		}
		out = append(out, kv)
	}
	return append(out, extra...)
}
