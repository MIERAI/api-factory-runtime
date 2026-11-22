package leader_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/MIERAI/api-factory-runtime/internal/itest"
	"github.com/MIERAI/api-factory-runtime/leader"
)

func TestKey(t *testing.T) {
	if leader.Key("factory.outbox.relay") != leader.Key("factory.outbox.relay") || leader.Key("a") == leader.Key("b") {
		t.Fatal("Key must be deterministic and distinguish names")
	}
}

// 同一个数据库上多个副本: 任何时刻最多一个在工作;工作者退出(连接断开)→ 待命的接手。
func TestSingleLeader(t *testing.T) {
	pool := itest.Pool(t, itest.NewDB(t))
	name := "factory.test." + itest.Suffix()
	var active, maxActive, terms atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = leader.Run(ctx, pool, name, leader.Options{Retry: 20 * time.Millisecond}, func(ctx context.Context, conn *pgx.Conn) error {
				n := active.Add(1)
				defer active.Add(-1)
				for {
					m := maxActive.Load()
					if n <= m || maxActive.CompareAndSwap(m, n) {
						break
					}
				}
				terms.Add(1)
				select { // 一个任期 100ms,然后"连接断了"(返回错误)→ 别人接手
				case <-ctx.Done():
					return nil
				case <-time.After(100 * time.Millisecond):
				}
				// 模拟会话结束: 关掉持锁的连接
				_ = conn.Close(context.Background())
				return context.DeadlineExceeded
			})
		}()
	}
	time.Sleep(time.Second)
	cancel()
	wg.Wait()
	if maxActive.Load() != 1 {
		t.Fatalf("at most one leader at a time, saw %d", maxActive.Load())
	}
	if terms.Load() < 3 {
		t.Fatalf("leadership must move on after a lost session: %d terms", terms.Load())
	}
	// 结束后锁已经释放: 别的会话能拿到
	var got bool
	if err := pool.QueryRow(context.Background(), `SELECT pg_try_advisory_lock($1)`, leader.Key(name)).Scan(&got); err != nil || !got {
		t.Fatalf("lock not released after shutdown: %v %v", got, err)
	}
}
