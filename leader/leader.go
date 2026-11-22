// Package leader: "每个数据库同时只有一个在工作"的后台任务(R4 E3 的 outbox 中继;R5 S8 的超时扫描也用它)。
//
// 每个服务副本都运行同一个任务;拿到 Postgres 会话级 advisory lock(pg_try_advisory_lock)的那个工作,
// 其余的待命,每隔一段时间再试。锁属于一个专用的连接(会话): 进程崩溃 / 连接断开 → Postgres 释放锁 → 待命的副本接手。
// 任务函数拿到的就是持锁的那个连接: 连接坏了,它的查询会失败、函数返回错误 → 这里销毁连接(确保锁释放)、退避、重新竞选。
//
// R5 的超时扫描: 在 server 的后台任务(server.Deps.Workers)里调用 leader.Run(ctx, pool, "factory.sm.timeout.<机器>", …)。
package leader

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Key: 任务名 → advisory lock 的键(64 位 FNV-1a;名字带 "factory." 前缀,避免和项目自己的 advisory lock 撞)。
func Key(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return int64(h.Sum64())
}

// Options: 都有默认值。
type Options struct {
	// Retry: 没拿到锁 / 任务出错之后,隔多久再试(默认 5s)
	Retry time.Duration
	// Log: 默认 slog.Default()
	Log *slog.Logger
	// OnChange: 成为 / 不再是工作者时调用(指标用;可以为 nil)
	OnChange func(leading bool)
}

// Run: 一直竞选并在当选时执行 fn(ctx, conn),直到 ctx 结束(返回 nil)。
// fn 应该一直工作到 ctx 结束;返回错误 = 这一任期结束(连接被销毁、锁释放,退避后重新竞选);返回 nil 且 ctx 没结束 → 同样重新竞选。
// fn 里的查询应该用 conn(持锁的会话): 这样锁丢了(连接断了)查询就会失败。
func Run(ctx context.Context, pool *pgxpool.Pool, name string, opts Options, fn func(ctx context.Context, conn *pgx.Conn) error) error {
	if opts.Retry <= 0 {
		opts.Retry = 5 * time.Second
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	key := Key(name)
	for {
		err := term(ctx, pool, key, opts, fn)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !errors.Is(err, errNotLeader) {
			log.Warn("leader: term ended", "task", name, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opts.Retry):
		}
	}
}

var errNotLeader = errors.New("leader: lock held by another instance")

func term(ctx context.Context, pool *pgxpool.Pool, key int64, opts Options, fn func(ctx context.Context, conn *pgx.Conn) error) (err error) {
	c, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	var got bool
	if err := c.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&got); err != nil {
		c.Release()
		return err
	}
	if !got {
		c.Release()
		return errNotLeader
	}
	if opts.OnChange != nil {
		opts.OnChange(true)
	}
	defer func() {
		if opts.OnChange != nil {
			opts.OnChange(false)
		}
		// 正常结束: 解锁后还回连接池。出错 / 解锁失败: 销毁连接(会话结束 = 锁一定释放),不把持锁的会话还回池里
		if err == nil || ctx.Err() != nil {
			uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer cancel()
			_, uerr := c.Exec(uctx, `SELECT pg_advisory_unlock($1)`, key)
			if uerr == nil {
				_, uerr = c.Exec(uctx, `UNLISTEN *`) // 任务可能 LISTEN 过(中继): 别把订阅留给池里的下一个使用者
			}
			if uerr == nil {
				c.Release()
				return
			}
		}
		_ = c.Hijack().Close(context.WithoutCancel(ctx))
	}()
	return fn(ctx, c.Conn())
}
