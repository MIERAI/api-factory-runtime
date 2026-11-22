package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Workers: 和服务器同生命周期的后台任务。生成的 wiring 在 Builder 里登记(R4: outbox 的中继、每个处理器的消费循环;
// R5: 状态机的超时扫描),服务器开始监听之后启动,停机时在 HTTP 之后停止(取消 ctx,等它们返回)。
//
// 任务函数应该一直工作到 ctx 结束、然后返回 nil(暂时的故障自己重试: 中继、消费循环都是这样)。
// 在那之前返回错误 = 不能继续(例: 拓扑的参数和 RabbitMQ 上已有的不一致)→ 服务器停机、进程退出(启动失败,不带病运行)。
type Workers struct {
	list []worker
}

type worker struct {
	name string
	run  func(ctx context.Context) error
}

// Go: 登记一个后台任务(Builder 里调用;服务器开始监听之后才启动)。
func (w *Workers) Go(name string, run func(ctx context.Context) error) {
	w.list = append(w.list, worker{name: name, run: run})
}

// start: 启动全部任务;返回"等它们结束"的函数(最多等 timeout)。提前返回错误的任务把错误送到 errCh。
func (w *Workers) start(ctx context.Context, log *slog.Logger, errCh chan<- error) func(timeout time.Duration) {
	var wg sync.WaitGroup
	for _, t := range w.list {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("worker started", "worker", t.name)
			err := t.run(ctx)
			switch {
			case ctx.Err() != nil:
				log.Info("worker stopped", "worker", t.name)
			case err != nil:
				log.Error("worker failed", "worker", t.name, "err", err)
				errCh <- fmt.Errorf("worker %s: %w", t.name, err)
			default:
				log.Warn("worker returned before shutdown", "worker", t.name)
			}
		}()
	}
	return func(timeout time.Duration) {
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(timeout):
			log.Warn("workers did not stop in time", "timeout", timeout)
		}
	}
}
