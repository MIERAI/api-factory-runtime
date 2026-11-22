package server

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Workers: 和服务器同生命周期的后台任务。生成的 wiring 在 Builder 里登记(R4: outbox 的中继、每个处理器的消费循环;
// R5: 状态机的超时扫描),服务器开始监听之后启动,停机时在 HTTP 之后停止(取消 ctx,等它们返回)。
//
// 任务函数应该一直工作到 ctx 结束、然后返回 nil(暂时的故障自己重试: 中继、消费循环都是这样)。
// 在那之前返回错误 = 不能继续(例: 拓扑的参数和 RabbitMQ 上已有的不一致)→ 服务器停机、进程退出(启动失败,不带病运行)。
//
// 准备好了(G19): 任务可以登记"准备好了"的条件(Ready),全部 Done 之前 /readyz 回答 503。生成的 wiring 用它表示
// "拓扑声明好了"(topology.Ensure 成功)和"每个消费循环第一次订阅成功";消费循环也先等拓扑的信号再订阅(G20)。
// 只管启动: 准备好之后 RabbitMQ 断开不会让 /readyz 变回 503(依赖的故障不进 readiness,见 server.Run 的说明)。
type Workers struct {
	list []worker

	mu    sync.Mutex
	ready []*Signal
}

// Signal: 一个"准备好了"的条件。Done 可以调用多次(只有第一次算)。
type Signal struct {
	name string
	once sync.Once
	ch   chan struct{}
}

// Done: 这个条件满足了。
func (s *Signal) Done() { s.once.Do(func() { close(s.ch) }) }

// C: 条件满足时关闭的 channel(消费循环等拓扑用)。
func (s *Signal) C() <-chan struct{} { return s.ch }

// Name: 登记时的名字(/readyz 的 503 里列出还没满足的)。
func (s *Signal) Name() string { return s.name }

func (s *Signal) done() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

// Ready: 登记一个"准备好了"的条件(Builder 里调用)。所有登记过的条件 Done 之前 /readyz 回答 503。
func (w *Workers) Ready(name string) *Signal {
	s := &Signal{name: name, ch: make(chan struct{})}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ready = append(w.ready, s)
	return s
}

// Pending: 还没满足的条件的名字(排序;空 = 准备好了)。
func (w *Workers) Pending() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for _, s := range w.ready {
		if !s.done() {
			out = append(out, s.name)
		}
	}
	sort.Strings(out)
	return out
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
	w.mu.Lock()
	signals := append([]*Signal(nil), w.ready...)
	w.mu.Unlock()
	if len(signals) > 0 { // 全部满足时记一行日志(部署时看得到"准备好了"花了多久)
		go func() {
			start := time.Now()
			for _, s := range signals {
				select {
				case <-s.ch:
				case <-ctx.Done():
					return
				}
			}
			log.Info("ready", "conditions", len(signals), "after_ms", time.Since(start).Milliseconds())
		}()
	}
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
