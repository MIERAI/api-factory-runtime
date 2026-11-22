package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkers(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var w Workers
	var stopped atomic.Int32
	for i := 0; i < 3; i++ {
		w.Go("ok", func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(20 * time.Millisecond) // 停机时做完手上的事
			stopped.Add(1)
			return nil
		})
	}
	errCh := make(chan error, 4)
	ctx, cancel := context.WithCancel(context.Background())
	wait := w.start(ctx, log, errCh)
	cancel()
	wait(time.Second)
	if stopped.Load() != 3 {
		t.Fatalf("wait must wait for every worker: %d stopped", stopped.Load())
	}
	select {
	case err := <-errCh:
		t.Fatalf("a worker stopping on shutdown is not an error: %v", err)
	default:
	}

	// 提前返回错误 → errCh(服务器停机)
	var w2 Workers
	w2.Go("broken", func(ctx context.Context) error { return errors.New("topology mismatch") })
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	w2.start(ctx2, log, errCh)
	select {
	case err := <-errCh:
		if !strings.Contains(err.Error(), "broken") || !strings.Contains(err.Error(), "topology mismatch") {
			t.Fatalf("error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker error not reported")
	}

	// 停不下来的任务: 最多等 timeout
	var w3 Workers
	w3.Go("stuck", func(ctx context.Context) error { time.Sleep(time.Hour); return nil })
	ctx3, cancel3 := context.WithCancel(context.Background())
	wait3 := w3.start(ctx3, log, errCh)
	cancel3()
	start := time.Now()
	wait3(50 * time.Millisecond)
	if time.Since(start) > time.Second {
		t.Fatal("wait must give up after the timeout")
	}
}
