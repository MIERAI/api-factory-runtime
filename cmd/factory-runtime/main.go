// Command factory-runtime: 生成的服务的运维子命令(R4 设计稿 §4.3 的 DLQ 处理)。
//
//	AMQP_URL=amqp://…/prod factory-runtime dlq list    --queue notification.order.confirmed [--limit 20]
//	AMQP_URL=amqp://…/prod factory-runtime dlq requeue --queue notification.order.confirmed [--limit 20]
//	AMQP_URL=amqp://…/prod factory-runtime dlq drop    --queue notification.order.confirmed [--limit 20]
//
// --queue 是主队列的名字(DLQ = <queue>.dlq)。requeue 保留原 id: 已经处理过的会被去重跳过。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/MIERAI/api-factory-runtime/broker"
	"github.com/MIERAI/api-factory-runtime/consume"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "factory-runtime:", err)
		os.Exit(1)
	}
}

func usage() error {
	return fmt.Errorf("usage: factory-runtime dlq {list|requeue|drop} --queue <main queue> [--limit n]  (AMQP_URL must be set)")
}

func run(args []string) error {
	if len(args) < 2 || args[0] != "dlq" {
		return usage()
	}
	verb := args[1]
	fs := flag.NewFlagSet("dlq "+verb, flag.ContinueOnError)
	queue := fs.String("queue", "", "main queue (the DLQ is <queue>.dlq)")
	limit := fs.Int("limit", 20, "at most this many messages")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	url := os.Getenv("AMQP_URL")
	if *queue == "" || url == "" {
		return usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	b := broker.New(url, "factory-runtime dlq")
	defer b.Close()
	switch verb {
	case "list":
		dls, err := consume.DLQList(ctx, b, *queue, *limit)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		for _, d := range dls {
			var body any = string(d.Body) // 毒消息的 body 可能不是 JSON
			if json.Valid(d.Body) {
				body = json.RawMessage(d.Body)
			}
			_ = enc.Encode(map[string]any{
				"id": d.ID, "type": d.Type, "outcome": d.Outcome, "error": d.Error, "attempt": d.Attempt, "body": body,
			})
		}
		return nil
	case "requeue":
		n, err := consume.DLQRequeue(ctx, b, *queue, *limit)
		fmt.Printf("requeued %d\n", n)
		return err
	case "drop":
		n, err := consume.DLQDrop(ctx, b, *queue, *limit)
		fmt.Printf("dropped %d\n", n)
		return err
	}
	return usage()
}
