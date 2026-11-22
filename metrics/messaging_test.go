package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestWriteMessaging(t *testing.T) {
	resetMessaging()
	var empty strings.Builder
	WriteMessaging(&empty)
	if empty.Len() != 0 {
		t.Fatalf("no messaging activity: no output, got\n%s", empty.String())
	}
	RelayLeader(true)
	OutboxBacklog(3, 1500*time.Millisecond)
	OutboxPublished(5)
	OutboxPublishError("unroutable", 1)
	Message("notification.order.confirmed", "ok")
	Message("notification.order.confirmed", "ok")
	Message("notification.order.confirmed", "duplicate")
	HandlerPanic("h")
	MessageLag("notification.order.confirmed", 200*time.Millisecond)
	HandlerDuration("notification.order.confirmed", 3*time.Millisecond)
	DLQSize("notification.order.confirmed.dlq", 2)
	var b strings.Builder
	WriteMessaging(&b)
	out := b.String()
	for _, want := range []string{
		"factory_outbox_pending 3",
		"factory_outbox_oldest_pending_age_seconds 1.5",
		"factory_relay_leader 1",
		"factory_outbox_published_total 5",
		`factory_outbox_publish_errors_total{reason="unroutable"} 1`,
		`factory_messages_total{handler="notification.order.confirmed",outcome="ok"} 2`,
		`factory_messages_total{handler="notification.order.confirmed",outcome="duplicate"} 1`,
		`factory_handler_panics_total{handler="h"} 1`,
		`factory_message_lag_seconds_bucket{handler="notification.order.confirmed",le="0.1"} 0`,
		`factory_message_lag_seconds_bucket{handler="notification.order.confirmed",le="0.25"} 1`,
		`factory_message_lag_seconds_count{handler="notification.order.confirmed"} 1`,
		`factory_handler_duration_seconds_bucket{handler="notification.order.confirmed",le="0.005"} 1`,
		`factory_dlq_messages{queue="notification.order.confirmed.dlq"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s\n%s", want, out)
		}
	}
	if MessageCount("notification.order.confirmed", "ok") != 2 {
		t.Error("MessageCount")
	}
	// 不再是工作者: 积压不导出(新的工作者导出)
	RelayLeader(false)
	b.Reset()
	WriteMessaging(&b)
	if strings.Contains(b.String(), "factory_outbox_pending") || !strings.Contains(b.String(), "factory_relay_leader 0") {
		t.Errorf("standby relay:\n%s", b.String())
	}
}
