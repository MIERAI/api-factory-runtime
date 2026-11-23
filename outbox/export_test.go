package outbox

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SetConfirmWait: 测试用,替换中继等 publisher confirm 的方式(模拟 broker 不回 confirm)。
func SetConfirmWait(r *Relay, f func(ctx context.Context, c *amqp.DeferredConfirmation) (bool, error)) {
	r.confirmWait = f
}

// Kick: 测试用,提交之后的唤醒(合并)。
var Kick = kick
