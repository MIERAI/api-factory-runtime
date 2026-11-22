package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MIERAI/api-factory-runtime/broker"
	"github.com/MIERAI/api-factory-runtime/leader"
	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/metrics"
	"github.com/MIERAI/api-factory-runtime/topology"
)

// LockName: 中继的 advisory lock(leader.Key(LockName)),每个数据库一个工作中的中继。
const LockName = "factory.outbox.relay"

// Relay: outbox 的中继。每个服务进程一个(server 的后台任务),每个数据库同时只有一个在工作。零值的字段用默认值。
//
// 一轮:
//  1. SELECT 未发送的行 ORDER BY seq LIMIT Batch(在持锁的连接上;不加行锁,不在事务里做网络 IO)
//  2. 在 confirm 模式的 channel 上逐条发布: 持久化;message_id = envelope.id,type / traceparent 也放进 AMQP 属性;
//     都开 mandatory(没有队列接收 → RabbitMQ 退回):
//     - 命令: 退回 = 配置错误,当作失败留在 outbox 里(按退避再试,队列声明之后就发出去)
//     - 事件: 退回 = 没有任何订阅队列(系统级的拓扑没有声明,或者真的没人订阅)。标成已发送(不堵 outbox:
//     系统外的订阅者 / 可选的事件可能永远没有队列),但不悄悄地: last_error = "unroutable: …"、
//     factory_outbox_unroutable_total{kind="event",type} + ERROR 日志(告警看这个计数)
//  3. 等这一批的 confirm: ack 的 → published_at = now();nack / 超时 / 退回的命令 → attempts + 1、last_error,
//     这一行按 1s → 30s 的退避再试(别的行照常发)
//  4. 每小时: 删除发送超过 Retention 的行、处理超过 DedupRetention 的去重记录(每次 ≤ 1000 行)
//
// 没有要发的就等 NOTIFY factory_outbox 或者 Poll(兜底轮询)。RabbitMQ 连不上 → 整体退避(1s → 30s)。
type Relay struct {
	DB        *pgxpool.Pool
	Broker    *broker.Conn
	Exchanges topology.Exchanges // 零值 = topology.DefaultExchanges

	Batch          int           // 一批最多几条(默认 100)
	Poll           time.Duration // 没有 NOTIFY 时的轮询间隔(默认 1s)
	ConfirmTimeout time.Duration // 等一批 confirm 的上限(默认 10s)
	MaxBackoff     time.Duration // 退避的上限(默认 30s;从 1s 开始翻倍)
	Retention      time.Duration // 已发送的行保留多久(默认 7 天)
	DedupRetention time.Duration // factory_processed_messages 保留多久(默认 14 天;0 以下 = 不清理)
	CleanupEvery   time.Duration // 清理的间隔(默认 1 小时)
	LeaderRetry    time.Duration // 待命的副本隔多久再试着拿锁(默认 5s)
	Log            *slog.Logger

	crash   crashPlan
	batches int
	skip    map[int64]time.Time // 失败的行: 下次可以再发的时刻(只在内存里;换了工作者就从头退避)
}

func (r *Relay) defaults() {
	if r.Batch <= 0 {
		r.Batch = 100
	}
	if r.Poll <= 0 {
		r.Poll = time.Second
	}
	if r.ConfirmTimeout <= 0 {
		r.ConfirmTimeout = 10 * time.Second
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = 30 * time.Second
	}
	if r.Retention <= 0 {
		r.Retention = 7 * 24 * time.Hour
	}
	if r.DedupRetention == 0 {
		r.DedupRetention = 14 * 24 * time.Hour
	}
	if r.CleanupEvery <= 0 {
		r.CleanupEvery = time.Hour
	}
	if r.Exchanges == (topology.Exchanges{}) {
		r.Exchanges = topology.DefaultExchanges
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
	if r.skip == nil {
		r.skip = map[int64]time.Time{}
	}
}

// Run: 竞选并工作,直到 ctx 结束(返回 nil)。测试的崩溃点写错了 → 立刻返回错误。
func (r *Relay) Run(ctx context.Context) error {
	r.defaults()
	plan, err := relayCrashPlan()
	if err != nil {
		return err
	}
	if plan.point != "" {
		r.Log.Warn(RelayCrashEnv + " is set: the relay will exit(137) at " + plan.point + " (test environments only)")
		r.crash = plan
	}
	return leader.Run(ctx, r.DB, LockName, leader.Options{Retry: r.LeaderRetry, Log: r.Log, OnChange: metrics.RelayLeader}, r.lead)
}

// errBroker: RabbitMQ 这一侧的失败(channel / 连接坏了): 换 channel、整体退避,不放弃工作者的身份。
type errBroker struct{ err error }

func (e errBroker) Error() string { return "broker: " + e.err.Error() }
func (e errBroker) Unwrap() error { return e.err }

func (r *Relay) lead(ctx context.Context, conn *pgx.Conn) error {
	r.Log.Info("outbox relay: active")
	if _, err := conn.Exec(ctx, `LISTEN `+NotifyChannel); err != nil {
		return err
	}
	var (
		ch          *amqp.Channel
		returns     chan amqp.Return
		backoff     time.Duration
		lastCleanup time.Time
	)
	defer func() {
		if ch != nil {
			_ = ch.Close()
		}
	}()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := r.backlog(ctx, conn); err != nil {
			return err
		}
		if time.Since(lastCleanup) >= r.CleanupEvery {
			if err := Cleanup(ctx, conn, r.Retention, r.DedupRetention); err != nil {
				return err
			}
			lastCleanup = time.Now()
		}
		if ch == nil || ch.IsClosed() {
			var err error
			ch, returns, err = r.channel(ctx)
			if err != nil {
				metrics.OutboxPublishError("broker", 0)
				backoff = nextBackoff(backoff, r.MaxBackoff)
				r.Log.Warn("outbox relay: broker unavailable", "err", err, "retry_in", backoff)
				if err := r.wait(ctx, conn, backoff); err != nil {
					return err
				}
				continue
			}
		}
		n, err := r.round(ctx, conn, ch, returns)
		var be errBroker
		switch {
		case errors.As(err, &be):
			_ = ch.Close()
			ch = nil
			backoff = nextBackoff(backoff, r.MaxBackoff)
			r.Log.Warn("outbox relay: publish failed", "err", err, "retry_in", backoff)
			if err := r.wait(ctx, conn, backoff); err != nil {
				return err
			}
			continue
		case err != nil:
			return err // 数据库: 结束这一任期(连接可能已经断了,锁也就没了)
		}
		backoff = 0
		if n >= r.Batch {
			continue // 还有: 马上下一批
		}
		if err := r.wait(ctx, conn, r.Poll); err != nil {
			return err
		}
	}
}

func (r *Relay) channel(ctx context.Context) (*amqp.Channel, chan amqp.Return, error) {
	ch, err := r.Broker.ConfirmChannel(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := topology.DeclareExchanges(ch, r.Exchanges); err != nil {
		_ = ch.Close()
		return nil, nil, err
	}
	// basic.return 在同一批的 basic.ack 之前送达(同一个 channel 上按顺序分发),缓冲要大于一批: 满了会堵住连接的读循环
	returns := ch.NotifyReturn(make(chan amqp.Return, r.Batch+16))
	return ch, returns, nil
}

// wait: 等 NOTIFY 或者超时。连接坏了 → 错误(结束任期)。
func (r *Relay) wait(ctx context.Context, conn *pgx.Conn, d time.Duration) error {
	wctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	_, err := conn.WaitForNotification(wctx)
	if err == nil || wctx.Err() != nil {
		return nil // 被唤醒 / 超时 / ctx 结束
	}
	return err
}

type row struct {
	seq      int64
	id       string
	kind     message.Kind
	typ      string
	version  int32
	envelope string
}

// round: 一批。返回取到的行数。
func (r *Relay) round(ctx context.Context, conn *pgx.Conn, ch *amqp.Channel, returns chan amqp.Return) (int, error) {
	now := time.Now()
	skip := make([]int64, 0, len(r.skip))
	for seq, at := range r.skip {
		if now.Before(at) {
			skip = append(skip, seq)
		} else {
			delete(r.skip, seq)
		}
	}
	rows, err := conn.Query(ctx, `SELECT seq, id::text, kind, type, version, envelope::text FROM factory_outbox
WHERE published_at IS NULL AND seq <> ALL($2::bigint[]) ORDER BY seq LIMIT $1`, r.Batch, skip)
	if err != nil {
		return 0, err
	}
	batch, err := pgx.CollectRows(rows, func(rw pgx.CollectableRow) (row, error) {
		var x row
		var kind int16
		err := rw.Scan(&x.seq, &x.id, &kind, &x.typ, &x.version, &x.envelope)
		x.kind = message.Kind(kind)
		return x, err
	})
	if err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}
	r.batches++
	// 这一批发出去了就要等完 confirm、标记完: 停机(ctx 结束)不打断它,只是不再取下一批
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.ConfirmTimeout)
	defer cancel()

	confirms := make([]*amqp.DeferredConfirmation, len(batch))
	failed := map[int64]string{}
	var pubErr error
	for i, x := range batch {
		ex, err := r.Exchanges.For(x.kind)
		if err != nil {
			failed[x.seq] = err.Error()
			continue
		}
		var tp string
		if env, err := message.Decode([]byte(x.envelope)); err == nil {
			tp = env.Traceparent
		}
		pub := amqp.Publishing{
			ContentType:  message.ContentType,
			DeliveryMode: amqp.Persistent,
			MessageId:    x.id,
			Type:         x.typ,
			Body:         []byte(x.envelope),
			Headers:      amqp.Table{message.HeaderVersion: x.version},
		}
		if tp != "" {
			pub.Headers[message.HeaderTraceparent] = tp
		}
		confirms[i], err = ch.PublishWithDeferredConfirmWithContext(bctx, ex, x.typ, true, false, pub)
		if err != nil {
			pubErr = err
			break
		}
	}
	if r.crash.point == CrashAfterPublish && r.batches == r.crash.batch {
		exit()
	}
	if pubErr != nil { // channel / 连接坏了: 这一批都不算发出(发出去的那几条之后会同 id 再发)
		for _, x := range batch {
			failed[x.seq] = pubErr.Error()
		}
		if err := r.markFailed(bctx, conn, batch, failed); err != nil {
			return 0, err
		}
		metrics.OutboxPublishError("broker", len(batch))
		return len(batch), errBroker{pubErr}
	}
	var brokerErr error
	for i, x := range batch {
		if confirms[i] == nil {
			continue
		}
		acked, err := confirms[i].WaitContext(bctx)
		switch {
		case err != nil:
			failed[x.seq] = "confirm timeout"
			metrics.OutboxPublishError("timeout", 1)
			brokerErr = errors.New("publisher confirm timed out")
		case !acked:
			failed[x.seq] = "nack from broker"
			metrics.OutboxPublishError("nack", 1)
		}
	}
	unroutable := map[int64]string{} // 退回的事件: 标成已发送,记下 last_error
	for done := false; !done; {      // 退回的(mandatory: 没有队列接收)
		select {
		case ret := <-returns:
			for _, x := range batch {
				if x.id != ret.MessageId {
					continue
				}
				why := fmt.Sprintf("unroutable: %d %s (exchange %s, routing key %s)", ret.ReplyCode, ret.ReplyText, ret.Exchange, ret.RoutingKey)
				if x.kind == message.Command {
					failed[x.seq] = why
					metrics.OutboxPublishError("unroutable", 1)
					metrics.OutboxUnroutable("command", x.typ)
					r.Log.Error("outbox relay: command has no queue (configuration error); kept in the outbox and retried", "type", x.typ, "id", x.id)
					continue
				}
				unroutable[x.seq] = why
				metrics.OutboxUnroutable("event", x.typ)
				r.Log.Error("outbox relay: event has no subscriber queue and is dropped (is the system topology applied?)", "type", x.typ, "id", x.id)
			}
		default:
			done = true
		}
	}
	if r.crash.point == CrashAfterConfirm && r.batches == r.crash.batch {
		exit()
	}
	ok := make([]int64, 0, len(batch))
	for _, x := range batch {
		if _, bad := failed[x.seq]; !bad {
			ok = append(ok, x.seq)
		}
	}
	if len(ok) > 0 {
		if _, err := conn.Exec(bctx, `UPDATE factory_outbox SET published_at = now() WHERE seq = ANY($1::bigint[])`, ok); err != nil {
			return 0, err
		}
		metrics.OutboxPublished(len(ok))
	}
	for _, x := range batch { // 退回的事件: 发出去了但没人收 —— outbox 里留下记录(运维查 last_error LIKE 'unroutable%')
		if why, bad := unroutable[x.seq]; bad {
			if _, err := conn.Exec(bctx, `UPDATE factory_outbox SET last_error = $2 WHERE seq = $1`, x.seq, why); err != nil {
				return 0, err
			}
		}
	}
	if err := r.markFailed(bctx, conn, batch, failed); err != nil {
		return 0, err
	}
	if brokerErr != nil {
		return len(batch), errBroker{brokerErr}
	}
	return len(batch), nil
}

// markFailed: attempts + 1、last_error;这一行在内存里按 attempts 退避。
func (r *Relay) markFailed(ctx context.Context, conn *pgx.Conn, batch []row, failed map[int64]string) error {
	if len(failed) == 0 {
		return nil
	}
	for _, x := range batch {
		msg, bad := failed[x.seq]
		if !bad {
			continue
		}
		var attempts int
		if err := conn.QueryRow(ctx, `UPDATE factory_outbox SET attempts = attempts + 1, last_error = $2 WHERE seq = $1 RETURNING attempts`,
			x.seq, msg).Scan(&attempts); err != nil {
			return err
		}
		d := time.Second
		for i := 1; i < attempts && d < r.MaxBackoff; i++ {
			d *= 2
		}
		r.skip[x.seq] = time.Now().Add(min(d, r.MaxBackoff))
	}
	return nil
}

// backlog: 积压的指标(行数、最老的年龄)。
func (r *Relay) backlog(ctx context.Context, conn *pgx.Conn) error {
	var n int64
	var age float64
	if err := conn.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(occurred_at)), 0)::float8
FROM factory_outbox WHERE published_at IS NULL`).Scan(&n, &age); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	metrics.OutboxBacklog(n, time.Duration(age*float64(time.Second)))
	return nil
}

func nextBackoff(cur, maxB time.Duration) time.Duration {
	if cur <= 0 {
		return time.Second
	}
	return min(cur*2, maxB)
}

// Execer: Cleanup 用到的连接(*pgx.Conn / *pgxpool.Pool / pgx.Tx)。
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Cleanup: 删除发送超过 retention 的 outbox 行、处理超过 dedupRetention 的去重记录(dedupRetention ≤ 0 → 不删去重记录)。
// 每条 DELETE ≤ 1000 行,每种最多 20 次(一次清理不会长时间占着连接;剩下的下次再删)。中继的工作者每小时调用一次。
func Cleanup(ctx context.Context, db Execer, retention, dedupRetention time.Duration) error {
	for i := 0; i < 20; i++ {
		tag, err := db.Exec(ctx, `DELETE FROM factory_outbox WHERE seq IN (
SELECT seq FROM factory_outbox WHERE published_at < now() - $1::interval ORDER BY published_at LIMIT 1000)`, retention)
		if err != nil {
			return fmt.Errorf("outbox cleanup: %w", err)
		}
		if tag.RowsAffected() < 1000 {
			break
		}
	}
	if dedupRetention <= 0 {
		return nil
	}
	for i := 0; i < 20; i++ {
		tag, err := db.Exec(ctx, `DELETE FROM factory_processed_messages WHERE (handler, message_id) IN (
SELECT handler, message_id FROM factory_processed_messages WHERE processed_at < now() - $1::interval LIMIT 1000)`, dedupRetention)
		if err != nil {
			return fmt.Errorf("dedup cleanup: %w", err)
		}
		if tag.RowsAffected() < 1000 {
			break
		}
	}
	return nil
}
