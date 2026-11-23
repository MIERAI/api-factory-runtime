package outbox

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/MIERAI/api-factory-runtime/txn"
)

// 唤醒中继(v0.5.2): NOTIFY 不在写事务里发,而是在提交之后、在事务外面发,并且按连接池合并。
//
// 为什么: 发过 NOTIFY 的事务在提交时要拿整个实例共用的一把锁(pg_database 上的 AccessExclusiveLock,保证通知
// 按提交顺序进队列),一直持有到提交记录的 WAL 落盘。写事务里带 NOTIFY = 所有写 outbox 的提交在每个 Postgres
// 实例上排成一队(每个提交一次 fsync),吞吐的上限和副本数无关(storefront v2 #21: 每秒约 45 单的拐点)。
//
// 现在: Writer.Write 用 txn.AfterCommit 登记"提交之后唤醒";提交成功后 kick(db) 把这个连接池标成"有新消息"。
// 每个连接池同时最多一个后台的 goroutine 在发 NOTIFY(自动提交的单条语句,没有别的 WAL → 提交不等 fsync,
// 持锁很短);发的时候又来的 kick 合并成下一次 NOTIFY。所以高负载下每个进程只是连续地发 NOTIFY(一次一条),
// 和提交的次数无关;低负载下每次提交之后马上一条,中继的延迟和以前一样。
//
// NOTIFY 只是唤醒: 发送失败 / 进程在提交之后、唤醒之前崩了 → 中继按 Poll 轮询照样会发出去(至少一次的保证
// 只靠 outbox 表,不靠通知)。
type wakeup struct {
	db      txn.Beginner
	mu      sync.Mutex
	pending bool // 有还没有被某次 NOTIFY 覆盖的提交
	running bool // 有 goroutine 在发
}

var wakeups sync.Map // txn.Beginner → *wakeup

// notifyTimeout: 一次 NOTIFY 的上限(数据库卡住时不让发送的 goroutine 一直挂着;丢了靠轮询)。
const notifyTimeout = 5 * time.Second

// kick: 这个连接池上有事务提交了 outbox 的行: 尽快发一次 NOTIFY(合并)。不阻塞。
func kick(db txn.Beginner) {
	v, ok := wakeups.Load(db)
	if !ok {
		v, _ = wakeups.LoadOrStore(db, &wakeup{db: db})
	}
	w := v.(*wakeup)
	w.mu.Lock()
	w.pending = true
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()
	go w.loop()
}

func (w *wakeup) loop() {
	for {
		w.mu.Lock()
		if !w.pending {
			w.running = false
			w.mu.Unlock()
			return
		}
		w.pending = false
		w.mu.Unlock()
		if err := notify(w.db); err != nil {
			slog.Debug("outbox: wake-up NOTIFY failed (the relay's poll will pick the messages up)", "err", err)
		}
	}
}

// notify: 事务外的一条 NOTIFY(*pgxpool.Pool / *pgx.Conn 直接 Exec;别的 Beginner 用一个只有 NOTIFY 的事务)。
func notify(db txn.Beginner) error {
	ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
	defer cancel()
	const q = `SELECT pg_notify('` + NotifyChannel + `', '')`
	if ex, ok := db.(Execer); ok {
		_, err := ex.Exec(ctx, q)
		return err
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, q); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
