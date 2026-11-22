// Package sm: 状态机(api-factory R5,docs/design/r5-state-machines.md)的运行时部分 —— 超时扫描和指标。
//
// 转移本身是生成的代码: 每个收到的消息类型一个 consume.Handler(一条条件更新 UPDATE … WHERE 状态 = 从 [AND 守卫],
// 不满足时按状态图分类成 忽略 / 拒绝)。这里只有每个状态机都一样的部分:
//
//	Scanner.Run       每个数据库一个在工作(leader: advisory lock "factory.sm.timeout.<机器>"),每 Every(默认 5s)一轮
//	Scanner.Round     一轮: Due(now, Batch) 找到到期的实体(生成的 SQL: 每个等待状态的 超时 / 期限)→ 每个实体一个事务
//	                  执行 Fire(生成的代码: 在事务里读行 → 判定 重发 / 期限 → UPDATE … WHERE 版本 = 读到的 → 写 outbox)
//	                  Fire 返回 Conflict(版本变了: 回答 / 别的扫描先改了这一行)→ 整个事务重来(重新读、重新判定),最多 3 次
//	TestHandler       测试环境的"跑一轮超时(now)"(POST /factory.test/timeouts,只在 FACTORY_TEST_FAULTS=1 时响应):
//	                  验收用测试时钟推进时间;FACTORY_TEST_CLOCK=1 时 Run 不自己跑(时间只由测试驱动)
//
// 超时的判断用传进来的 now(平台时钟),不用数据库的 now()。
package sm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MIERAI/api-factory-runtime/clock"
	"github.com/MIERAI/api-factory-runtime/leader"
	"github.com/MIERAI/api-factory-runtime/metrics"
	"github.com/MIERAI/api-factory-runtime/txn"
)

// Querier: Due / Stats 用的连接(持锁的会话 / 连接池)。
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Kind: 一个实体的超时处理的结果。
type Kind uint8

const (
	None     Kind = iota // 没有到期(扫描之后状态变了 / 还没到): 什么都不做
	Retry                // 重发: 同一个状态,重发计数 + 1,发出这个状态的命令
	Deadline             // 期限: 转移到补偿的状态(和普通的转移一样: 进入时间 = now,重发计数清零)
	Conflict             // 版本变了(并发): Round 让整个事务重来
)

func (k Kind) String() string {
	return [...]string{"none", "retry", "deadline", "conflict"}[k]
}

// Fired: Fire 的结果(From / To = 状态名;Retry 时 To = From)。
type Fired struct {
	Kind     Kind
	From, To string
}

// ErrConflict: 生成的 Fire 在"WHERE 版本 = 读到的"没有碰到行时返回(txn 的 Reason Conflict 也认)。
var ErrConflict = errors.New("sm: version conflict")

// MaxAttempts: 一个实体的超时事务因为版本冲突最多执行几次。
const MaxAttempts = 3

// StateStat: 一个等待状态的实体数和停留最久的那个的进入时间(指标)。
type StateStat struct {
	State  string
	Count  int64
	Oldest time.Time
}

// Scanner: 一个状态机的超时扫描(生成的 wiring 构造)。
type Scanner struct {
	Machine string
	Exec    *txn.Executor
	// Due: 到期的实体的键(按进入时间,最多 limit 个)。生成的 SQL
	Due func(ctx context.Context, q Querier, now time.Time, limit int) ([]any, error)
	// Fire: 一个实体: 在 tx 里读行、判定、写(生成的代码)
	Fire func(ctx context.Context, tx pgx.Tx, key any, now time.Time) (Fired, error)
	// Stats: 每个等待状态的实体数 / 最老的进入时间(可以为 nil)
	Stats func(ctx context.Context, q Querier) ([]StateStat, error)
	Every time.Duration // 默认 5s
	Batch int           // 默认 100
	Log   *slog.Logger
}

// RoundResult: 一轮的统计。
type RoundResult struct {
	Due       int      `json:"due"`
	Retried   int      `json:"retried"`
	Deadlines int      `json:"deadlines"`
	Skipped   int      `json:"skipped"`   // 读到的时候已经不到期(状态变了)
	Conflicts int      `json:"conflicts"` // 版本冲突的重来次数
	Backlog   int      `json:"backlog"`   // 这一轮满了 Batch 个(可能还有没处理的)
	Errors    []string `json:"errors,omitempty"`
}

func (s *Scanner) batch() int {
	if s.Batch <= 0 {
		return 100
	}
	return s.Batch
}

// Round: 一轮超时。q = 查询到期实体的连接。一个实体的失败不影响别的实体(错误记在结果里,下一轮再来)。
func (s *Scanner) Round(ctx context.Context, q Querier, now time.Time) (RoundResult, error) {
	var res RoundResult
	now = now.UTC().Truncate(time.Microsecond)
	keys, err := s.Due(ctx, q, now, s.batch())
	if err != nil {
		return res, err
	}
	res.Due = len(keys)
	if len(keys) >= s.batch() {
		res.Backlog = len(keys)
	}
	metrics.MachineBacklog(s.Machine, res.Backlog)
	for _, k := range keys {
		f, conflicts, err := s.fireOne(ctx, k, now)
		res.Conflicts += conflicts
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("%v: %v", k, err))
			continue
		}
		switch f.Kind {
		case Retry:
			res.Retried++
			metrics.MachineTransition(s.Machine, f.From, f.To, "timeout")
		case Deadline:
			res.Deadlines++
			metrics.MachineTransition(s.Machine, f.From, f.To, "deadline")
		default:
			res.Skipped++
		}
	}
	if s.Stats != nil {
		st, err := s.Stats(ctx, q)
		if err != nil {
			return res, err
		}
		for _, x := range st {
			age := 0.0
			if x.Count > 0 {
				age = max(now.Sub(x.Oldest).Seconds(), 0)
			}
			metrics.MachineState(s.Machine, x.State, x.Count, age)
		}
	}
	return res, nil
}

// fireOne: 一个实体的事务;版本冲突 → 重来(最多 MaxAttempts 次;仍冲突 = 这一轮放过,下一轮再判定)。
func (s *Scanner) fireOne(ctx context.Context, key any, now time.Time) (Fired, int, error) {
	conflicts := 0
	for attempt := 1; attempt <= MaxAttempts; attempt++ {
		var f Fired
		err := s.Exec.InTx(clock.With(ctx, now), func(ctx context.Context, tx pgx.Tx) error {
			var err error
			f, err = s.Fire(ctx, tx, key, now)
			if err == nil && f.Kind == Conflict {
				err = ErrConflict
			}
			return err
		})
		if err == nil {
			return f, conflicts, nil
		}
		if !errors.Is(err, ErrConflict) && txn.ReasonOf(err) != txn.Conflict {
			return f, conflicts, err
		}
		conflicts++
	}
	return Fired{Kind: None}, conflicts, nil
}

// TestClock: FACTORY_TEST_CLOCK=1(测试环境: 时间只由测试驱动,Run 不自己扫描)。
func TestClock() bool { return os.Getenv(clock.TestClockEnv) == "1" }

// Run: 后台任务(server.Deps.Workers)。每个数据库一个在工作;测试时钟打开时什么都不做(TestHandler 驱动)。
func (s *Scanner) Run(ctx context.Context, pool *pgxpool.Pool) error {
	if TestClock() {
		<-ctx.Done()
		return nil
	}
	log := s.Log
	if log == nil {
		log = slog.Default()
	}
	every := s.Every
	if every <= 0 {
		every = 5 * time.Second
	}
	return leader.Run(ctx, pool, "factory.sm.timeout."+s.Machine, leader.Options{Log: log}, func(ctx context.Context, conn *pgx.Conn) error {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			res, err := s.Round(ctx, conn, time.Now())
			if err != nil {
				return err // 持锁的连接坏了: 交给 leader 重新竞选
			}
			for _, e := range res.Errors {
				log.Warn("sm: timeout failed", "machine", s.Machine, "err", e)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
			}
		}
	})
}

// TestPath: 测试环境的"跑一轮超时"。
const TestPath = "/factory.test/timeouts"

// TestRequest: {"machine": "OrderSaga", "now": "2026-11-22T01:00:00Z", "headers": {"x-factory-fail-after": "2"}}
type TestRequest struct {
	Machine string            `json:"machine"`
	Now     string            `json:"now"`
	Headers map[string]string `json:"headers,omitempty"`
}

// TestHandler: 测试环境(FACTORY_TEST_FAULTS=1)的进程内入口: 跑一轮超时,返回 RoundResult。生产环境 404。
func TestHandler(pool *pgxpool.Pool, scanners ...*Scanner) http.Handler {
	by := map[string]*Scanner{}
	for _, s := range scanners {
		by[s.Machine] = s
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !txn.TestFaultsEnabled() {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req TestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		s := by[req.Machine]
		if s == nil {
			http.Error(w, fmt.Sprintf("no machine %q", req.Machine), http.StatusNotFound)
			return
		}
		now, err := time.Parse(time.RFC3339Nano, req.Now)
		if err != nil {
			http.Error(w, "now: "+err.Error(), http.StatusBadRequest)
			return
		}
		h := http.Header{}
		for k, v := range req.Headers {
			h.Set(k, v)
		}
		ctx := r.Context()
		k, err := txn.FaultFromHeader(h, true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if k > 0 {
			ctx = txn.WithFault(ctx, k)
		}
		res, err := s.Round(ctx, poolQuerier{pool}, now)
		if err != nil {
			res.Errors = append(res.Errors, "round: "+err.Error())
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(res)
	})
}

type poolQuerier struct{ p *pgxpool.Pool }

func (q poolQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return q.p.Query(ctx, sql, args...)
}

// Ignored / Rejected / Transition / Completed: 生成的转移处理器调用的指标(在处理器的事务里调用: 事务重来时可能多计一次)。
func Ignored(machine, reason string)  { metrics.MachineIgnored(machine, reason) }
func Rejected(machine, reason string) { metrics.MachineRejected(machine, reason) }
func Transition(machine, from, to, trigger string) {
	metrics.MachineTransition(machine, from, to, trigger)
}

// Completed: 进入终态(created = 实体的创建时刻):"创建到终态的耗时"直方图。
func Completed(machine, state string, created, now time.Time) {
	metrics.MachineCompleted(machine, state, now.Sub(created))
}
