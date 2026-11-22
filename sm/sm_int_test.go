package sm_test

// 集成测试: 真实的 Postgres(RUNTIME_TEST_DB),没有设置时跳过。一个最小的"生成的"状态机: 等待状态 1(超时 30s 重发,
// 之后每 60s;期限 300s → 状态 2),终态 3。Fire 照生成代码的写法: 读行 → 判定 → UPDATE … WHERE 版本 = 读到的。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/MIERAI/api-factory-runtime/internal/itest"
	"github.com/MIERAI/api-factory-runtime/metrics"
	"github.com/MIERAI/api-factory-runtime/sm"
	"github.com/MIERAI/api-factory-runtime/txn"
)

const schema = `CREATE TABLE jobs (id bigint PRIMARY KEY, status int NOT NULL, version int NOT NULL DEFAULT 1,
  attempts int NOT NULL DEFAULT 0, entered timestamptz NOT NULL);
CREATE TABLE sent (id bigserial PRIMARY KEY, job bigint NOT NULL, kind text NOT NULL)`

var t0 = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func scanner(e *txn.Executor) *sm.Scanner {
	return &sm.Scanner{
		Machine: "Jobs",
		Exec:    e,
		Due: func(ctx context.Context, q sm.Querier, now time.Time, limit int) ([]any, error) {
			rows, err := q.Query(ctx, `SELECT id FROM jobs WHERE status = 1 AND (entered + make_interval(secs => 30 + 60 * attempts) <= $1
  OR entered + interval '300 seconds' <= $1) ORDER BY entered, id LIMIT $2`, now, limit)
			if err != nil {
				return nil, err
			}
			ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
			out := make([]any, len(ids))
			for i, id := range ids {
				out[i] = id
			}
			return out, err
		},
		Fire: func(ctx context.Context, tx pgx.Tx, key any, now time.Time) (sm.Fired, error) {
			var st, ver, att int32
			var entered time.Time
			if err := tx.QueryRow(ctx, `SELECT status, version, attempts, entered FROM jobs WHERE id = $1`, key).Scan(&st, &ver, &att, &entered); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return sm.Fired{}, nil
				}
				return sm.Fired{}, err
			}
			if st != 1 {
				return sm.Fired{}, nil
			}
			kind, sets, to := sm.None, "", "WAIT"
			switch {
			case !entered.Add(300 * time.Second).After(now):
				kind, sets, to = sm.Deadline, "status = 2, attempts = 0, entered = $3", "COMPENSATE"
			case !entered.Add(time.Duration(30+60*att) * time.Second).After(now):
				kind, sets = sm.Retry, "attempts = attempts + 1"
			default:
				return sm.Fired{}, nil
			}
			args := []any{key, ver}
			if kind == sm.Deadline {
				args = append(args, now)
			}
			tag, err := tx.Exec(ctx, `UPDATE jobs SET `+sets+`, version = version + 1 WHERE id = $1 AND version = $2`, args...)
			if err != nil {
				return sm.Fired{}, err
			}
			if tag.RowsAffected() == 0 {
				return sm.Fired{Kind: sm.Conflict}, nil
			}
			if err := txn.AfterWrite(ctx, "jobs"); err != nil {
				return sm.Fired{}, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO sent (job, kind) VALUES ($1, $2)`, key, kind.String()); err != nil {
				return sm.Fired{}, err
			}
			if err := txn.AfterWrite(ctx, "sent"); err != nil {
				return sm.Fired{}, err
			}
			return sm.Fired{Kind: kind, From: "WAIT", To: to}, nil
		},
		Stats: func(ctx context.Context, q sm.Querier) ([]sm.StateStat, error) {
			rows, err := q.Query(ctx, `SELECT 'WAIT', count(*), coalesce(min(entered), now()) FROM jobs WHERE status = 1`)
			if err != nil {
				return nil, err
			}
			return pgx.CollectRows(rows, func(r pgx.CollectableRow) (sm.StateStat, error) {
				var s sm.StateStat
				return s, r.Scan(&s.State, &s.Count, &s.Oldest)
			})
		},
	}
}

func row(t *testing.T, ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id int64) (st, att int32) {
	t.Helper()
	if err := q.QueryRow(ctx, `SELECT status, attempts FROM jobs WHERE id = $1`, id).Scan(&st, &att); err != nil {
		t.Fatal(err)
	}
	return
}

// 重发的时刻: 进入之后 30s(正好 30s 到期)、之后每 60s;期限 300s → 补偿状态;版本冲突 → 重来;故障注入 → 回滚。
func TestRound(t *testing.T) {
	pool := itest.Pool(t, itest.NewDB(t))
	ctx := context.Background()
	if _, err := pool.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO jobs (id, status, entered) VALUES (1, 1, $1), (2, 1, $2), (3, 3, $1)`, t0, t0.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	s := scanner(txn.New(pool))
	q := poolQ{pool}
	steps := []struct {
		at                 time.Duration
		retried, deadlines int
	}{
		{29*time.Second + 999999*time.Microsecond, 0, 0}, // 刚好未到
		{30 * time.Second, 1, 0},                         // 1 号: 刚好到(30s)
		{40 * time.Second, 1, 0},                         // 2 号: 30s
		{89 * time.Second, 0, 0},                         // 1 号的第 2 次: 30 + 60 = 90s
		{90 * time.Second, 1, 0},
		{100 * time.Second, 1, 0}, // 2 号的第 2 次
		{150 * time.Second, 1, 0}, // 1 号的第 3 次: 30 + 120
		{160 * time.Second, 1, 0},
		{210 * time.Second, 1, 0},
		{220 * time.Second, 1, 0},
		{270 * time.Second, 1, 0},
		{280 * time.Second, 1, 0},
		{299 * time.Second, 0, 0},
		{300 * time.Second, 0, 1}, // 1 号: 期限(300s)
		{310 * time.Second, 0, 1}, // 2 号: 期限
		{900 * time.Second, 0, 0}, // 都不在等待状态了
	}
	for _, st := range steps {
		res, err := s.Round(ctx, q, t0.Add(st.at))
		if err != nil {
			t.Fatal(err)
		}
		if res.Retried != st.retried || res.Deadlines != st.deadlines || len(res.Errors) > 0 {
			t.Fatalf("at %v: %+v, want retried %d deadlines %d", st.at, res, st.retried, st.deadlines)
		}
	}
	if s, a := row(t, ctx, pool, 1); s != 2 || a != 0 {
		t.Fatalf("job 1: status %d attempts %d", s, a)
	}
	var sent int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM sent`).Scan(&sent)
	if sent != 5+5+2 {
		t.Fatalf("sent %d", sent)
	}
	if n := metrics.MachineTransitions("Jobs", "WAIT", "COMPENSATE", "deadline"); n != 2 {
		t.Fatalf("deadline metric %d", n)
	}
	var buf bytes.Buffer
	metrics.WriteMachines(&buf)
	if !strings.Contains(buf.String(), `factory_sm_instances{machine="Jobs",state="WAIT"} 0`) {
		t.Fatalf("metrics:\n%s", buf.String())
	}

	// 故障注入: 第 1 个写之后失败 → 回滚(重发计数不变、没有发出);错误记在结果里
	if _, err := pool.Exec(ctx, `INSERT INTO jobs (id, status, entered) VALUES (4, 1, $1)`, t0); err != nil {
		t.Fatal(err)
	}
	res, err := s.Round(txn.WithFault(ctx, 1), q, t0.Add(31*time.Second))
	if err != nil || len(res.Errors) != 1 {
		t.Fatalf("fault: %+v %v", res, err)
	}
	if _, a := row(t, ctx, pool, 4); a != 0 {
		t.Fatalf("fault: attempts %d", a)
	}
	// 并发: 两个扫描同时跑同一轮 → 只重发一次(版本冲突的一方重新读到"还没到期")
	var wg sync.WaitGroup
	results := make([]sm.RoundResult, 2)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], _ = s.Round(ctx, q, t0.Add(31*time.Second))
		}()
	}
	wg.Wait()
	if results[0].Retried+results[1].Retried != 1 {
		t.Fatalf("concurrent rounds: %+v", results)
	}
	if _, a := row(t, ctx, pool, 4); a != 1 {
		t.Fatalf("concurrent: attempts %d", a)
	}
}

// TestHandler: 生产环境 404;测试环境跑一轮(now 由请求给)。
func TestTestHandler(t *testing.T) {
	pool := itest.Pool(t, itest.NewDB(t))
	ctx := context.Background()
	if _, err := pool.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO jobs (id, status, entered) VALUES (1, 1, $1)`, t0); err != nil {
		t.Fatal(err)
	}
	h := sm.TestHandler(pool, scanner(txn.New(pool)))
	call := func(body string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, sm.TestPath, strings.NewReader(body)))
		return rr
	}
	t.Setenv(txn.TestFaultsEnv, "")
	if rr := call(`{"machine":"Jobs","now":"2026-11-01T00:01:00Z"}`); rr.Code != http.StatusNotFound {
		t.Fatalf("production: %d", rr.Code)
	}
	t.Setenv(txn.TestFaultsEnv, "1")
	rr := call(`{"machine":"Jobs","now":"2026-11-01T00:00:30Z","headers":{"x-factory-fail-after":"1"}}`)
	var res sm.RoundResult
	if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil || len(res.Errors) != 1 || res.Retried != 0 {
		t.Fatalf("fault: %s", rr.Body)
	}
	rr = call(`{"machine":"Jobs","now":"2026-11-01T00:00:30Z"}`)
	if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil || res.Retried != 1 {
		t.Fatalf("round: %s", rr.Body)
	}
	if rr := call(`{"machine":"Nope","now":"2026-11-01T00:00:30Z"}`); rr.Code != http.StatusNotFound {
		t.Fatalf("unknown machine: %d", rr.Code)
	}
}

type poolQ struct {
	p interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	}
}

func (q poolQ) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return q.p.Query(ctx, sql, args...)
}
