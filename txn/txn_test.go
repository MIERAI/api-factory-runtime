package txn

import (
	"context"
	"errors"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestBuild(t *testing.T) {
	cases := []struct {
		name string
		op   Op
		sql  string
		args []any
	}{
		{"insert", Op{Kind: Insert, Table: "products", Values: []Col{{Name: "sku", Value: "A-1"}, {Name: "price", Value: "10"}}, Returning: []string{"sku", "version"}},
			`INSERT INTO "products" ("sku", "price") VALUES ($1, $2) RETURNING "sku", "version"`, []any{"A-1", "10"}},
		{"insert defaults", Op{Kind: Insert, Table: "t"}, `INSERT INTO "t" DEFAULT VALUES RETURNING 1`, nil},
		{"update with version", Op{Kind: Update, Table: "products", Values: []Col{{Name: "price", Value: "12"}, Incr1("version")},
			Key: []Col{{Name: "sku", Value: "A-1"}}, Version: &Col{Name: "version", Value: int32(3)}, Returning: []string{"price", "version"}},
			`UPDATE "products" SET "price" = $1, "version" = "version" + 1 WHERE "sku" = $2 AND "version" = $3 RETURNING "price", "version"`,
			[]any{"12", "A-1", int32(3)}},
		{"update without version", Op{Kind: Update, Table: "jobs", Values: []Col{{Name: "status", Value: int32(1)}}, Key: []Col{{Name: "id", Value: int64(7)}}},
			`UPDATE "jobs" SET "status" = $1 WHERE "id" = $2 RETURNING 1`, []any{int32(1), int64(7)}},
		{"delete", Op{Kind: Delete, Table: "jobs", Key: []Col{{Name: "id", Value: int64(7)}}, Version: &Col{Name: "version", Value: int32(2)}, Returning: []string{"id"}},
			`DELETE FROM "jobs" WHERE "id" = $1 AND "version" = $2 RETURNING "id"`, []any{int64(7), int32(2)}},
		{"quoted identifiers", Op{Kind: Delete, Table: `we"ird`, Key: []Col{{Name: "a b", Value: 1}}},
			`DELETE FROM "we""ird" WHERE "a b" = $1 RETURNING 1`, []any{1}},
	}
	for _, c := range cases {
		sql, args, err := Build(c.op)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if sql != c.sql || !reflect.DeepEqual(args, c.args) {
			t.Errorf("%s:\n got %s %v\nwant %s %v", c.name, sql, args, c.sql, c.args)
		}
	}
	bad := []Op{
		{Kind: Update, Table: "t", Values: []Col{{Name: "a", Value: 1}}}, // 没有键
		{Kind: Update, Table: "t", Key: []Col{{Name: "id", Value: 1}}},   // 没有要改的列
		{Kind: Delete, Table: "t"},                                       // 没有键
		{Kind: Insert, Table: "t", Key: []Col{{Name: "id", Value: 1}}},   // insert 带键
		{Kind: Insert, Table: "t", Values: []Col{Incr1("version")}},      // insert 带 Incr
		{Kind: Insert},              // 没有表
		{Kind: Kind(9), Table: "t"}, // 不认识的种类
	}
	for i, op := range bad {
		if _, _, err := Build(op); err == nil {
			t.Errorf("bad op #%d: want an error", i)
		}
	}
}

func TestClassify(t *testing.T) {
	op := Op{Kind: Insert, Table: "products"}
	cases := map[string]Reason{"23505": Unique, "23514": Check, "22003": Check, "23502": NotNull, "23503": ForeignKey}
	for code, want := range cases {
		err := classify(0, op, &pgconn.PgError{Code: code, ConstraintName: "c"})
		if got := ReasonOf(err); got != want {
			t.Errorf("%s: got %s, want %s", code, got, want)
		}
		if !strings.Contains(err.Error(), "(c)") {
			t.Errorf("%s: message should name the constraint: %v", code, err)
		}
	}
	for _, code := range []string{"40001", "40P01", "XX000"} { // 重试 / 平台的错误: 不是写操作的原因
		if got := ReasonOf(classify(0, op, &pgconn.PgError{Code: code})); got != None {
			t.Errorf("%s: got %s, want none", code, got)
		}
	}
	if !retryable(&pgconn.PgError{Code: "40001"}) || !retryable(&pgconn.PgError{Code: "40P01"}) || retryable(&pgconn.PgError{Code: "23505"}) {
		t.Error("retryable: serialization failure and deadlock only")
	}
	if ReasonOf(errors.New("x")) != None {
		t.Error("plain error: none")
	}
}

// D3: 生产环境(FACTORY_TEST_FAULTS 没有设置)请求头无效。
func TestFaultHeaderIgnoredInProduction(t *testing.T) {
	h := http.Header{}
	h.Set(FaultHeader, "1")
	if k, err := FaultFromHeader(h, false); k != 0 || err != nil {
		t.Fatalf("production must ignore %s: %d %v", FaultHeader, k, err)
	}
	if k, err := FaultFromHeader(h, true); k != 1 || err != nil {
		t.Fatalf("test faults: %d %v", k, err)
	}
	h.Set(FaultHeader, "0")
	if _, err := FaultFromHeader(h, true); err == nil {
		t.Fatal("k must be >= 1")
	}
	os.Unsetenv(TestFaultsEnv)
	called := false
	next := func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		called = true
		if faultFrom(ctx) != 0 {
			t.Error("production: the fault must not reach the executor")
		}
		return nil, nil
	}
	req := connect.NewRequest(&struct{}{})
	req.Header().Set(FaultHeader, "1")
	_, _ = NewInterceptor()(next)(context.Background(), req)
	if !called {
		t.Fatal("interceptor did not call next")
	}
}

func testDB(t *testing.T) string {
	url := os.Getenv("TXN_TEST_DB")
	if url == "" {
		url = os.Getenv("RUNTIME_TEST_DB")
	}
	if url == "" {
		t.Skip("TXN_TEST_DB / RUNTIME_TEST_DB not set")
	}
	return url
}

// 数据库上的执行(TXN_TEST_DB 或 RUNTIME_TEST_DB=postgres://… 时运行): 原因的判定、回滚、故障注入。
func TestExecutorOnDB(t *testing.T) {
	url := testDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	mustExec := func(sql string) {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(`DROP TABLE IF EXISTS txn_test`)
	mustExec(`CREATE TABLE txn_test (k text PRIMARY KEY, v int NOT NULL CHECK (v >= 0), version int NOT NULL DEFAULT 1)`)
	defer mustExec(`DROP TABLE txn_test`)
	e := New(conn)
	var v, ver int32
	ins := func(k string, val int32) Op {
		return Op{Kind: Insert, Table: "txn_test", Values: []Col{{Name: "k", Value: k}, {Name: "v", Value: val}}, Returning: []string{"v", "version"}, Into: []any{&v, &ver}}
	}
	upd := func(k string, val, version int32) Op {
		return Op{Kind: Update, Table: "txn_test", Values: []Col{{Name: "v", Value: val}, Incr1("version")}, Key: []Col{{Name: "k", Value: k}},
			Version: &Col{Name: "version", Value: version}, Returning: []string{"v", "version"}, Into: []any{&v, &ver}, NotFound: "no " + k}
	}
	if err := e.Run(ctx, ins("a", 5)); err != nil || v != 5 || ver != 1 {
		t.Fatalf("insert: %v %d %d", err, v, ver)
	}
	if got := ReasonOf(e.Run(ctx, ins("a", 6))); got != Unique {
		t.Errorf("duplicate key: %s", got)
	}
	if got := ReasonOf(e.Run(ctx, ins("b", -1))); got != Check {
		t.Errorf("check: %s", got)
	}
	if err := e.Run(ctx, upd("a", 7, 1)); err != nil || v != 7 || ver != 2 {
		t.Fatalf("update: %v %d %d", err, v, ver)
	}
	if got := ReasonOf(e.Run(ctx, upd("a", 8, 1))); got != Conflict {
		t.Errorf("stale version: %s", got)
	}
	err = e.Run(ctx, upd("zz", 8, 1))
	if got := ReasonOf(err); got != NotFound || !strings.Contains(err.Error(), "no zz") {
		t.Errorf("missing row: %s %v", got, err)
	}
	// 一个事务: 第二个操作失败 → 第一个也回滚
	if got := ReasonOf(e.Run(ctx, ins("c", 1), ins("a", 1))); got != Unique {
		t.Errorf("second op: %s", got)
	}
	// 故障注入: 第 1 个写操作之后失败 → 回滚
	if got := ReasonOf(e.Run(WithFault(ctx, 1), ins("d", 1))); got != Injected {
		t.Errorf("fault: %s", got)
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM txn_test WHERE k IN ('c', 'd')`).Scan(&n); err != nil || n != 0 {
		t.Errorf("rolled back rows are visible: %d %v", n, err)
	}
	del := Op{Kind: Delete, Table: "txn_test", Key: []Col{{Name: "k", Value: "a"}}, Version: &Col{Name: "version", Value: int32(2)}}
	if err := e.Run(ctx, del); err != nil {
		t.Errorf("delete: %v", err)
	}
	if got := ReasonOf(e.Run(ctx, del)); got != NotFound {
		t.Errorf("delete again: %s", got)
	}
}

// AfterWrite: 不经过 RunIn 的写入(outbox)也计入"第 k 个写操作之后失败";生产环境(没有 WithFault)总是 nil。
func TestAfterWriteWithoutDB(t *testing.T) {
	if err := AfterWrite(context.Background(), "t"); err != nil {
		t.Fatalf("no fault: %v", err)
	}
	// 不在 InTx 里: AfterWrite 不计数(没有事务的计数器)
	if err := AfterWrite(WithFault(context.Background(), 1), "t"); err != nil {
		t.Fatalf("outside InTx: %v", err)
	}
	st := &attemptState{}
	ctx := context.WithValue(WithFault(context.Background(), 2), stateKey{}, st)
	if err := AfterWrite(ctx, "a"); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if got := ReasonOf(AfterWrite(ctx, "factory_outbox")); got != Injected {
		t.Fatalf("second write: %s", got)
	}
}

// InTx / RunIn / Savepoint / AfterWrite 在数据库上: 保存点只回滚自己的写入;故障注入按整个事务计数;fn 的错误整个回滚。
func TestInTxOnDB(t *testing.T) {
	url := testDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `DROP TABLE IF EXISTS txn_intx; CREATE TABLE txn_intx (k text PRIMARY KEY, v int NOT NULL CHECK (v >= 0))`); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(ctx, `DROP TABLE txn_intx`)
	e := New(conn)
	ins := func(k string, v int32) Op {
		return Op{Kind: Insert, Table: "txn_intx", Values: []Col{{Name: "k", Value: k}, {Name: "v", Value: v}}}
	}
	count := func(where string) int {
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM txn_intx WHERE `+where).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	// 保存点: 第二个插入违反 CHECK → 只回滚保存点里的,外面的写入照样提交
	var spErr error
	err = e.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := RunIn(ctx, tx, ins("a", 1)); err != nil {
			return err
		}
		spErr = Savepoint(ctx, tx, func(ctx context.Context, tx pgx.Tx) error {
			return RunIn(ctx, tx, ins("b", 1), ins("c", -1))
		})
		return RunIn(ctx, tx, ins("d", 1))
	})
	if err != nil || ReasonOf(spErr) != Check {
		t.Fatalf("savepoint: %v / %v", err, spErr)
	}
	if count(`k IN ('a','d')`) != 2 || count(`k IN ('b','c')`) != 0 {
		t.Fatal("savepoint: wrong rows committed")
	}
	// fn 返回错误 → 整个回滚
	boom := errors.New("boom")
	if err := e.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_ = RunIn(ctx, tx, ins("e", 1))
		return boom
	}); !errors.Is(err, boom) || count(`k = 'e'`) != 0 {
		t.Fatalf("fn error: %v", err)
	}
	// 故障注入跨 RunIn / AfterWrite 计数: 第 3 个写 = AfterWrite
	err = e.InTx(WithFault(ctx, 3), func(ctx context.Context, tx pgx.Tx) error {
		if err := RunIn(ctx, tx, ins("f", 1)); err != nil {
			return err
		}
		if err := RunIn(ctx, tx, ins("g", 1)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO txn_intx VALUES ('h', 1)`); err != nil {
			return err
		}
		return AfterWrite(ctx, "txn_intx")
	})
	if ReasonOf(err) != Injected || count(`k IN ('f','g','h')`) != 0 {
		t.Fatalf("fault across the transaction: %v", err)
	}
	// 保存点里的序列化失败: fn 吞掉了错误也要整个重来
	calls := 0
	err = e.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		calls++
		_ = Savepoint(ctx, tx, func(ctx context.Context, tx pgx.Tx) error {
			if calls == 1 {
				_, err := tx.Exec(ctx, `DO $$ BEGIN RAISE EXCEPTION 'fake' USING ERRCODE = '40001'; END $$`)
				return err
			}
			return nil
		})
		return RunIn(ctx, tx, ins("i", int32(calls)))
	})
	var v int
	if err != nil || calls != 2 || conn.QueryRow(ctx, `SELECT v FROM txn_intx WHERE k = 'i'`).Scan(&v) != nil || v != 2 {
		t.Fatalf("retry after a serialization failure in a savepoint: err=%v calls=%d v=%d", err, calls, v)
	}
}
