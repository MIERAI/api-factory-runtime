// Package txn 是命令(写入)的事务执行器: 生成的 handler 把写操作作为数据(Op)交给这里,
// 在一个事务里按顺序执行,失败时整个回滚,把失败分成固定的几种原因(Reason)。
//
// 分工(api-factory 的命令工厂,R3):
//   - 这里: 事务(开始 / 提交 / 回滚)、SQL 的组装(标识符加引号,值一律是参数)、"没有更新"的原因判定
//     (同一个事务里再读一次)、数据库错误的分类、序列化失败 / 死锁的有限重试、测试用的故障注入钩子
//   - 生成的代码: 原因 → 错误码(每个服务的 commands_gen.go;改错误码的决定只改生成器,不用发运行时)
//
// handler 里不写 SQL,也不碰连接: 生成代码只依赖这个包的 Op / Run / ReasonOf。
package txn

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/MIERAI/api-factory-runtime/anomaly"
)

// Kind: 写操作的种类。
type Kind uint8

const (
	Insert Kind = iota + 1 // INSERT … RETURNING
	Update                 // UPDATE … WHERE 主键 [AND 版本] RETURNING
	Delete                 // DELETE … WHERE 主键 [AND 版本] RETURNING
)

func (k Kind) String() string {
	switch k {
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	}
	return "kind(" + strconv.Itoa(int(k)) + ")"
}

// Col: 列 = 值。Incr 不为 0 时是 "列 = 列 + Incr"(乐观锁的版本号加 1),Value 不用。
type Col struct {
	Name  string
	Value any
	Incr  int64
}

// Incr1: 版本列加 1(update 的 SET 里写;生成代码里看得见,改错了变异测试能抓到)。
func Incr1(name string) Col { return Col{Name: name, Incr: 1} }

// Op: 一个写操作(数据)。
type Op struct {
	Kind  Kind
	Table string
	// Values: insert = 插入的列;update = SET 的列(包括 Incr1 的版本列)。delete 不用
	Values []Col
	// Key: update / delete 的主键(WHERE 主键 = …)。insert 不用
	Key []Col
	// Version: 乐观锁(update / delete): WHERE … AND 版本列 = 请求带的版本号。nil = 不检查版本
	Version *Col
	// Returning: RETURNING 的列;Into: 扫描的目标(和 Returning 一一对应,指针)
	Returning []string
	Into      []any
	// NotFound: 行不存在时的消息(给调用方看的)
	NotFound string
	// R3-2 的扩展点: Cond(条件更新: WHERE 里另外的谓词 + 不满足时的业务原因码)、
	// Idempotent(幂等新建: ON CONFLICT (键) DO NOTHING + 读出已有的行)加在这里,原因判定在 explain 里加一种
}

// Reason: 写操作失败的原因(生成代码按它决定错误码)。
type Reason uint8

const (
	None       Reason = iota
	NotFound          // update / delete: 行不存在
	Conflict          // update / delete: 行存在,版本不一致(乐观锁)
	Unique            // 唯一约束冲突(23505)
	Check             // CHECK 约束(23514)/ 值不合法(22xxx: 溢出、格式)
	NotNull           // NOT NULL(23502)
	ForeignKey        // 外键(23503)
	Retry             // 序列化失败 / 死锁(40001 / 40P01),重试 MaxAttempts 次之后仍然失败
	Injected          // 测试用的故障注入(FACTORY_TEST_FAULTS=1 + 请求头 X-Factory-Fail-After)
)

func (r Reason) String() string {
	return [...]string{"none", "not_found", "conflict", "unique", "check", "not_null", "foreign_key", "retry", "injected"}[r]
}

// Error: 写操作失败。Op = 第几个操作(从 0 开始),Constraint = 数据库报的约束名(有的话)。
type Error struct {
	Reason     Reason
	Op         int
	Table      string
	Constraint string
	Msg        string // 给调用方看的消息(不含 SQL 和值)
	Err        error  // 原始的数据库错误(日志用)
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Msg + ": " + e.Err.Error()
	}
	return e.Msg
}

func (e *Error) Unwrap() error { return e.Err }

// ReasonOf: err 的原因(不是写操作的错误 → None: 连接断开、超时等,交给平台的拦截器)。
func ReasonOf(err error) Reason {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return None
}

// MaxAttempts: 序列化失败 / 死锁时最多执行几次(第一次 + 重试)。
const MaxAttempts = 3

// Beginner: 能开始事务的连接(*pgxpool.Pool / *pgx.Conn)。
type Beginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// Executor: 事务执行器。生成的服务每个进程一个(wiring: txn.New(deps.Pool))。
type Executor struct {
	db Beginner
	// ⚖️ D2(命令工厂设计稿 §3.4,待负责人确认): 隔离级别 READ COMMITTED(Postgres 的默认)。
	// 乐观锁和"没有更新"的判定都靠单条 UPDATE / DELETE 的原子性,不需要更高的级别。
	// 组合命令(R3-2)需要时在这里按操作选 REPEATABLE READ
	iso pgx.TxIsoLevel
}

func New(db Beginner) *Executor { return &Executor{db: db, iso: pgx.ReadCommitted} }

// Run: 在一个事务里按顺序执行 ops。任何一个失败 → 回滚,返回 *Error(或者连接等平台的错误)。
// 序列化失败 / 死锁 → 整个事务重来(最多 MaxAttempts 次);Into 每次都会被重新写入。
func (e *Executor) Run(ctx context.Context, ops ...Op) error {
	return e.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error { return RunIn(ctx, tx, ops...) })
}

// InTx: 在一个事务里执行 fn,fn 返回 nil 就提交。给"不只是一串 Op"的写入用(R4: 消息处理器的去重 + 操作 + outbox,
// 写操作之后按返回值组装消息再写 outbox)。fn 里的写操作用 RunIn / Savepoint / outbox.Write,都在 tx 上。
//
//   - fn 返回错误 → 回滚,原样返回这个错误
//   - 序列化失败 / 死锁(fn 里或提交时)→ 整个事务重来,fn 会被再调用(最多 MaxAttempts 次),所以 fn 除了 tx 以外不能有副作用
//   - 测试的故障注入(WithFault: 第 k 个写操作之后失败)按整个事务计数: RunIn 的每个操作和 AfterWrite 各算一个
func (e *Executor) InTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	var err error
	for attempt := 1; attempt <= MaxAttempts; attempt++ {
		err = e.once(ctx, fn)
		if !retryable(err) {
			return err
		}
	}
	var pe *pgconn.PgError
	errors.As(err, &pe)
	return &Error{Reason: Retry, Op: -1, Msg: "concurrent update: retry the request", Err: err, Constraint: constraintOf(pe)}
}

func (e *Executor) once(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) (err error) {
	tx, err := e.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: e.iso})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	st := &attemptState{} // 每次尝试从头开始: 故障注入的计数、保存点里遇到的序列化失败
	ctx = context.WithValue(ctx, stateKey{}, st)
	ferr := fn(ctx, tx)
	if st.retry != nil { // 保存点里遇到了序列化失败 / 死锁: 不管 fn 怎么处理了那个错误,整个事务重来
		return st.retry
	}
	if ferr != nil {
		return ferr
	}
	if err := tx.Commit(ctx); err != nil { // 提交时的错误(序列化失败 → 重试;延迟检查的约束 → 原因)
		return classify(-1, Op{Table: "(commit)"}, err)
	}
	return nil
}

// RunIn: 在已经开始的事务 tx 里按顺序执行 ops(InTx 的 fn 里用)。失败时不回滚 tx: 交给 InTx(返回错误 = 整个回滚)
// 或者 Savepoint(只回滚到保存点)。错误和 Run 一样(*Error 的 Op 是 ops 里的下标)。
func RunIn(ctx context.Context, tx pgx.Tx, ops ...Op) error {
	for i, op := range ops {
		if err := exec(ctx, tx, i, op); err != nil {
			return err
		}
		if err := afterWrite(ctx, i, op.Table); err != nil {
			return err
		}
	}
	return nil
}

// AfterWrite: 不经过 RunIn 的写入(例: outbox.Write 的每一行)也计入故障注入的写操作个数。
// 测试设置了"第 k 个写操作之后失败"且这是第 k 个时返回 Reason = Injected 的错误;否则 nil。生产环境总是 nil。
func AfterWrite(ctx context.Context, table string) error { return afterWrite(ctx, -1, table) }

func afterWrite(ctx context.Context, i int, table string) error {
	failAfter := faultFrom(ctx)
	if failAfter <= 0 {
		return nil
	}
	var n *int
	if st, _ := ctx.Value(stateKey{}).(*attemptState); st != nil {
		n = &st.writes
	}
	if n == nil { // 不在 InTx 里(直接对一个 tx 调用 RunIn): 按这一次调用的下标数
		n = new(int)
		*n = i
		if i < 0 {
			return nil
		}
	}
	*n++
	if *n == failAfter { // 测试用: 第 k 个写操作之后强制失败(之前的写入必须被回滚)
		return &Error{Reason: Injected, Op: i, Table: table, Msg: fmt.Sprintf("injected fault after write %d", failAfter)}
	}
	return nil
}

type stateKey struct{}

type attemptState struct {
	writes int   // 这次尝试里做过的写操作(故障注入)
	retry  error // 保存点里遇到的序列化失败 / 死锁
}

// Savepoint: 在 tx 里开一个保存点执行 fn。fn 返回错误 → 只回滚到保存点(之前的写入和 tx 都还在),返回 fn 的错误;
// 否则释放保存点。R4 §4.2: 业务结果(条件不满足)要回滚已经做了的操作,但去重记录和结果消息照样提交。
// 序列化失败 / 死锁不是业务结果: 在 InTx 里时记下来,InTx 不管 fn 怎么处理了这个错误都会让整个事务重来。
func Savepoint(ctx context.Context, tx pgx.Tx, fn func(ctx context.Context, tx pgx.Tx) error) error {
	sp, err := tx.Begin(ctx) // pgx: 事务里的 Begin = SAVEPOINT
	if err != nil {
		return err
	}
	if err := fn(ctx, sp); err != nil {
		if st, _ := ctx.Value(stateKey{}).(*attemptState); st != nil && retryable(err) && st.retry == nil {
			st.retry = err
		}
		if rerr := sp.Rollback(ctx); rerr != nil {
			return errors.Join(err, rerr)
		}
		return err
	}
	return sp.Commit(ctx) // RELEASE SAVEPOINT
}

// exec: 一个操作。RETURNING 0 行(update / delete)→ 同一个事务里再读一次,分出 NotFound / Conflict。
func exec(ctx context.Context, tx pgx.Tx, i int, op Op) error {
	sql, args, err := Build(op)
	if err != nil {
		return err
	}
	if len(op.Into) != len(op.Returning) {
		return fmt.Errorf("txn: op %d (%s %s): %d returning columns but %d scan targets", i, op.Kind, op.Table, len(op.Returning), len(op.Into))
	}
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return classify(i, op, err)
	}
	n := 0
	for rows.Next() {
		n++
		if n == 1 && len(op.Into) > 0 {
			if err := rows.Scan(op.Into...); err != nil {
				rows.Close()
				return err
			}
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return classify(i, op, err)
	}
	switch {
	case n == 1:
		return nil
	case n > 1: // 主键 = 一行;多于一行说明 Key 不是主键(生成器的 bug)
		return fmt.Errorf("txn: op %d (%s %s) touched %d rows, want 1 (key is not the primary key?)", i, op.Kind, op.Table, n)
	case op.Kind == Insert: // INSERT 没有返回行: 不会发生(R3-2 的幂等新建在这里分支)
		return fmt.Errorf("txn: op %d (insert %s) returned no row", i, op.Table)
	}
	return explain(ctx, tx, i, op)
}

// explain: UPDATE / DELETE 没有碰到行的原因。同一个事务里按主键再读一次:
// 没有这一行 → NotFound;有(版本不一致,或者读到之前被别的事务改过)→ Conflict。
// R3-2 的条件更新: 行存在且版本一致 → 条件不满足(业务原因码)。
func explain(ctx context.Context, tx pgx.Tx, i int, op Op) error {
	var b strings.Builder
	b.WriteString("SELECT 1 FROM ")
	b.WriteString(pgx.Identifier{op.Table}.Sanitize())
	args := where(&b, op.Key, nil, 0)
	var one int
	err := tx.QueryRow(ctx, b.String(), args...).Scan(&one)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		msg := op.NotFound
		if msg == "" {
			msg = op.Table + " not found"
		}
		return &Error{Reason: NotFound, Op: i, Table: op.Table, Msg: msg}
	case err != nil:
		return classify(i, op, err)
	}
	return &Error{Reason: Conflict, Op: i, Table: op.Table,
		Msg: op.Table + ": version conflict (the row was changed by someone else; read it again and retry)"}
}

// Build: 操作 → SQL + 参数。标识符一律加引号,值一律是参数($n)。
func Build(op Op) (string, []any, error) {
	if op.Table == "" {
		return "", nil, errors.New("txn: op without a table")
	}
	var b strings.Builder
	var args []any
	table := pgx.Identifier{op.Table}.Sanitize()
	switch op.Kind {
	case Insert:
		if len(op.Key) > 0 || op.Version != nil {
			return "", nil, fmt.Errorf("txn: insert %s: Key / Version are for update and delete", op.Table)
		}
		b.WriteString("INSERT INTO " + table)
		if len(op.Values) == 0 {
			b.WriteString(" DEFAULT VALUES")
			break
		}
		names, holders := make([]string, len(op.Values)), make([]string, len(op.Values))
		for i, c := range op.Values {
			if c.Incr != 0 {
				return "", nil, fmt.Errorf("txn: insert %s: column %s: Incr is for update", op.Table, c.Name)
			}
			names[i] = pgx.Identifier{c.Name}.Sanitize()
			args = append(args, c.Value)
			holders[i] = "$" + strconv.Itoa(len(args))
		}
		b.WriteString(" (" + strings.Join(names, ", ") + ") VALUES (" + strings.Join(holders, ", ") + ")")
	case Update:
		if len(op.Key) == 0 {
			return "", nil, fmt.Errorf("txn: update %s without a key", op.Table)
		}
		if len(op.Values) == 0 {
			return "", nil, fmt.Errorf("txn: update %s without columns to set", op.Table)
		}
		b.WriteString("UPDATE " + table + " SET ")
		for i, c := range op.Values {
			if i > 0 {
				b.WriteString(", ")
			}
			name := pgx.Identifier{c.Name}.Sanitize()
			if c.Incr != 0 {
				fmt.Fprintf(&b, "%s = %s + %d", name, name, c.Incr)
				continue
			}
			args = append(args, c.Value)
			fmt.Fprintf(&b, "%s = $%d", name, len(args))
		}
		args = append(args, where(&b, op.Key, op.Version, len(args))...)
	case Delete:
		if len(op.Key) == 0 {
			return "", nil, fmt.Errorf("txn: delete %s without a key", op.Table)
		}
		b.WriteString("DELETE FROM " + table)
		args = append(args, where(&b, op.Key, op.Version, 0)...)
	default:
		return "", nil, fmt.Errorf("txn: unknown op kind %d", op.Kind)
	}
	// RETURNING 总是要有: 碰到了几行靠返回的行数判断(没有要返回的列时 RETURNING 1)
	names := []string{"1"}
	if len(op.Returning) > 0 {
		names = make([]string, len(op.Returning))
		for i, c := range op.Returning {
			names[i] = pgx.Identifier{c}.Sanitize()
		}
	}
	b.WriteString(" RETURNING " + strings.Join(names, ", "))
	return b.String(), args, nil
}

// where: " WHERE k1 = $n AND k2 = $n+1 [AND version = $m]"。n 从 offset+1 开始。
func where(b *strings.Builder, key []Col, version *Col, offset int) []any {
	var args []any
	conds := append([]Col{}, key...)
	if version != nil {
		conds = append(conds, *version)
	}
	for i, c := range conds {
		if i == 0 {
			b.WriteString(" WHERE ")
		} else {
			b.WriteString(" AND ")
		}
		args = append(args, c.Value)
		fmt.Fprintf(b, "%s = $%d", pgx.Identifier{c.Name}.Sanitize(), offset+len(args))
	}
	return args
}

// classify: 数据库错误 → 原因。CHECK / NOT NULL / 值不合法说明请求的校验漏了规则(设计稿的问题):
// 同时计入异常指标(write_rejected_total),不能只是悄悄地变成 INVALID_ARGUMENT。
func classify(i int, op Op, err error) error {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return err
	}
	e := &Error{Op: i, Table: op.Table, Constraint: constraintOf(pe), Err: err}
	switch {
	case pe.Code == "23505":
		e.Reason, e.Msg = Unique, op.Table+": already exists"
	case pe.Code == "23514" || strings.HasPrefix(pe.Code, "22"):
		e.Reason, e.Msg = Check, op.Table+": rejected by a database rule"
	case pe.Code == "23502":
		e.Reason, e.Msg = NotNull, op.Table+": a required value is missing"
	case pe.Code == "23503":
		e.Reason, e.Msg = ForeignKey, op.Table+": a referenced row does not exist, or the row is still referenced"
	case pe.Code == "40001" || pe.Code == "40P01":
		return err // 重试(Run)
	default:
		return err
	}
	if e.Constraint != "" {
		e.Msg += " (" + e.Constraint + ")"
	}
	if e.Reason == Check || e.Reason == NotNull {
		anomaly.ReportWrite(op.Table, e.Constraint)
	}
	return e
}

func constraintOf(pe *pgconn.PgError) string {
	if pe == nil {
		return ""
	}
	if pe.ConstraintName != "" {
		return pe.ConstraintName
	}
	return pe.ColumnName
}

// IsRetryable: 序列化失败 / 死锁(40001 / 40P01)。InTx 会自己重来;在 InTx 外面自己管事务时用。
func IsRetryable(err error) bool { return retryable(err) }

func retryable(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && (pe.Code == "40001" || pe.Code == "40P01")
}
