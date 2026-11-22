package txn

// 条件更新 / 组合命令(api-factory 命令工厂的 R3-2)用的表达式: update 的 SET 的值(例: available = available + $1)
// 和条件(例: available + $1 >= $2)。只有很小的一组运算(列、值、加减、比较、并且),SQL 由 Build 组装:
// 标识符一律加引号,值一律是参数。表达式也是数据: 生成的代码写出表达式,这里不解析任何文本。

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Expr: 表达式的一个节点。
//   - "col": 这一行(update 的对象)的列 Name 的值(更新之前的值,和 SQL 一样)
//   - "val": 值 Value(参数;可以是 Later: 执行到这个操作时才取值)
//   - "+" / "-": Args[0] ± Args[1];"*": Args[0] × Args[1](R5: 数量 × 单价)
//   - "cast": Args[0]::Name(参数的类型: 两边都是参数的乘法,Postgres 推不出参数的类型)
//   - "=" / "<>" / "<" / "<=" / ">" / ">=": 比较(任一边为空 → 不确定 → 条件不成立)
//   - "and": 全部成立
type Expr struct {
	Op    string
	Name  string
	Value any
	Args  []*Expr
}

// C: 列。
func C(name string) *Expr { return &Expr{Op: "col", Name: name} }

// V: 值(参数)。
func V(v any) *Expr { return &Expr{Op: "val", Value: v} }

// Add / Sub: 加 / 减。
func Add(a, b *Expr) *Expr { return &Expr{Op: "+", Args: []*Expr{a, b}} }
func Sub(a, b *Expr) *Expr { return &Expr{Op: "-", Args: []*Expr{a, b}} }

// Mul: 乘(两边常常都是参数: 用 T 写明参数的类型)。
func Mul(a, b *Expr) *Expr { return &Expr{Op: "*", Args: []*Expr{a, b}} }

// T: x::typ(typ 是 Postgres 的类型名: int4 / int8 / numeric …)。
func T(x *Expr, typ string) *Expr { return &Expr{Op: "cast", Name: typ, Args: []*Expr{x}} }

// Cmp: 比较(op = "=" / "<>" / "<" / "<=" / ">" / ">=")。
func Cmp(op string, a, b *Expr) *Expr { return &Expr{Op: op, Args: []*Expr{a, b}} }

// And: 全部成立。
func And(xs ...*Expr) *Expr { return &Expr{Op: "and", Args: xs} }

// Later: 执行到这个操作时才取的值 —— 组合命令里后面的操作引用前面操作的返回值(前面的操作执行完才知道)。
// 生成的代码写成 txn.Later(func() any { return r1.Id }),r1 是前一个操作的 Into 的目标。
type Later func() any

// arg: 参数的值(Later → 现在取值)。
func arg(v any) any {
	if l, ok := v.(Later); ok {
		return l()
	}
	return v
}

var typeName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

var cmpOps = map[string]bool{"=": true, "<>": true, "<": true, "<=": true, ">": true, ">=": true}

// sqlExpr: 表达式 → SQL(参数接在 args 后面)。
func sqlExpr(b *strings.Builder, x *Expr, args *[]any) error {
	if x == nil {
		return errors.New("txn: nil expression")
	}
	switch {
	case x.Op == "col":
		if x.Name == "" {
			return errors.New("txn: column without a name")
		}
		b.WriteString(pgx.Identifier{x.Name}.Sanitize())
	case x.Op == "val":
		*args = append(*args, arg(x.Value))
		b.WriteString("$" + strconv.Itoa(len(*args)))
	case x.Op == "cast":
		if len(x.Args) != 1 || !typeName.MatchString(x.Name) {
			return fmt.Errorf("txn: cast to %q", x.Name)
		}
		b.WriteString("(")
		if err := sqlExpr(b, x.Args[0], args); err != nil {
			return err
		}
		b.WriteString(")::" + x.Name)
	case x.Op == "+" || x.Op == "-" || x.Op == "*" || cmpOps[x.Op]:
		if len(x.Args) != 2 {
			return fmt.Errorf("txn: %s needs 2 operands, got %d", x.Op, len(x.Args))
		}
		b.WriteString("(")
		if err := sqlExpr(b, x.Args[0], args); err != nil {
			return err
		}
		b.WriteString(" " + x.Op + " ")
		if err := sqlExpr(b, x.Args[1], args); err != nil {
			return err
		}
		b.WriteString(")")
	case x.Op == "and":
		if len(x.Args) == 0 {
			return errors.New("txn: and without operands")
		}
		b.WriteString("(")
		for i, a := range x.Args {
			if i > 0 {
				b.WriteString(" AND ")
			}
			if err := sqlExpr(b, a, args); err != nil {
				return err
			}
		}
		b.WriteString(")")
	default:
		return fmt.Errorf("txn: unknown expression %q", x.Op)
	}
	return nil
}

// isCond: 条件的表达式(比较 / 并且)。
func isCond(x *Expr) bool {
	if x == nil {
		return false
	}
	if x.Op == "and" {
		for _, a := range x.Args {
			if !isCond(a) {
				return false
			}
		}
		return len(x.Args) > 0
	}
	return cmpOps[x.Op]
}
