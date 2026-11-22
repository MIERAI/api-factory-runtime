// Package server 是运行时模板的启动部分。所有生成出来的服务共用,AI 不可修改。
//
// 提供: 配置读取 / DB 连接池 / 拦截器链(日志→错误清洗→超时→请求开始的时刻→校验) / livez・readyz・healthz / 优雅停机
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MIERAI/api-factory-runtime/anomaly"
	"github.com/MIERAI/api-factory-runtime/apierr"
	"github.com/MIERAI/api-factory-runtime/broker"
	"github.com/MIERAI/api-factory-runtime/clock"
	"github.com/MIERAI/api-factory-runtime/message"
	"github.com/MIERAI/api-factory-runtime/metrics"
	"github.com/MIERAI/api-factory-runtime/paging"
	"github.com/MIERAI/api-factory-runtime/shard"
	"github.com/MIERAI/api-factory-runtime/txn"
)

// Service 是一个已挂载的 Connect 服务。
type Service struct {
	Path    string
	Handler http.Handler
}

// Deps 是平台提供给 handler 的依赖。
type Deps struct {
	Shards *shard.Router // 分库的连接路由(没有配置时 = 主库)
	Pool   *pgxpool.Pool
	Pager  *paging.Pager
	// AMQP: 到 RabbitMQ 的连接(AMQP_URL;没有设置时 = nil)。outbox 的中继、消费循环用(R4)
	AMQP *broker.Conn
	// Workers: 和服务器同生命周期的后台任务(outbox 的中继、消费循环、R5 的超时扫描)。Builder 里登记
	Workers *Workers
}

// Builder 由生成的 wiring 代码实现,负责把 handler 挂到服务上。
type Builder func(deps Deps, opts ...connect.HandlerOption) []Service

type config struct {
	port            string
	databaseURL     string
	requestTimeout  time.Duration
	shutdownTimeout time.Duration
}

func loadConfig() (config, error) {
	c := config{
		port:           envOr("PORT", "8080"),
		databaseURL:    os.Getenv("DATABASE_URL"),
		requestTimeout: 3 * time.Second,
		// 停机时等 HTTP 请求 / 后台任务各自的上限: 要 ≥ 消息处理器的事务超时(consume.Consumer.Timeout,默认 10s)
		shutdownTimeout: 15 * time.Second,
	}
	if c.databaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	if v := os.Getenv("REQUEST_TIMEOUT_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("REQUEST_TIMEOUT_MS: %w", err)
		}
		c.requestTimeout = time.Duration(ms) * time.Millisecond
	}
	if v := os.Getenv("SHUTDOWN_TIMEOUT_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("SHUTDOWN_TIMEOUT_MS: %w", err)
		}
		c.shutdownTimeout = time.Duration(ms) * time.Millisecond
	}
	return c, nil
}

// Run 启动服务,直到收到 SIGINT/SIGTERM。
func Run(build Builder) {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log, build); err != nil {
		log.Error("server exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, build Builder) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	authn, err := loadAuth()
	if err != nil {
		return err
	}
	if os.Getenv(clock.TestClockEnv) == "1" {
		log.Warn("FACTORY_TEST_CLOCK=1: the request header " + clock.Header + " sets now() (test environments only)")
	}
	if txn.TestFaultsEnabled() {
		log.Warn(txn.TestFaultsEnv + "=1: the request header " + txn.FaultHeader + " makes writes fail (test environments only)")
	}
	if authn.mode == "none" {
		log.Warn("AUTH_MODE=none: the server does not authenticate (the platform must, e.g. Cloud Run IAM)")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	poolCfg, err := pgxpool.ParseConfig(cfg.databaseURL)
	if err != nil {
		return fmt.Errorf("db config: %w", err)
	}
	// 匿名预编译语句: Postgres 每次都用实际参数值做执行计划。
	// 生成的查询大量使用 "(@x IS NULL OR col = @x)" 形式的可选条件和 keyset 游标,
	// 命名预编译语句在执行若干次后可能切换成通用计划,游标就不再是索引条件(深页退化成 OFFSET)。
	// 一次往返,PgBouncer 事务模式下也可用。
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("db pool: %w", err)
	}
	defer pool.Close()
	// 分库: 和主库同样的连接设置
	rules, err := shard.Parse(os.Getenv("FACTORY_SHARDS"))
	if err != nil {
		return err
	}
	shards := shard.New(rules, pool, func(ctx context.Context, url string) (*pgxpool.Pool, error) {
		c, err := pgxpool.ParseConfig(url)
		if err != nil {
			return nil, err
		}
		c.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
		return pgxpool.NewWithConfig(ctx, c)
	})
	defer shards.Close()

	opts := connect.WithInterceptors(
		newLoggingInterceptor(log),
		apierr.NewSanitizeInterceptor(log),
		newTimeoutInterceptor(cfg.requestTimeout),
		clock.NewInterceptor(),        // 查询里的 now(): 请求开始的时刻(FACTORY_TEST_CLOCK=1 时由请求头 X-Factory-Now 固定)
		message.NewTraceInterceptor(), // 请求头 traceparent → 写操作发出的消息(R4)
		txn.NewInterceptor(),          // 写操作的故障注入: 只在 FACTORY_TEST_FAULTS=1 时认请求头 X-Factory-Fail-After
		validate.NewInterceptor(),
	)

	mux := http.NewServeMux()
	if os.Getenv("PAGE_TOKEN_KEY") == "" {
		log.Warn("PAGE_TOKEN_KEY not set: using a random key (page tokens break across restarts/instances)")
	}
	deps := Deps{Pool: pool, Pager: paging.New([]byte(os.Getenv("PAGE_TOKEN_KEY"))), Shards: shards, Workers: &Workers{}}
	if u := os.Getenv("AMQP_URL"); u != "" {
		deps.AMQP = broker.New(u, envOr("SERVICE_NAME", "api-factory-service"))
		defer deps.AMQP.Close()
	}
	for _, s := range build(deps, opts) {
		mux.Handle(s.Path, s.Handler)
		log.Info("service mounted", "path", s.Path)
	}
	writeMetrics := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		anomaly.WriteMetrics(w)
		metrics.Write(w)
		metrics.WriteMessaging(w)
		metrics.WriteMachines(w)
	}
	mux.HandleFunc("/metrics", writeMetrics) // 对外的端口: 和 API 一样要认证
	// METRICS_PORT: 指标的内部端口(不认证,不对外暴露;Prometheus 从这里抓)
	if p := os.Getenv("METRICS_PORT"); p != "" {
		mm := http.NewServeMux()
		mm.HandleFunc("/metrics", writeMetrics)
		ms := &http.Server{Addr: ":" + p, Handler: mm, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			log.Info("metrics listening", "port", p)
			if err := ms.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics server", "err", err)
			}
		}()
		defer ms.Close()
	}
	// 探针(依赖不进 liveness / readiness): 数据库挂了,重启进程或把所有 pod 摘出 Service 都修不好它,只会让用户
	// 拿到连接被拒绝、让 pod 内的 SLI 什么都数不到。进程照常接请求,对数据库失败快速返回 unavailable(计入 SLO)。
	//   /livez  进程能回答 → liveness
	//   /readyz 准备好接流量(停机开始后 503,先被摘出 Service)→ readiness
	//   /healthz 深度检查(数据库能 ping 通): 外部探针 SLO、部署后的 smoke test
	var stopping atomic.Bool
	addProbes(mux, &stopping)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := pool.Ping(pingCtx); err != nil {
			http.Error(w, "db unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("X-App-Version", envOr("APP_VERSION", "dev")) // 部署后确认是哪个版本在服务
		// INSTANCE_ID(测试用): 回答健康检查的是不是这个进程(并行测试时端口可能被别的服务器占着)
		_, _ = w.Write([]byte("ok " + os.Getenv("INSTANCE_ID")))
	})

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true) // gRPC 需要 h2c
	srv := &http.Server{
		Addr:              ":" + cfg.port,
		Handler:           authn.wrap(mux),
		Protocols:         protocols,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1+len(deps.Workers.list))
	go func() {
		log.Info("listening", "port", cfg.port)
		errCh <- srv.ListenAndServe()
	}()
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()
	waitWorkers := deps.Workers.start(workerCtx, log, errCh)

	select {
	case err := <-errCh:
		stopping.Store(true)
		stopWorkers()
		waitWorkers(cfg.shutdownTimeout)
		return err
	case <-ctx.Done():
		stopping.Store(true)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
		defer cancel()
		err := srv.Shutdown(shutdownCtx)
		// 后台任务在 HTTP 之后停: 消费循环做完正在处理的消息(上限 = 消息的事务超时),中继做完这一批
		stopWorkers()
		waitWorkers(cfg.shutdownTimeout)
		return err
	}
}

// addProbes: /livez 和 /readyz,都不碰数据库(见 Run 里的说明)。
func addProbes(mux *http.ServeMux, stopping *atomic.Bool) {
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if stopping.Load() {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
}

func newLoggingInterceptor(log *slog.Logger) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()
			res, err := next(ctx, req)
			code := "ok"
			if err != nil {
				code = connect.CodeOf(err).String()
			}
			metrics.Observe(req.Spec().Procedure, code, time.Since(start))
			log.InfoContext(ctx, "rpc",
				"procedure", req.Spec().Procedure,
				"code", code,
				"duration_ms", time.Since(start).Milliseconds(),
			)
			return res, err
		}
	}
}

func newTimeoutInterceptor(d time.Duration) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, req)
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
