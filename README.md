# api-factory-runtime

The runtime library of the services that **api-factory** generates: everything a generated service needs at run time,
nothing it needs at generation time. Generated code imports only this module (and its own generated packages); the
factory itself — the generator, the core algebra, the verification — is a separate, private project.

| Package | What it does |
|---|---|
| `server` | The HTTP/Connect server: health (`/healthz`, `X-App-Version`), metrics, authentication (`AUTH_MODE`), graceful shutdown, the database pool; the RabbitMQ connection (`AMQP_URL`) and background workers (`Deps.Workers`: outbox relay, consumers) |
| `apierr` | Maps database and validation errors to Connect error codes |
| `paging` | Keyset page tokens: signed, bound to the request's filters |
| `shard` | Routes a request to one of several databases by a shard key (e.g. per store) |
| `txn` | The transaction executor of commands (writes): runs write operations given as data in one transaction (READ COMMITTED), tells "not found" from "version conflict" when an update/delete touches no row, classifies database errors (unique / check / not null / foreign key), retries serialization failures and deadlocks; test-only fault injection (`FACTORY_TEST_FAULTS=1` + `X-Factory-Fail-After`) |
| `clock` | The request's start time (`now()` in queries); in tests (`FACTORY_TEST_CLOCK=1`) the header `X-Factory-Now` fixes it |
| `conv` | Conversions between database and protobuf types (decimal, date, timestamp) |
| `metrics` | Request counts and latency histograms in the Prometheus text format (no dependency); messaging metrics (outbox backlog, consumer outcomes, lag, DLQ size) |
| `anomaly` | Counts values read from the database that the domain layer does not know (`data_anomaly_total`) and writes rejected by a database rule the request validation did not check (`write_rejected_total`) |
| `testkit` | Helpers for the generated services' tests |
| `message` | The message envelope (UUIDv7 id, type, version, occurred_at, source, subject, traceparent, data), its AMQP representation, W3C traceparent propagation |
| `outbox` | Writes messages into the outbox table inside the command's transaction; the relay that publishes them with publisher confirms (one active relay per database); the platform tables' DDL (`zz_factory_platform.sql`) |
| `consume` | The consumer loop of a message handler: dedup by message id in the handler's transaction, ack after commit, tiered retry queues, DLQ, poison messages, graceful shutdown; DLQ admin (`list` / `requeue` / `drop`) |
| `topology` | Declares the exchanges, queues, retry queues and dead letter queues |
| `broker` | The process's RabbitMQ connection (reconnects on demand) |
| `leader` | "One active instance per database" background tasks (Postgres advisory lock): the outbox relay, later the state machines' timeout scanner |
| `cmd/factory-runtime` | Operations CLI: `factory-runtime dlq {list\|requeue\|drop} --queue <queue>` |

## Messaging (R4, v0.4.0)

Writes can emit messages (events and commands) and messages can trigger writes, with no I/O code in the generated
handlers. Delivery is **at least once**; effects are made exactly-once by deduplication (message id) plus reentrant
handlers. **No ordering is guaranteed**, not even per subject: handlers must be safe under reordering.

```
 write (txn.Executor.InTx) ──► factory_outbox ──► outbox.Relay ──► factory.events (topic) / factory.commands (direct)
                                (same tx)          confirms, then marks published        │ routing key = message type
                                                                                         ▼
 handler effects + factory_processed_messages ◄── consume.Consumer ◄── <service>.<type> ──► .retry.1/2/3 (TTL) ──► back
                 (one tx, then ack)                                                     └──► .dlq
```

### `message`

- `Envelope{ID, Type, Version, OccurredAt, Source, Subject, Traceparent, Data}`; `Encode()` (UTC, microseconds) /
  `Decode(body)` (validates the envelope, ignores unknown fields; failures are `*DecodeError` = poison).
- `NewID()` returns a UUIDv7; `ValidID`, `IDTime`.
- `WithTraceparent(ctx, tp)` / `Traceparent(ctx)` / `ChildTraceparent(tp)`; `NewTraceInterceptor()` takes the HTTP
  `traceparent` header (installed by `server`). Consumers continue the trace with a child span id.
- AMQP header names: `x-factory-attempt`, `x-factory-error`, `x-factory-outcome`, `x-factory-version`, and the test-only
  `x-factory-crash`, `x-factory-fail-after`, `x-factory-now`.

### `outbox`

- `PlatformSQL` (embedded `zz_factory_platform.sql`): `factory_outbox` and `factory_processed_messages`. The factory
  copies it verbatim into a project's `db/schema/`; the runtime never creates tables at start-up.
- `NewWriter(service).Write(ctx, tx, outbox.Message{Kind, Type, Version, Subject, Data}...)` inserts the messages in
  the caller's transaction and `NOTIFY`s the relay. Rollback means no message. `occurred_at` is `clock.Now(ctx)`;
  each row counts as a write for test fault injection (`txn.AfterWrite`).
- `Relay{DB, Broker}.Run(ctx)`: one active relay per database (`leader`, advisory lock); batches of 100 by `seq`;
  persistent publishes with `message_id` = envelope id; waits for publisher confirms and only then sets
  `published_at`. Commands are published `mandatory` (an unroutable command stays in the outbox with `last_error`);
  events are not. Failed rows back off individually (1s → 30s); a broken broker connection backs off the relay.
  Woken by `NOTIFY`, polls every second as a fallback. Hourly cleanup (`Cleanup`): published rows after 7 days,
  dedup rows after 14 days. A crash between publish and mark re-publishes the same id with the same bytes.

### `consume`

- `Consumer{Queue, Handler, Exec, Broker, Accept, Retry}.Run(ctx)`; `Handler` is
  `func(ctx, tx pgx.Tx, env message.Envelope) error` and does all its writes on `tx` (`txn.RunIn`, `txn.Savepoint`
  for business outcomes, `outbox.Writer.Write` for follow-up messages).
- Per message: decode and check type/version → in one transaction insert `(handler, message_id)` into
  `factory_processed_messages` (conflict = duplicate: commit, ack, skip) and run the handler → commit → ack.
- Errors: `consume.Poison(err)` (or an undecodable envelope / unaccepted type or version) goes straight to `<queue>.dlq`.
  Anything else — including panics and the 10s transaction timeout — is transient: a copy goes to
  `<queue>.retry.<tier>` (`x-factory-attempt` = next attempt) after a publisher confirm, then the original is acked.
  Attempt 5 failing goes to the DLQ with `x-factory-error` / `x-factory-outcome: exhausted`.
- A declared business outcome is not an error: the handler rolls back its operations with `txn.Savepoint`, writes the
  outcome's messages, commits, and may call `consume.Business(ctx, "INSUFFICIENT_STOCK")` so the outcome is counted as
  `business`.
- Prefetch 16, handled concurrently. On shutdown: cancel the subscription, finish in-flight messages, leave prefetched
  ones unacked for redelivery.
- `Process(ctx, Delivery)` is the transport-free core (decision + transaction) for in-process tests; `Run` turns its
  `Result` into ack / retry / DLQ.
- DLQ admin: `DLQList`, `DLQRequeue` (keeps the id, so already-processed messages are deduplicated), `DLQDrop`,
  `QueueDepth`; also as the `factory-runtime dlq` CLI.

### `topology`

- `Declare(ch, Spec{Queues, Retry})`: exchanges `factory.events` (topic) and `factory.commands` (direct); per queue
  `<service>.<type>` (classic, durable, dead-lettering to its DLQ), `.retry.1/.2/.3` (queue TTL 1s / 10s / 60s,
  dead-lettering back to the queue) and `.dlq`. Idempotent; mismatching arguments fail (RabbitMQ
  `PRECONDITION_FAILED`) instead of being changed silently.
- `DefaultRetry` = tiers 1s/10s/60s, max 5 attempts (delays 1s, 10s, 60s, 60s). `QueueName`, `RetryQueue`, `DLQ`.
- One vhost per environment, chosen by `AMQP_URL` (e.g. `amqp://user:pass@rabbitmq:5672/prod`).

### `leader`

- `Run(ctx, pool, name, Options, fn)` runs `fn(ctx, conn)` only while holding the session-level advisory lock
  `Key(name)` on a dedicated connection; other instances stand by and retry. A lost connection ends the term.
  The outbox relay uses `factory.outbox.relay`; the R5 timeout scanner is meant to use its own name the same way.

### `server` additions

- `Deps.AMQP` (`AMQP_URL`, connects lazily) and `Deps.Workers.Go(name, fn)`: background tasks started after the server
  listens and stopped after HTTP on shutdown (`SHUTDOWN_TIMEOUT_MS`, default 15s ≥ the consumer's 10s transaction
  timeout). A worker returning an error before shutdown stops the server (fail fast, e.g. a topology mismatch).
- `/metrics` also writes the messaging metrics: `factory_outbox_pending`, `factory_outbox_oldest_pending_age_seconds`,
  `factory_relay_leader`, `factory_outbox_published_total`, `factory_outbox_publish_errors_total{reason}`,
  `factory_messages_total{handler,outcome}`, `factory_handler_panics_total`, `factory_message_lag_seconds`,
  `factory_handler_duration_seconds`, `factory_dlq_messages{queue}`.

### `txn` additions

- `Executor.InTx(ctx, fn)`: a transaction around arbitrary writes with the same retry (serialization failure /
  deadlock) and classification as `Run` (now `Run` = `InTx` + `RunIn`). `RunIn(ctx, tx, ops...)`,
  `Savepoint(ctx, tx, fn)` (roll back only the savepoint; a serialization failure inside still restarts the whole
  transaction), `AfterWrite(ctx, table)`, `IsRetryable(err)`.

### Test hooks (test environments only)

Only with `FACTORY_TEST_FAULTS=1`; production ignores them (unit-tested):

| Hook | Effect |
|---|---|
| `FACTORY_TEST_RELAY_CRASH=after_publish[:n]` / `after_confirm[:n]` | The relay calls `os.Exit(137)` in batch `n` (default 1) after publishing / after the confirms, before marking |
| AMQP header `x-factory-crash: before_commit` / `after_commit` / `after_retry_publish` | The consumer exits 137 at that point (first delivery only; copies drop the header) |
| AMQP header `x-factory-fail-after: k` | Like HTTP `X-Factory-Fail-After`: the handler's transaction fails after write `k` |
| AMQP header `x-factory-now` (with `FACTORY_TEST_CLOCK=1`) | Fixes `clock.Now` in the handler |

### Integration tests

Run against a real Postgres and RabbitMQ when `RUNTIME_TEST_DB` (a user that may `CREATE DATABASE`; each test gets
its own database) and `RUNTIME_TEST_AMQP` are set, otherwise skipped. CI runs them with service containers.

```sh
docker run -d --name rt-pg -p 55460:5432 -e POSTGRES_PASSWORD=pw --tmpfs /var/lib/postgresql/data postgres:16-alpine
docker run -d --name rt-mq -p 55672:5672 --tmpfs /var/lib/rabbitmq rabbitmq:3.13.7-management
RUNTIME_TEST_DB='postgres://postgres:pw@localhost:55460/postgres?sslmode=disable' \
RUNTIME_TEST_AMQP='amqp://guest:guest@localhost:55672/' go test -race ./...
```

Code comments are in Chinese (the factory's working language); identifiers and this README are in English.

Versioned with SemVer tags; a generated service pins the version it was generated against.

License: Apache-2.0.
