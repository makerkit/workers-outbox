# Changelog

## 0.0.1 — 2026-11-04

Initial release of `@makerkit/workers-outbox`: a transactional outbox for Postgres on Cloudflare Workers (Hyperdrive + Queues + Cron Triggers).

### Features

- **Transactional enqueue.** `outbox.enqueue(tx, …)` writes a job in your business transaction. `outbox.transaction(db, (tx, enqueue) => …)` does the same and nudges every created job once the transaction commits. A job enqueued with `delayMs` is nudged with that delay (up to Queues' 24 h ceiling).
- **Per-tenant dedupe keys**, `maxPayloadBytes` (default 256 KiB, `invalid_payload` above it) and `TenantScope`-scoped enqueues.
- **Typed jobs and `deps`.** `defineJob` and `defineJobFor<Deps>()` type payloads and `ctx.deps`. `createOutbox` infers the required `deps` from its jobs and won't compile without them. `Outbox<J>` is a named, exportable interface.
- **Outcomes and retries.** `ok()`, `retry({ code, after })`, `fail({ message })` and fine-grained retry rules. `ok({ commit })` completes a job atomically with your own writes, behind a fenced commit.
- **Leases and crash recovery.** Claims break ties on `id` and are fenced by an attempt ledger. The cron sweep recovers expired leases, re-drives lost nudges, hands due long jobs to their queue (at most once per 15 min) and cleans up.
- **Lanes** isolate slow jobs on their own connections. Deploy-version gating (`version`) lets an older deploy skip newer jobs during a gradual rollout.
- **Recurring schedules.** `createScheduledHandler(outboxFor, { schedules })` with `defineSchedule({ cron, job, payload })` or `defineSchedule({ cron, name, run })`. Occurrences are keyed by scheduled time, so repeated deliveries enqueue nothing new; missed ticks are skipped.
- **Cloudflare handlers** in `./cloudflare`: `createQueueHandler`, `createDlqHandler`, `createScheduledHandler`.
- **Time limits.** One invocation deadline per queue batch or cron tick (`invocationBudgetMs`, default 840 s), a recovery cap (`recoveryBudgetMs`, default 120 s), a settlement deadline enforced by native Postgres timeouts, `operationTimeoutMs` for operator actions, and `claim_late` for claims that arrive too late to run safely.
- **Operator actions:** `cancel`, `replay`, `retryNow`, `eraseTenant` and bounded bulk operations. **`./inspect`** provides read-only queries under a transaction-local `statement_timeout`.
- **Events** (`OutboxEvent`) for every transition, including `claim_late`, `schedule_ran` and `schedule_failed`, plus `onTransition` hooks.
- **Schema** via drizzle (`./drizzle`) or plain SQL (`OUTBOX_MIGRATIONS` in `./sql`). `verifyOutboxSchema` reports drift, invalid indexes and unsupported server versions. `0001_init` reserves `job.ordering_key`, `job.seq` and `attempt.kind` for future per-key ordering and per-kind rate limits.
- **`./pg` connectors.** `createPgConnector(connectionString)` (a fresh `pg.Client` per lane, as Hyperdrive expects) and `createPgPoolConnector(pool)`. Both detect broken sessions (`LaneConnection.isBroken()`) and treat node-pg's code-less connection errors as retryable.
- **Test harness** in `./testing`: `createOutboxTestDatabase()`, `createTestClock()` and `runUntilIdle(outbox, { clock, db })`.
- **`examples/worker`:** a complete, typechecked Worker with a setup checklist.

### Requirements

- PostgreSQL 17 or newer.
- `pg` 8.15+ (optional peer dependency) for the `./pg` connectors.
- Custom connectors must implement an idempotent `LaneConnection.destroy()`.
