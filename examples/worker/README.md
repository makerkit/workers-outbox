# Example Worker

A minimal Worker: `POST /signup` inserts a user and enqueues a welcome email in the same transaction; the queue consumer sends it, and the cron trigger is the backstop.

| File                               | What it shows                                                                        |
| ---------------------------------- | ------------------------------------------------------------------------------------ |
| [`src/jobs.ts`](src/jobs.ts)       | A job with typed `ctx.deps` (`defineJobFor`), outcomes, `signal` and idempotency key |
| [`src/index.ts`](src/index.ts)     | `createOutbox` with the shipped `pg` connector, `outbox.transaction`, the handlers   |
| [`src/schema.ts`](src/schema.ts)   | Re-exporting the outbox tables for drizzle-kit                                       |
| [`wrangler.jsonc`](wrangler.jsonc) | Hyperdrive ×2, the queue and its DLQ, the cron trigger, version metadata             |

It is typechecked with the library (`pnpm typecheck`), so it can't drift from the API.

## Setup checklist

Each item fails quietly if missing, so check them all:

1. **PostgreSQL 17+.** Older servers can't run `transaction_timeout`.
2. **Schema.** Generate and apply the migration with drizzle-kit (from `src/schema.ts`), or apply `OUTBOX_MIGRATIONS` from `./sql`. Then run `verifyOutboxSchema(db)` in CI or a release step and fail on any result. With drizzle-kit this is the only version check.
3. **Two Hyperdrive configs** for the same database: the app's, and `OUTBOX_DB` with caching disabled.
4. **`idle_in_transaction_session_timeout` on the role `OUTBOX_DB` connects as**, plus grants on the app tables your handlers touch. A dedicated `outbox_worker` role is recommended ([roles and session guardrails](../../README.md#production-setup)).
5. **`nodejs_compat`** in `compatibility_flags` (node-postgres needs it).
6. **A queue and its DLQ**, each with a consumer. One `queue()` export serves both, routed on `batch.queue`. Without a queue, jobs still run, but only on the cron sweep.
7. **A cron trigger** (`*/5 * * * *`). It recovers crashed jobs and re-drives lost nudges; without it, a lost nudge strands a job.
8. **`version_metadata`**, passed as `version`, so an older deploy skips newer jobs during a gradual rollout.
9. **An `events` sink.** `nudge_skipped`, `settle_failed` and `infra_error` are how a misconfiguration shows up.

## Test it locally

`pnpm test:example` (from the repo root, with `pnpm db:up` running) starts this Worker under `wrangler dev`, which is workerd with local Queues and cron. It runs against a fresh PostgreSQL database and a stub mail API, and drives the Worker over HTTP and the cron endpoint:

- **Enqueue:**
  - A signup commits the user and its job, and the queue runs the job once.
  - The Worker's events sink logs the job's lifecycle.
  - An invalid email is rejected before any write.
  - A duplicate signup returns 409 and rolls back, leaving one user, one job and one email.
  - 20 concurrent signups each get exactly one email.
- **Handler outcomes:**
  - A 429 with `Retry-After` retries after the provider's delay, with the same idempotency key.
  - A 5xx backs off for 15–30 s, isn't re-sent before it's due, and is then retried by the cron sweep.
  - A 4xx dead-letters at once, with an error-severity event, and is never re-run.
  - A dropped connection to the provider throws, and the default `classify` retries it.
- **Cron:**
  - The sweep runs a job that was never nudged.
  - It recovers a job whose worker died mid-attempt.
  - An older deploy leaves a newer deploy's job alone.
  - Cleanup deletes succeeded jobs past their retention and keeps recent ones.

It needs wrangler, either on `PATH` or given as `WRANGLER=/path/to/wrangler`; without it the test is skipped. Hyperdrive is a local passthrough under `wrangler dev`, so its pooling isn't exercised.

## Try it

```sh
wrangler hyperdrive create app-db --connection-string=…
wrangler hyperdrive create outbox-db --connection-string=… --caching-disabled
wrangler queues create outbox && wrangler queues create outbox-dlq
wrangler deploy
curl -X POST https://<worker>/signup -d '{"email":"ada@example.com"}'
```
