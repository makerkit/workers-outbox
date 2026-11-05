# workers-outbox

A transactional outbox for Postgres on Cloudflare Workers (Hyperdrive + Queues + Cron Triggers).

Enqueue a job **inside your own database transaction**. It runs if and only if the transaction commits, survives isolate death, retries with backoff, and never runs for the wrong job.

> Status: pre-release.

This README explains why the library exists and when to use it, then builds up from a four-step quick start to production patterns:

1. [Quick start](#quick-start): one job, enqueued with your business write.
2. [Outcomes and retries](#outcomes-and-retries), [typed dependencies](#typed-dependencies), [enqueueing in depth](#enqueueing-in-depth).
3. [Completing atomically](#completing-atomically), [fine-grained retry rules](#fine-grained-retry-rules), [scheduled and recurring jobs](#scheduled-and-recurring-jobs), [lanes](#lanes-isolating-slow-jobs).
4. [Testing](#testing), [production setup](#production-setup), [performance](#performance), [operating](#operate), [best practices](#best-practices).

Examples import the outbox from the package and your app's own code from relative modules (`./jobs`, `./mailer`, `./users`, …); `Env` and `ExecutionContext` are the types `wrangler types` generates. Every `ts` block is typechecked against the library (`pnpm check:readme`).

[`examples/worker`](examples/worker) is a complete, typechecked Worker.

## Why

Most side effects of a web request (a welcome email, a CRM sync, a webhook, a search-index update) must happen **if and only if** the request's database write commits. Doing both directly is a _dual write_, and it fails in both orders:

```ts
import { db } from './db';
import { sendWelcomeEmail } from './mailer';
import { insertUser } from './users';

// ✗ Commit, then send: if the isolate dies or the provider times out between the two lines,
// the user exists and never gets the email. Nothing will ever retry it.
const created = await db.transaction((tx) => insertUser(tx, input));

await sendWelcomeEmail(created.id, {
  signal: AbortSignal.timeout(30_000),
  idempotencyKey: created.id,
});
```

Send first and the opposite happens: the email goes out for a signup that then rolls back. The outbox pattern fixes this by writing the side effect as a row in the **same transaction** as the business data, and running it afterwards from that durable row.

On Workers, the usual tools don't do this:

- **Cloudflare Queues and Workflows** can't take part in a Postgres transaction. A `queue.send()` after commit is still a dual write.
- **Postgres job queues** (pg-boss, graphile-worker) assume a long-lived worker process that polls. Workers have no such process, only short invocations.

This library keeps the job in your Postgres and uses Cloudflare only to wake it up. A Queue nudge after commit makes jobs start within seconds. A Cron Trigger sweep is the durable backstop: it recovers jobs whose isolate died and runs anything a lost nudge missed. It was extracted from a production outbox, keeping what survived incidents and fixing what didn't.

## When to use it

**A good fit:**

- You use Cloudflare
- A Worker that writes to **PostgreSQL 17+ through Hyperdrive with Drizzle**, and needs side effects tied to those writes: transactional email, webhooks, syncs to CRMs, billing or search, provisioning, cache invalidation.
- Work that is **idempotent, or can be made so**. Delivery is at-least-once; every handler gets a stable `idempotencyKey` to pass to providers that dedupe.
- Multi-tenant SaaS: jobs carry a `tenantKey` that scopes dedupe, operator actions and erasure.
- Scheduled and recurring work that can run a few minutes late: newsletters, weekly digests, trial-ending reminders. See [scheduled and recurring jobs](#scheduled-and-recurring-jobs).
- Teams that want jobs inspectable with SQL, next to the data they belong to, rather than in another system.

**Not a good fit:**

| If you need…                                                    | Because                                                                                                          | Consider                                                    |
| --------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| A side effect with no database write to tie it to               | There is no transaction to join; the outbox only adds a round trip                                               | Cloudflare Queues directly                                  |
| D1, Durable Objects or another database                         | Postgres only, and Drizzle only in v1                                                                            | Queues, Durable Object alarms                               |
| Multi-step workflows: durable steps, sleeps, waiting for events | One job is one handler call; there are no checkpoints inside it                                                  | Cloudflare Workflows, DBOS, or a chain of jobs (see below)  |
| Jobs longer than 10 minutes                                     | `defineJob` rejects `timeoutMs` over 10 min, so every attempt fits one 15-min invocation                         | Split the work into smaller jobs, or use Workflows          |
| Strict ordering per key                                         | Jobs run in due-time order, not FIFO per entity                                                                  | Make handlers order-independent (versions, last-write-wins) |
| Priority, per-tenant fairness, per-kind concurrency/rate limits | Deferred; not in v1                                                                                              | Lanes for isolation; a rate limiter in your handler         |
| Exactly-once side effects with a provider that can't dedupe     | A crash after the call but before settling re-runs the handler                                                   | A provider with idempotency keys, or a dedupe table         |
| Large payloads                                                  | Payloads live in the hot job table; 256 KiB cap by default                                                       | Store the data in R2, enqueue its key                       |
| Thousands of jobs per second, sustained                         | Throughput grows with Postgres connections, and each job is several writes to one table in your primary database | A dedicated queue or streaming system                       |

## Install

```sh
pnpm add @makerkit/workers-outbox drizzle-orm pg
```

Requires **PostgreSQL 17 or newer**. Peer dependencies: `drizzle-orm`; `pg` 8.15+ (and `@types/pg`) for the `./pg` connectors; `@electric-sql/pglite` for `./testing`. Payload schemas can be any Standard Schema validator (zod, valibot, arktype).

## Quick start

Four steps: tables, a job, the outbox, and the Worker's handlers.

**1. Add the tables.** Re-export them from your Drizzle schema, then generate and apply the migration with drizzle-kit as usual:

```ts
// drizzle/schema.ts
export { outboxSchema, outboxJob, outboxAttempt } from '@makerkit/workers-outbox/drizzle';
```

**2. Define a job.** A kind, a payload schema, a timeout, and a handler that returns an outcome:

```ts
// src/jobs.ts
import { defineJob, ok } from '@makerkit/workers-outbox';
import { z } from 'zod';

import { sendWelcomeEmail } from './mailer';

export const welcomeEmail = defineJob({
  kind: 'email.welcome',
  payload: z.object({ userId: z.string() }),
  timeoutMs: 30_000,
  async handle({ userId }, ctx) {
    await sendWelcomeEmail(userId, {
      signal: ctx.signal, // aborts once the attempt has timed out
      idempotencyKey: ctx.idempotencyKey, // stable across retries: delivery is at-least-once
    });

    return ok();
  },
});
```

**3. Create the outbox and enqueue inside your transaction.** The user row and the job commit together, or neither does:

```ts
// src/outbox.ts
import { createOutbox } from '@makerkit/workers-outbox';
import { createPgConnector } from '@makerkit/workers-outbox/pg';

import { welcomeEmail } from './jobs';

// One outbox per invocation; it connects lazily, never at construction.
export const outboxFor = (env: Env) =>
  createOutbox({
    jobs: [welcomeEmail],
    connect: createPgConnector(env.OUTBOX_DB.connectionString),
    queue: env.OUTBOX_QUEUE,
  });
```

```ts
// src/signup.ts, inside your fetch handler (`env` is the handler's)
import { db } from './db';
import { welcomeEmail } from './jobs';
import { outboxFor } from './outbox';
import { insertUser } from './users';

const user = await outboxFor(env).transaction(db, async (tx, enqueue) => {
  const user = await insertUser(tx, input);

  await enqueue(welcomeEmail, { payload: { userId: user.id }, dedupeKey: `welcome:${user.id}` });

  return user;
});
```

`outbox.transaction` nudges the queue once the transaction commits, so the job starts within seconds. A repeated `dedupeKey` returns the existing job instead of creating a second one.

**4. Wire the Worker.** The queue consumer runs nudged jobs; the cron trigger recovers crashed ones and catches anything a lost nudge missed:

```ts
// src/index.ts
import { createQueueHandler, createScheduledHandler } from '@makerkit/workers-outbox/cloudflare';

import { app } from './app';
import { outboxFor } from './outbox';

export default {
  fetch: app.fetch,
  queue: createQueueHandler(outboxFor),
  scheduled: createScheduledHandler(outboxFor),
};
```

```jsonc
// wrangler.jsonc
{
  "compatibility_flags": ["nodejs_compat"], // node-postgres
  // Same database as your app, through a Hyperdrive config with caching disabled.
  "hyperdrive": [{ "binding": "OUTBOX_DB", "id": "<outbox hyperdrive id>" }],
  "queues": {
    "producers": [{ "binding": "OUTBOX_QUEUE", "queue": "outbox" }],
    "consumers": [{ "queue": "outbox" }],
  },
  "triggers": { "crons": ["*/5 * * * *"] },
}
```

That's a working outbox. Each section below adds one concern; before going live, go through the [setup checklist](examples/worker/README.md#setup-checklist).

## Outcomes and retries

A handler returns its result rather than throwing it:

| Outcome                   | Effect                                                                                                              |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `ok()`                    | Job succeeded. `ok({ commit: tx => … })` runs your DB writes in the same transaction as the `succeeded` transition. |
| `retry({ code, after? })` | Retry with backoff, or after `after` ms.                                                                            |
| `fail({ code })`          | Dead-letter, no more retries.                                                                                       |
| `cancel({ reason })`      | Cancel the job.                                                                                                     |

A thrown error goes through `classify`, which defaults to `retry({ code: 'unexpected' })`. The retry policy is per job:

```ts
import { defineJob, fail, ok, retry } from '@makerkit/workers-outbox';
import { z } from 'zod';

import { isHttp4xx } from './http';
import { sendWelcomeEmail } from './mailer';

export const welcomeEmail = defineJob({
  kind: 'email.welcome',
  payload: z.object({ userId: z.string() }),
  timeoutMs: 30_000, // the lease is derived from this
  retry: {
    maxAttempts: 20, // default 20
    backoff: { baseMs: 30_000, maxMs: 3_600_000 }, // exponential, jittered
  },
  // Maps a thrown error to an outcome.
  classify: (err) => (isHttp4xx(err) ? fail({ code: 'http_4xx' }) : retry({ code: 'unexpected' })),
  async handle({ userId }, ctx) {
    // ctx: jobId, kind, attempt, idempotencyKey, signal, deadline, remainingMs(), db, deps,
    //      tenantKey, correlationId, metadata
    await sendWelcomeEmail(userId, { signal: ctx.signal, idempotencyKey: ctx.idempotencyKey });

    return ok();
  },
});
```

To special-case one error and keep the default for the rest, return `undefined` from `classify` (or call the exported `defaultClassify` yourself):

```ts
import { defineJob, ok, retry } from '@makerkit/workers-outbox';
import { z } from 'zod';

export const nightlyPurge = defineJob({
  kind: 'retention.nightly',
  payload: z.object({ day: z.string() }),
  timeoutMs: 60_000,
  // 55P03 (lock_not_available): give way and come back; anything else gets the default.
  classify: (err) =>
    (err as { code?: string }).code === '55P03' ? retry({ code: 'yielded' }) : undefined,
  async handle() {
    return ok();
  },
});
```

`retry({ code, after })` overrides the backoff, e.g. with a provider's `Retry-After`. A job dead-letters once its attempts run out; `warnAtStreak` emits an event when one error code keeps repeating. Per-code budgets are in [fine-grained retry rules](#fine-grained-retry-rules).

## Typed dependencies

Handlers get their clients through `ctx.deps` rather than module globals, so each invocation builds them from its own `env`. Bind the deps type once with `defineJobFor`; `createOutbox` then refuses to compile without matching `deps`:

```ts
import { createOutbox, defineJobFor, ok, retry } from '@makerkit/workers-outbox';
import { createPgConnector } from '@makerkit/workers-outbox/pg';
import { z } from 'zod';

import { createMailer, type Mailer } from './mailer';

interface Deps {
  mailer: Mailer;
}

const defineJob = defineJobFor<Deps>();

export const welcomeEmail = defineJob({
  kind: 'email.welcome',
  payload: z.object({ userId: z.string() }),
  timeoutMs: 30_000,
  async handle({ userId }, ctx) {
    const res = await ctx.deps.mailer.send(userId, {
      signal: ctx.signal,
      idempotencyKey: ctx.idempotencyKey,
    });

    if (res.rateLimited) return retry({ code: 'rate_limited', after: res.retryAfterMs });

    return ok();
  },
});

export const outboxFor = (env: Env) =>
  createOutbox({
    jobs: [welcomeEmail],
    deps: { mailer: createMailer(env) }, // a compile error if missing or the wrong shape
    connect: createPgConnector(env.OUTBOX_DB.connectionString),
    queue: env.OUTBOX_QUEUE,
  });
```

With several jobs, `deps` must satisfy all of them at once.

## Enqueueing in depth

`enqueue` takes optional fields beside the payload, and returns a result rather than throwing:

```ts
import { db } from './db';
import { welcomeEmail } from './jobs';
import { outboxFor } from './outbox';
import { insertUser } from './users';

// `env` and `requestId` come from your fetch handler.
const outbox = outboxFor(env);

const user = await outbox.transaction(db, async (tx, enqueue) => {
  const user = await insertUser(tx, input);

  const job = await enqueue(welcomeEmail, {
    payload: { userId: user.id },
    dedupeKey: `welcome:${user.id}`, // unique per tenant
    tenantKey: user.orgId, // scopes dedupe and operator actions
    correlationId: requestId, // find related jobs later
    delayMs: 60_000, // run no earlier than a minute from now
    metadata: { source: 'signup' }, // passed to the handler as ctx.metadata
  });

  // A rejected job is a result, not a throw: throw it, or the user commits without its job.
  if (job.status !== 'created' && job.status !== 'exists') throw new Error(job.status);

  return user;
});
```

Or let the outbox throw: with `createOutbox({ …, strictEnqueue: true })`, `enqueue`, `transaction`'s `enqueue` and a `run` schedule's `enqueue` throw `EnqueueRefusedError` (its `result` is the refusal) instead, so the business write rolls back with it. The result is then typed as created or exists, and the `StrictOutbox` it returns is accepted wherever an `Outbox` is (`runUntilIdle`, `createQueueHandler`, `createScheduledHandler`):

```ts
import { createOutbox, EnqueueRefusedError } from '@makerkit/workers-outbox';
import { createPgConnector } from '@makerkit/workers-outbox/pg';

import { db } from './db';
import { welcomeEmail } from './jobs';
import { createMailer } from './mailer';
import { insertUser } from './users';

const strict = createOutbox({
  jobs: [welcomeEmail],
  deps: { mailer: createMailer(env) },
  connect: createPgConnector(env.OUTBOX_DB.connectionString),
  strictEnqueue: true,
});

try {
  await strict.transaction(db, async (tx, enqueue) => {
    const user = await insertUser(tx, input);
    const job = await enqueue(welcomeEmail, {
      payload: { userId: user.id },
      dedupeKey: `welcome:${user.id}`,
    });

    return job.id; // `created` or `exists`: a refusal threw and rolled back the user
  });
} catch (error) {
  if (error instanceof EnqueueRefusedError) console.error(error.result.status);
  throw error;
}
```

In a `run` schedule, a strict refusal fails the whole occurrence (rolled back, `schedule_failed`) where a lenient one counts it as `rejected` and commits the rest.

`enqueue` returns one of:

- `{ status: 'created' | 'exists', id, kind }`. A repeated `dedupeKey` returns the existing job.
- `{ status: 'dedupe_conflict', reason: 'kind', existingKind }`. The key exists for a different job kind.
- `{ status: 'invalid_payload', issues }`. The payload failed its schema, or is over `maxPayloadBytes` (default 256 KiB) as JSON: store large data elsewhere (e.g. R2) and enqueue its key.

Pass `outbox.transaction` the top-level `db`, not a transaction: inside a `tx`, drizzle opens a savepoint, and the nudge would go out before the outer commit.

**Already inside a transaction you don't own?** Call `outbox.enqueue(tx, …)` directly and nudge after commit yourself. If the nudge is forgotten or lost, the job still runs, minutes later, on the cron sweep:

```ts
// `ctx` is the fetch handler's ExecutionContext; `user` is already committed by your code.
const result = await db.transaction((tx) =>
  outbox.enqueue(tx, welcomeEmail, {
    payload: { userId: user.id },
    dedupeKey: `welcome:${user.id}`,
  }),
);

if (result.status === 'created') {
  ctx.waitUntil(outbox.nudge([{ id: result.id, kind: result.kind }])); // never throws
}
```

## Completing atomically

A handler's external call and its bookkeeping often belong together: once the email is sent, mark the user as welcomed. Return the write from `ok({ commit })`. It runs in the same transaction that marks the job `succeeded`, so the two commit together or not at all:

```ts
// src/jobs.ts
import { defineJobFor, ok, retry } from '@makerkit/workers-outbox';
import { z } from 'zod';

import type { Mailer } from './mailer';

import { markWelcomed } from './users';

const defineJob = defineJobFor<{ mailer: Mailer }>();

export const welcomeEmail = defineJob({
  kind: 'email.welcome',
  payload: z.object({ userId: z.string() }),
  timeoutMs: 30_000,
  async handle({ userId }, ctx) {
    const res = await ctx.deps.mailer.send(userId, {
      signal: ctx.signal,
      idempotencyKey: ctx.idempotencyKey,
    });

    if (res.rateLimited) return retry({ code: 'rate_limited', after: res.retryAfterMs });

    // Same transaction as the `succeeded` transition. If it throws, both roll back and the
    // error is classified like a handler throw, so the send may repeat: keep it idempotent.
    return ok({ commit: (tx) => markWelcomed(tx, userId) });
  },
});
```

For reactions to _any_ transition, such as flagging a user when their job dead-letters, use the outbox-wide `onTransition` hook. It also runs inside the settle transaction:

```ts
// src/outbox.ts
import { createOutbox } from '@makerkit/workers-outbox';
import { createPgConnector } from '@makerkit/workers-outbox/pg';

import { welcomeEmail } from './jobs';
import { createMailer } from './mailer';
import { flagUser } from './users';

export const outboxFor = (env: Env) =>
  createOutbox({
    jobs: [welcomeEmail],
    deps: { mailer: createMailer(env) },
    connect: createPgConnector(env.OUTBOX_DB.connectionString),
    queue: env.OUTBOX_QUEUE,
    onTransition: async (tx, { job, transition }) => {
      if (transition.to === 'dead') {
        await flagUser(tx, job.id);
      }
    },
  });
```

A hook that throws rolls the transition back and retries the job with code `hook_failed`, which spends its retry budget, so a hook that always throws ends the job dead rather than looping. Keep hooks to quick database writes: no external calls inside the transaction.

**Chaining jobs.** For multi-step work, have each step's handler do one thing, and enqueue the next step from the request that starts the chain, or from your own code once a step's result is committed. Each step gets its own retries, timeout and dedupe key.

## Long work: stopping at the deadline

An attempt that is still running at `timeoutMs` is settled as `timeout`, and whatever it returns later is discarded: `ctx.signal` aborts only then, to stop abandoned I/O. To work in slices, stop _before_ the limit and hand the rest to a continuation. `ctx.deadline` is the instant (epoch ms) the attempt times out and `ctx.remainingMs()` the time left, both on the outbox's clock. A test clock moves them only when the test advances it; the timeout itself is real time, so a handler looping on `remainingMs()` under a frozen clock runs until the real `timeoutMs` and times out. Check it before each unit of work, keeping a margin for the slowest unit and the return:

```ts
// src/jobs.ts
import {
  createOutbox,
  defineJobFor,
  ok,
  type HandlerOutcome,
  type Outbox,
} from '@makerkit/workers-outbox';
import { createPgConnector } from '@makerkit/workers-outbox/pg';
import { z } from 'zod';

import { deleteExpiredBatch } from './retention';

// The outbox comes through deps, as a getter: it is created after its jobs.
const defineJob = defineJobFor<{ getOutbox: () => Outbox }>();

const BATCH = 1_000;
const MARGIN_MS = 10_000; // the slowest batch, plus room to return

export const purgeExpired = defineJob({
  kind: 'retention.purge',
  payload: z.object({ day: z.string(), step: z.number().int() }),
  timeoutMs: 60_000,
  async handle({ day, step }, ctx): Promise<HandlerOutcome> {
    let deleted = BATCH;

    // ctx.db is fenced: once the attempt has timed out, a straggling batch can't commit.
    while (deleted === BATCH && ctx.remainingMs() > MARGIN_MS) {
      deleted = await deleteExpiredBatch(ctx.db, BATCH);
    }

    if (deleted < BATCH) return ok(); // done

    // Out of time with rows left: the next step commits together with this one's success.
    return ok({
      commit: async (tx) => {
        const next = await ctx.deps.getOutbox().enqueue(tx, purgeExpired, {
          payload: { day, step: step + 1 },
          dedupeKey: `purge:${day}:${step + 1}`,
        });

        if (next.status !== 'created' && next.status !== 'exists') throw new Error(next.status);
      },
    });
  },
});

export const outboxFor = (env: Env) => {
  const outbox: Outbox = createOutbox({
    jobs: [purgeExpired],
    deps: { getOutbox: () => outbox },
    connect: createPgConnector(env.OUTBOX_DB.connectionString),
    queue: env.OUTBOX_QUEUE,
  });

  return outbox;
};
```

A job enqueued in `commit` isn't nudged, so it runs on the next cron sweep. To start it at once, enqueue it with `ctx.deps.getOutbox().transaction(ctx.db, …)` before returning `ok()` instead: the step's dedupe key makes a retried step find the same continuation.

`ctx.db` is the lane's own session: the outbox's `connect`, role and Hyperdrive config, not your app's database. It is typed as the minimal `OutboxDb`; the object is your connector's `db` (a drizzle database with the `./pg` connectors). Once the attempt returns or times out, every call on it, on a `tx` from its `transaction()` or on a captured method rejects; a query builder created before then and awaited after it is not covered. Never await an external call inside `ctx.db.transaction()`.

## Fine-grained retry rules

`retry.perCode` tunes the policy per error code. A number caps how many times in a row one code may occur; an object can also widen the attempt budget for that code, cap its delay, or cancel instead of dead-lettering when it runs out:

```ts
import { cancel, defineJobFor, fail, ok, retry } from '@makerkit/workers-outbox';
import { z } from 'zod';

import type { Mailer } from './mailer';

import { isHttp4xx } from './http';

const defineJob = defineJobFor<{ mailer: Mailer }>();

export const welcomeEmail = defineJob({
  kind: 'email.welcome',
  payload: z.object({ userId: z.string(), plan: z.enum(['free', 'pro']) }),
  timeoutMs: 30_000,
  retry: {
    maxAttempts: 10,
    perCode: {
      // Throttling isn't failure: allow up to 50 attempts, at most 10 minutes apart.
      rate_limited: { maxAttempts: 50, maxDelayMs: 600_000 },
      // Three timeouts in a row means the provider is down for this job: dead-letter it.
      timeout: 3,
      // A user who keeps bouncing gets 2 tries, then the job is cancelled, not dead-lettered.
      bounced: { streak: 2, onExhaustion: 'cancel' },
    },
    maxCrashes: 3, // consecutive isolate deaths mid-attempt before `dead(crash_loop)`
  },
  classify: (err) => (isHttp4xx(err) ? fail({ code: 'http_4xx' }) : retry({ code: 'unexpected' })),
  async handle({ userId, plan }, ctx) {
    if (plan === 'free' && ctx.attempt > 5) {
      return cancel({ reason: 'not_worth_retrying' });
    }

    const res = await ctx.deps.mailer.send(userId, {
      signal: ctx.signal,
      idempotencyKey: ctx.idempotencyKey,
    });

    if (res.rateLimited) return retry({ code: 'rate_limited', after: res.retryAfterMs });

    return ok();
  },
});
```

Budgets count attempts since the last [replay](#operate), and crashes are charged only to `maxCrashes`, never to `maxAttempts`. The defaults cap `timeout` at 5 and `invalid_payload` at 3 in a row; your `perCode` merges over them.

## Scheduled and recurring jobs

**Run once, later.** `enqueue(job, { delayMs })` schedules a job for any time in the future: a reminder two weeks before a trial ends, a follow-up a day after signup. `outbox.transaction` sends its nudge with the same delay, so a job due within 24 hours starts within seconds of its due time. A longer delay is past what Queues can hold, so that job runs on the first cron sweep after it's due: up to 5 minutes late with the default `*/5` trigger. So does a job you enqueue with `outbox.enqueue` and nudge yourself, since `outbox.nudge` sends immediately and an early nudge is dropped.

**Run on a schedule.** List your recurring jobs in `createScheduledHandler({ schedules })` and add their cron expressions to the Worker. When a tick's cron matches a schedule, the handler enqueues that occurrence's job, then runs the usual sweep:

```ts
import { createOutbox, defineJob, ok } from '@makerkit/workers-outbox';
import {
  createQueueHandler,
  createScheduledHandler,
  defineSchedule,
} from '@makerkit/workers-outbox/cloudflare';
import { createPgConnector } from '@makerkit/workers-outbox/pg';
import { z } from 'zod';

import { app } from './app';
import { isoWeek } from './dates';
import { sendNewsletter } from './newsletter';

export const weeklyNewsletter = defineJob({
  kind: 'newsletter.weekly',
  payload: z.object({ week: z.string() }), // e.g. '2026-W45'
  timeoutMs: 120_000,
  async handle({ week }, ctx) {
    await sendNewsletter(week, { signal: ctx.signal, idempotencyKey: ctx.idempotencyKey });

    return ok();
  },
});

export const outboxFor = (env: Env) =>
  createOutbox({
    jobs: [weeklyNewsletter],
    connect: createPgConnector(env.OUTBOX_DB.connectionString),
    queue: env.OUTBOX_QUEUE,
  });

export default {
  fetch: app.fetch,
  queue: createQueueHandler(outboxFor),
  scheduled: createScheduledHandler(outboxFor, {
    schedules: [
      defineSchedule({
        cron: '0 9 * * MON', // must match an entry in triggers.crons exactly
        job: weeklyNewsletter,
        payload: ({ scheduledAt }) => ({ week: isoWeek(scheduledAt) }), // type-checked
      }),
    ],
  }),
};
```

```jsonc
// wrangler.jsonc: cron expressions run in UTC
{ "triggers": { "crons": ["*/5 * * * *", "0 9 * * MON"] } }
```

Each occurrence's job is keyed by the tick's scheduled time (`schedule:<name>:<scheduledAt>`, where `name` defaults to the job's kind), so a tick delivered twice enqueues nothing new; pass `dedupeKey` to choose your own. The job is nudged when it's enqueued, so it starts within seconds of the trigger, and from then on it's an ordinary job, with retries, a dead-letter path and operator actions.

**Fan out** with `run` instead of `job` and `payload`: it gets a transaction and a typed `enqueue`, and everything it enqueues commits together. Derive each dedupe key from `scheduledAt` so a repeated tick finds the jobs it already made:

```ts
import { defineJob, ok } from '@makerkit/workers-outbox';
import { defineSchedule } from '@makerkit/workers-outbox/cloudflare';
import { z } from 'zod';

import { syncInsights } from './insights';
import { listShops } from './shops';

export const insightsSync = defineJob({
  kind: 'insights.sync',
  payload: z.object({ shopId: z.string(), day: z.string() }),
  timeoutMs: 60_000,
  async handle({ shopId, day }, ctx) {
    await syncInsights(shopId, day, { signal: ctx.signal });

    return ok();
  },
});

export const nightlyInsights = defineSchedule({
  cron: '0 2 * * *',
  name: 'insights', // required for run schedules
  async run({ scheduledAt, tx, enqueue }) {
    const day = scheduledAt.toISOString().slice(0, 10);

    for (const shop of await listShops(tx)) {
      await enqueue(insightsSync, {
        payload: { shopId: shop.id, day },
        dedupeKey: `insights:${shop.id}:${day}`,
        tenantKey: shop.id,
      });
    }
  },
});
```

`run` executes on the outbox's connection, so reading app tables (like `listShops` above) needs grants for the outbox role. For a large mailing list, the same pattern enqueues one job per batch of subscribers, so each batch retries on its own and a failure doesn't resend to everyone.

What to expect:

- **Minute-level precision**, not second-level: a trigger can fire a little late, and a retry runs after its backoff. Fine for newsletters, digests and nightly syncs; the wrong tool for anything that must happen at an exact second.
- **Transient database errors are retried** within the tick. Anything else, like a throw from `run` or a rejected single-job enqueue, rolls the occurrence back and emits `schedule_failed` (severity `error`); other schedules on the same tick and the sweep still run.
- **A missed tick is skipped, not caught up.** If Cloudflare doesn't fire a trigger, or every retry fails, that occurrence doesn't run. Alert on `schedule_failed`, and re-run by hand with `enqueue` if it matters.
- **Every tick runs the sweep**, including schedule ticks. That's cheap, and it means one `scheduled` export serves both.

## Lanes: isolating slow jobs

By default every job shares one queue and one consumer, so a burst of slow jobs delays fast ones. A **lane** gives a group of kinds its own queue and consumer. Here, five-minute exports run on `bulk` and can't hold up welcome emails:

```ts
import { createOutbox, defineJob, ok } from '@makerkit/workers-outbox';
import {
  createDlqHandler,
  createQueueHandler,
  createScheduledHandler,
  type QueueBatch,
} from '@makerkit/workers-outbox/cloudflare';
import { createPgConnector } from '@makerkit/workers-outbox/pg';
import { z } from 'zod';

import { app } from './app';
import { buildExport } from './exports';
import { welcomeEmail } from './jobs';
import { createMailer } from './mailer';

export const orgExport = defineJob({
  kind: 'org.export',
  lane: 'bulk', // jobs without a lane run on 'default'
  payload: z.object({ orgId: z.string() }),
  timeoutMs: 300_000, // at most 10 minutes
  retry: { maxAttempts: 5 },
  async handle({ orgId }, ctx) {
    await buildExport(orgId, { signal: ctx.signal });

    return ok();
  },
});

export const outboxFor = (env: Env) =>
  createOutbox({
    jobs: [welcomeEmail, orgExport],
    deps: { mailer: createMailer(env) },
    connect: createPgConnector(env.OUTBOX_DB.connectionString),
    queue: { default: env.OUTBOX_QUEUE, bulk: env.OUTBOX_BULK_QUEUE }, // one queue per lane
    events: (e) => console.log(JSON.stringify(e)),
  });

const mail = createQueueHandler(outboxFor); // claims only 'default' kinds
const bulk = createQueueHandler(outboxFor, { lane: 'bulk' }); // claims only 'bulk' kinds
const dlq = createDlqHandler({ events: (e) => console.log(JSON.stringify(e)) });

export default {
  fetch: app.fetch,
  queue: (batch: QueueBatch, env: Env, ctx: ExecutionContext) => {
    if (batch.queue === 'outbox-dlq' || batch.queue === 'outbox-bulk-dlq') return dlq(batch);

    return batch.queue === 'outbox-bulk' ? bulk(batch, env, ctx) : mail(batch, env, ctx);
  },
  scheduled: createScheduledHandler(outboxFor, {
    // Swept before the catch-all, so a backlog of other kinds can't starve signups.
    reserved: [
      { name: 'mail', kinds: ['email.welcome'], budget: { maxJobs: 50, deadlineMs: 45_000 } },
    ],
    retention: { succeededRetentionMs: 3 * 86_400_000 }, // default 7 days; dead/cancelled 90
  }),
};
```

```jsonc
// wrangler.jsonc (queues)
{
  "queues": {
    "producers": [
      { "binding": "OUTBOX_QUEUE", "queue": "outbox" },
      { "binding": "OUTBOX_BULK_QUEUE", "queue": "outbox-bulk" },
    ],
    "consumers": [
      { "queue": "outbox", "dead_letter_queue": "outbox-dlq", "max_concurrency": 2 },
      { "queue": "outbox-bulk", "dead_letter_queue": "outbox-bulk-dlq", "max_concurrency": 1 },
      { "queue": "outbox-dlq" },
      { "queue": "outbox-bulk-dlq" },
    ],
  },
}
```

Two names are easy to confuse: `defineJob({ lane })` is a named routing lane, while `createOutbox({ lanes })` is how many Postgres connections one invocation may hold (default 1). The cron sweep still covers every lane, so a lost nudge is caught either way. A job whose lease is longer than the sweep's 180 s budget, like the export above, isn't run by cron: the sweep hands it to its lane's queue instead.

Every consumer invocation holds connections, so peak outbox connections are about `lanes × (sum of max_concurrency) + lanes` for cron. Here that's 1 × (2 + 1) + 1 = 4. Keep it under your Hyperdrive origin limit and Postgres `max_connections`, after your app's own traffic.

## Testing

`./testing` runs the real outbox against an in-memory database:

```ts
import { createOutbox } from '@makerkit/workers-outbox';
import { createOutboxTestDatabase, runUntilIdle } from '@makerkit/workers-outbox/testing';
import { expect, it } from 'vitest';

import { welcomeEmail } from '../src/jobs';
import { fakeMailer } from './mailers'; // records each userId it "sends" to

it('sends the welcome email', async () => {
  const test = await createOutboxTestDatabase(); // pglite with the schema applied
  const sent: string[] = [];
  const outbox = createOutbox({
    jobs: [welcomeEmail],
    deps: { mailer: fakeMailer(sent) },
    connect: test.connect,
  });

  await outbox.transaction(test.db, (tx, enqueue) =>
    enqueue(welcomeEmail, { payload: { userId: 'u1' }, dedupeKey: 'welcome:u1' }),
  );

  expect(await runUntilIdle(outbox)).toMatchObject({ succeeded: 1 });
  expect(sent).toEqual(['u1']);
});
```

Without a clock, `runUntilIdle` runs only what is due now. To test retries and delays, give the outbox a test clock; `runUntilIdle` then skips ahead to each next due job:

```ts
import { createOutbox } from '@makerkit/workers-outbox';
import {
  createOutboxTestDatabase,
  createTestClock,
  runUntilIdle,
} from '@makerkit/workers-outbox/testing';
import { expect, it } from 'vitest';

import { welcomeEmail } from '../src/jobs';
import { flakyMailer } from './mailers'; // throws on the first `failures` sends

it('retries until the provider recovers', async () => {
  const test = await createOutboxTestDatabase();
  const clock = createTestClock();
  const outbox = createOutbox({
    jobs: [welcomeEmail],
    deps: { mailer: flakyMailer({ failures: 2 }) },
    connect: test.connect,
    clock,
  });

  await outbox.transaction(test.db, (tx, enqueue) =>
    enqueue(welcomeEmail, { payload: { userId: 'u1' }, dedupeKey: 'welcome:u1' }),
  );

  const report = await runUntilIdle(outbox, { clock, db: test.db });

  expect(report).toMatchObject({ retrying: 2, succeeded: 1 });
});
```

`runUntilIdle` throws after `maxRounds` (default 100), so a job that retries forever fails the test instead of hanging it. pglite is a single session, so it can't reproduce multi-connection races; this repo's real-Postgres tests run with `pnpm db:up && pnpm test:pg`.

## Production setup

The quick start runs; these make it resilient. The [setup checklist](examples/worker/README.md#setup-checklist) lists them all, since missing pieces degrade quietly rather than fail.

**A dead-letter queue.** Messages that keep failing land there; the job row stays durable and the next sweep re-drives it. One `queue()` export serves both consumers:

```ts
import {
  createDlqHandler,
  createQueueHandler,
  createScheduledHandler,
  type QueueBatch,
} from '@makerkit/workers-outbox/cloudflare';

import { app } from './app';
import { outboxFor } from './outbox';

const queue = createQueueHandler(outboxFor); // nudge → targeted claim; drain → bounded sweep
const dlq = createDlqHandler({ events: (e) => console.log(JSON.stringify(e)) });

export default {
  fetch: app.fetch,
  queue: (batch: QueueBatch, env: Env, ctx: ExecutionContext) =>
    batch.queue === 'outbox-dlq' ? dlq(batch) : queue(batch, env, ctx),
  scheduled: createScheduledHandler(outboxFor), // recover → drain → cleanup
};
```

```jsonc
// wrangler.jsonc
{
  "compatibility_flags": ["nodejs_compat"],
  "hyperdrive": [{ "binding": "OUTBOX_DB", "id": "<caching-disabled config>" }],
  "queues": {
    "producers": [{ "binding": "OUTBOX_QUEUE", "queue": "outbox" }],
    "consumers": [
      { "queue": "outbox", "dead_letter_queue": "outbox-dlq", "max_concurrency": 2 },
      { "queue": "outbox-dlq" },
    ],
  },
  "triggers": { "crons": ["*/5 * * * *"] },
  "version_metadata": { "binding": "CF_VERSION_METADATA" }, // pass as `version`
}
```

**A dedicated Hyperdrive config** with caching disabled, for the outbox only.

**Roles and session guardrails.** The outbox runs as whatever role `OUTBOX_DB` connects as. **Required:** set `idle_in_transaction_session_timeout` on that role, so a transaction orphaned by a dead isolate can't hold its locks indefinitely. **Recommended:** make it a dedicated `outbox_worker` role, so its timeouts don't apply to your app's queries and it can only touch what it needs. Whichever role it is also runs your handlers, `onTransition` hooks and `ok({ commit })` writes, so it needs grants on the app tables they touch. Put settings on the role, not in a `SET`: under Hyperdrive's transaction pooling a session `SET` leaks to the next client. Run as the schema owner, after the migrations:

```sql
-- No password here, so none lands in logs or history, and the role can't log in until you set one:
-- `\password outbox_worker` in psql, or your provider's console.
CREATE ROLE outbox_worker LOGIN;

-- Replace app_role with the role your app connects as.
GRANT USAGE ON SCHEMA outbox TO outbox_worker, app_role;

-- The worker runs every outbox statement.
GRANT SELECT, INSERT, UPDATE, DELETE ON outbox.job, outbox.attempt TO outbox_worker;

-- The producer only enqueues (insert, then a re-read on a dedupe hit).
GRANT SELECT, INSERT ON outbox.job TO app_role;

-- Whatever your handlers, onTransition hooks and ok({ commit }) touch, for example:
-- GRANT SELECT, UPDATE ON public.users TO outbox_worker;

-- Required: ends a session left inside an open transaction, releasing its locks.
ALTER ROLE outbox_worker SET idle_in_transaction_session_timeout = '60s';

-- Recommended: a statement waiting on a lock fails instead of queueing behind it.
ALTER ROLE outbox_worker SET lock_timeout = '5s';

-- Optional backstop: no transaction on this role outlives a 15-minute invocation.
ALTER ROLE outbox_worker SET transaction_timeout = '15min';
```

Role settings apply to new sessions only; Hyperdrive's existing origin connections keep the old values until they're recycled. A later migration that adds a table needs the grants re-run, or set `ALTER DEFAULT PRIVILEGES FOR ROLE <owner> IN SCHEMA outbox GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO outbox_worker`.

**A schema check at deploy.** `OUTBOX_MIGRATIONS` refuses a server older than PostgreSQL 17, but with drizzle-kit nothing checks the version. Run `verifyOutboxSchema` in CI or a release step and fail on any result:

```ts
import { verifyOutboxSchema } from '@makerkit/workers-outbox/drizzle';

import { db } from './db';

// [] when they match; otherwise missing or changed columns, constraints and indexes,
// invalid indexes (a failed CREATE INDEX CONCURRENTLY), or an unsupported server version.
const drift = await verifyOutboxSchema(db);

if (drift.length > 0) {
  throw new Error(`outbox schema: ${JSON.stringify(drift)}`);
}
```

Not using drizzle-kit? Apply `OUTBOX_MIGRATIONS` from `./sql`, one statement at a time, in order.

**Time limits.** Each invocation has one deadline shared by everything it awaits: claims, handlers, settlements, recovery, cleanup, queue sends and connection acquisition. The queue and scheduled handlers take `invocationBudgetMs` (default 840 s, under Cloudflare's 15-minute limit); the scheduled handler's `recoveryBudgetMs` (default 120 s) caps recovery's share, so a crash backlog can't starve fresh work, and is held back from `schedules`, so slow schedules can't starve recovery. A job whose claim arrives too late to run safely never starts: its attempt closes as `claim_late`, charging neither an attempt nor a crash, and it retries in 5 s.

## Performance

Each lane runs one job at a time on one Postgres connection: claim, handler, settle, next. A job costs about five database round trips besides the handler (one claim statement, and a settle transaction of four). Throughput is therefore set by connections and handler time:

```
jobs/min ≈ 60 × outbox connections ÷ (handler time + 5 × DB round trip)
```

Outbox connections are `lanes × (sum of the consumers' max_concurrency) + lanes` for cron: 3 with the defaults (`lanes: 1`, `max_concurrency: 2`).

Measured with `pnpm test:load` on a laptop, against PostgreSQL 17 in local Docker limited to 2 CPUs, one run each. Local round trips are far cheaper than Hyperdrive's, so treat these as upper bounds per connection, not Cloudflare figures:

| Handler | Connections | jobs/s  | Per connection |
| ------- | ----------- | ------- | -------------- |
| 2 ms    | 1           | 113–161 | 113–161        |
| 2 ms    | 8           | 919     | 115            |
| 200 ms  | 32          | 135     | 4.2 (~250/min) |

- **Scaling is near-linear in connections** up to 8 in these runs, with latency flat. Past that, the 2-CPU database was the limit, so higher counts are unmeasured.
- **Slow handlers dominate.** With a 200 ms handler, each connection does about 250 jobs/min whatever the database, so three default connections handle roughly 750 jobs/min, or about a million a day.
- **To go faster,** raise the queue consumers' `max_concurrency` or `lanes`, and split slow kinds onto their own [lane](#lanes-isolating-slow-jobs). Each step spends connections, so stay under your Hyperdrive origin limit and Postgres `max_connections`, after your app's own traffic.
- **Whose limits these are.** The connection budget, round-trip cost and per-job write churn come with any transactional outbox on Postgres over Hyperdrive. Running one job at a time per connection is this library's choice: it gives each lane an exclusive session, so settle transactions can never interleave. That choice is what ties throughput to connections when handlers are slow.
- **Sustained volume** also depends on autovacuum and [retention](#lanes-isolating-slow-jobs) keeping the `outbox` tables small: every job is an insert, a few updates and, after retention, a delete.

To size it for your workload, run the load test against a staging Postgres server with your handlers' typical latency. It creates and drops its own throwaway database, so the role needs `CREATEDB`; never point it at production:

```sh
OUTBOX_LOAD=1 OUTBOX_LOAD_HANDLER_MS=200 OUTBOX_PG_URLS=postgres://… pnpm vitest run src/load --testTimeout=600000
```

In production, `aggregateAttemptOutcomes` from `./inspect` reports real attempt durations.

## Operate

```ts
import { outboxFor } from './outbox';

// `env` from your handler; `jobId`, `jobIds` and `user` from the request.
const outbox = outboxFor(env);

await outbox.cancel({ id: jobId }, 'user_deleted');
await outbox.replay(jobId); // dead/cancelled → fresh retry budget
await outbox.retryNow(jobId); // make a pending/retrying job due now, and nudge it
await outbox.cancelMany(jobIds, 'reason'); // also: replayMany
await outbox.replay(jobId, { tenantKey: user.orgId }); // only if it belongs to this tenant
await outbox.eraseTenant(user.orgId); // deletes every job of one tenant
```

The optional `{ tenantKey }` scope reports out-of-scope jobs as `not_found`. Mind the difference: an omitted `tenantKey` means _any_ tenant, `null` means only jobs enqueued without one. Pass the tenant explicitly whenever an action is on a tenant's behalf.

Read-only inspection lives in `./inspect`: `getJob`, `listJobs`, `countJobs`, `findJobsByCorrelationId`, `countBacklog`, `aggregateAttemptOutcomes`, plus `evaluateBacklog` to grade a backlog count against thresholds. Each query runs in a read-only transaction bounded by `statementTimeoutMs` (default 30 s), so a dashboard can't hold a connection indefinitely:

```ts
import { getJob, listJobs } from '@makerkit/workers-outbox/inspect';

import { db } from './db';

const job = await getJob(db, { id: jobId, tenantKey: user.orgId }); // null when not found
const dead = await listJobs(db, { status: 'dead', limit: 50, statementTimeoutMs: 5_000 });
```

## Best practices

**Enqueueing**

- **Derive `dedupeKey` from business identity**, such as `welcome:${user.id}` or `invoice.paid:${invoice.id}`, never from a random value. A retried request then finds its job instead of creating a second one. Keys are permanent per tenant, so to send something again on purpose, put the reason in the key (`reminder:${user.id}:${week}`).
- **Check the `enqueue` result and throw on a rejection,** or use `strictEnqueue: true`. `invalid_payload` and `dedupe_conflict` are returned, not thrown by default; ignore them and the business row commits without its job.
- **Pass the top-level `db` to `outbox.transaction`**, not a `tx`, or the nudge goes out before the real commit.
- **Validate at the edge with the job's own schema** (`welcomeEmail.payload`) so a bad request fails before it opens a transaction, as in [the example Worker](examples/worker/src/index.ts).
- **Enqueue references, not snapshots.** Put ids in the payload and read current state in the handler; store anything large in R2 and enqueue its key.

**Handlers**

- **Make every handler idempotent.** Delivery is at-least-once: pass `ctx.idempotencyKey` to providers that dedupe, or record what you've done and check it first.
- **Pass `ctx.signal` to every I/O call.** It fires once the attempt has been settled as `timeout`; a handler that ignores it keeps running after its attempt is already retried. A result after it is discarded: to stop early and still return `ok()`, watch `ctx.remainingMs()` ([long work](#long-work-stopping-at-the-deadline)).
- **Return outcomes for errors you understand.** `fail` for permanent errors (4xx, validation), `retry({ after })` for `Retry-After`, `cancel` when the work is moot. Leave `classify`'s default `retry` for the unexpected.
- **Set `timeoutMs` close to the real worst case.** The lease is derived from it, so an overgenerous timeout delays crash recovery, and a long lease can't run on the cron sweep.
- **Never await an external call inside a database transaction**, whether yours or the one in `ok({ commit })` and `onTransition`. Do the network call first, then commit the bookkeeping.
- **Build clients per invocation through `deps`**, never at module scope, so each invocation uses its own `env` and connections.

**Deploys**

- **Evolve payload schemas additively.** Jobs already in the table carry the old shape and are validated again when they run. Add optional fields; don't rename or remove required ones.
- **Keep a handler registered until its jobs have drained.** Claims are filtered to registered kinds, so jobs of a kind you removed sit pending forever.
- **Pass `version: env.CF_VERSION_METADATA`** so during a gradual rollout an older deploy leaves jobs from a newer one alone.
- **Run `verifyOutboxSchema` in CI** and fail the release on drift.

**Operations**

- **Send `events` somewhere you alert on.** `settle_failed`, `infra_error`, `nudge_skipped`, `schedule_failed` and dead-letters are how a broken setup shows up; nothing else will fail loudly.
- **Budget connections before raising `lanes` or `max_concurrency`.** Peak is about `lanes × (sum of max_concurrency) + lanes`; past the origin limit, both the outbox and your app suffer.
- **Scope operator actions to a tenant.** Pass `{ tenantKey }` whenever an action is on a tenant's behalf, and call `eraseTenant` when a tenant leaves.
- **Use `correlationId`** (a request or trace id) so you can find every job a request produced.

## Configuration

Everything beyond `jobs` and `connect` is optional:

```ts
import { createOutbox } from '@makerkit/workers-outbox';
import { createPgConnector } from '@makerkit/workers-outbox/pg';

import { welcomeEmail } from './jobs';
import { createMailer } from './mailer';
import { flagUser } from './users';

export const outboxFor = (env: Env) =>
  createOutbox({
    jobs: [welcomeEmail],
    deps: { mailer: createMailer(env) },
    connect: createPgConnector(env.OUTBOX_DB.connectionString),
    queue: env.OUTBOX_QUEUE,
    version: env.CF_VERSION_METADATA, // gradual rollouts: older deploys skip newer jobs
    events: (e) => console.log(JSON.stringify(e)), // structured logs and metrics
    onTransition: async (tx, { job, transition }) => {
      // Same transaction as the job row write: both commit, or neither does.
      if (transition.to === 'dead') {
        await flagUser(tx, job.id);
      }
    },
  });
```

| Option               | Purpose                                                                                                                                                                |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `jobs`, `connect`    | Required. Registered jobs and a connection factory.                                                                                                                    |
| `deps`               | Passed to handlers as `ctx.deps`. Required, and type-checked, once any job declares deps.                                                                              |
| `queue`              | One Queue, or one per lane. Without it, jobs run on the cron sweep only.                                                                                               |
| `lanes`              | Connections per invocation. Default 1.                                                                                                                                 |
| `version`            | e.g. `env.CF_VERSION_METADATA`. Gates newer jobs from older deploys during a rollout.                                                                                  |
| `events`             | Structured event sink. Misconfigurations surface here (`nudge_skipped`, `settle_failed`, `infra_error`).                                                               |
| `onTransition`       | Hook run inside the settle transaction. A throw rolls back and retries the job.                                                                                        |
| `redactError`        | Scrub error messages before they are persisted.                                                                                                                        |
| `warnAtStreak`       | Emit an event when the same error code repeats this many times.                                                                                                        |
| `recordNudges`       | Record nudge results on the row (on the lane's idle connection when it holds one). Cron handoff nudges are always recorded: their cooldown reads `nudge_sent_at`.      |
| `operationTimeoutMs` | Bound for one operator action (`cancel`, `replay`, `retryNow`, …): acquisition, SQL and close. Past it the connection is destroyed and the call rejects. Default 60 s. |
| `maxPayloadBytes`    | Largest payload accepted, as JSON bytes. Default 256 KiB; larger ones return `invalid_payload`.                                                                        |
| `strictEnqueue`      | `true`: enqueues throw `EnqueueRefusedError` instead of returning `invalid_payload` / `dedupe_conflict`, and `createOutbox` returns a `StrictOutbox`. Default `false`. |
| `generateId`         | Job id generator. Default `uuidv7()`. Must return a UUID; prefer time-ordered ids for index locality.                                                                  |
| `clock`              | For tests: see [Testing](#testing).                                                                                                                                    |

### Connections

`./pg` ships two node-postgres connectors:

- `createPgConnector(connectionString, { connectionTimeoutMs?, onError? })`: a fresh `pg.Client` per lane, the shape Hyperdrive expects. Connection timeout defaults to 10 s.
- `createPgPoolConnector(pool, { onError? })`: checks clients out of a pool you own. Set the pool's `connectionTimeoutMillis`, and attach `pool.on('error', …)`: node-pg re-emits a returned client's late errors on the pool, and an unlistened one crashes the process.

Both catch errors a session raises outside a query (such as the FATAL `transaction_timeout` sends) instead of crashing the Worker, report the session as broken so the outbox replaces it, and end a lane idempotently. The pool connector destroys a broken client rather than returning it to the pool.

Writing your own? `connect` returns a `LaneConnection` the outbox owns until it calls exactly one of:

- `close()`: the session is healthy; return it (e.g. `client.release()`).
- `destroy()`: the session must never be reused (a runaway handler, a settlement that hit its deadline); discard it (e.g. `client.release(true)` or `client.end()`). It is called _before_ a replacement is acquired, so a one-client pool can reconnect. It must be idempotent, and make a later `close()` a no-op.

Optionally implement `isBroken()`, true once the session is known dead (e.g. its socket errored while idle): the outbox then replaces the lane before its next claim or settlement instead of failing a round trip first. Listen for the client's `'error'` event before connecting, and bound acquisition. [`src/pg/index.ts`](src/pg/index.ts) is a short reference.

## Entry points

| Entry point    | Contents                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------- |
| `.`            | `defineJob`, `defineJobFor`, `createOutbox`, `ok` / `retry` / `fail` / `cancel`                         |
| `./cloudflare` | `createQueueHandler`, `createScheduledHandler` (with `schedules`), `createDlqHandler`, `defineSchedule` |
| `./pg`         | `createPgConnector` (Hyperdrive), `createPgPoolConnector`                                               |
| `./drizzle`    | Drizzle tables, `verifyOutboxSchema`                                                                    |
| `./sql`        | `OUTBOX_MIGRATIONS` for non-drizzle-kit setups                                                          |
| `./inspect`    | Read-only operator queries, backlog evaluation                                                          |
| `./testing`    | `createOutboxTestDatabase` (pglite), `createTestClock`, `runUntilIdle`                                  |

## Development

```sh
pnpm install
pnpm test          # vitest + pglite
pnpm typecheck
pnpm check:readme  # typechecks every ts block in this README
pnpm db:up         # PostgreSQL 17 in Docker, for the next three
pnpm test:pg       # the suite against real Postgres
pnpm test:load     # concurrency and recovery under load (OUTBOX_LOAD_JOBS, _WORKERS, _LANES, _CRASHED)
pnpm test:example  # examples/worker under wrangler dev (needs wrangler; WRANGLER=/path)
pnpm oxlint
pnpm format
pnpm build         # rolldown → dist/
```

## License

MIT
