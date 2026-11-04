// Load checks against real PostgreSQL: many concurrent "invocations" (outboxes) with several
// lanes each, sharing one pool, over thousands of jobs. Correctness is asserted strictly; the
// throughput numbers are printed, not asserted, since they depend on the machine.
//
//   pnpm db:up && pnpm test:load            # defaults below
//   OUTBOX_LOAD_JOBS=20000 OUTBOX_LOAD_WORKERS=16 pnpm test:load
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import { defineJob, fail, ok, retry } from '../index';
import { createPgPoolConnector } from '../pg/index';
import { createOutbox, RUNTIME } from '../runtime/create-outbox';
import { claimDue } from '../store/outbox-store';
import { pgSuiteDrivers, pgUrls, type PgDriver } from '../test-support/drivers';

const enabled = process.env['OUTBOX_LOAD'] === '1' && pgUrls().length > 0;
const num = (name: string, fallback: number) => Number(process.env[name] ?? fallback);

const JOBS = num('OUTBOX_LOAD_JOBS', 5_000);
const WORKERS = num('OUTBOX_LOAD_WORKERS', 8);
const LANES = num('OUTBOX_LOAD_LANES', 4);
const HANDLER_MS = num('OUTBOX_LOAD_HANDLER_MS', 2);
const CRASHED = num('OUTBOX_LOAD_CRASHED', 5_000);
const PRODUCERS = 16;

const sleep = (ms: number) => new Promise((done) => setTimeout(done, ms));
const seconds = (ms: number) => `${(ms / 1_000).toFixed(2)} s`;

/** Counts events by type; correctness checks read the ones that must never happen. */
function eventCounter() {
  const counts = new Map<string, number>();

  return {
    sink: (event: { type: string }) => counts.set(event.type, (counts.get(event.type) ?? 0) + 1),
    count: (type: string) => counts.get(type) ?? 0,
    all: () => Object.fromEntries(counts),
  };
}

describe.skipIf(!enabled).each(pgSuiteDrivers())('load on $name', ({ create }) => {
  let driver: PgDriver;
  let pool: Pool;
  const summary: Record<string, Record<string, string | number>> = {};

  beforeAll(async () => {
    driver = await create();
    // Every worker's lanes share it, like isolates sharing one database; plus room to observe.
    pool = new Pool({ ...driver.pool.options, max: WORKERS * LANES + 4 });
  });

  beforeEach(() => driver.reset());

  afterAll(async () => {
    // Straight to stdout: vitest's console capture can drop output from afterAll.
    for (const [scenario, row] of Object.entries(summary)) {
      process.stdout.write(`\n${scenario}\n`);

      for (const [key, value] of Object.entries(row)) {
        process.stdout.write(`  ${key.padEnd(26)} ${value}\n`);
      }
    }
    await pool?.end();
    await driver?.close();
  });

  const query = async <T>(text: string, values: unknown[] = []) =>
    (await driver.pool.query(text, values)).rows as T[];

  const statusCounts = async () =>
    Object.fromEntries(
      (
        await query<{ status: string; n: number }>(
          `SELECT status, count(*)::int AS n FROM "outbox"."job" GROUP BY status`,
        )
      ).map((r) => [r.status, r.n]),
    ) as Record<string, number>;

  const unfinished = async () =>
    (
      await query<{ n: number }>(
        `SELECT count(*)::int AS n FROM "outbox"."job" WHERE status IN ('pending', 'retrying', 'processing')`,
      )
    )[0]!.n;

  /** Runs `workers` concurrent drain loops until nothing is left, sampling backend counts. */
  async function drainAll(outboxes: ReturnType<typeof createOutbox>[]) {
    // The workers' own pool: the producers and these checks use a separate one.
    let peakConnections = 0;
    const sampling = { done: false };
    const sampler = (async () => {
      while (!sampling.done) {
        peakConnections = Math.max(peakConnections, pool.totalCount);
        await sleep(10);
      }
    })();

    const started = Date.now();
    const reports = await Promise.all(
      outboxes.map(async (outbox) => {
        let claimed = 0;

        for (;;) {
          const report = await outbox[RUNTIME].drain(
            { maxJobs: 500, deadlineMs: 120_000 },
            'sweep',
          );
          claimed += report.claimed;

          if (report.claimed === 0) {
            if ((await unfinished()) === 0) {
              return claimed;
            }

            await sleep(20); // another worker holds the rest, or a retry is a moment from due
          }
        }
      }),
    );
    const elapsedMs = Date.now() - started;
    sampling.done = true;
    await sampler;

    return { elapsedMs, claimedPerWorker: reports, peakConnections };
  }

  it('concurrent producers with overlapping dedupe keys create each job exactly once', async () => {
    const job = defineJob({
      kind: 'load.dedupe',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({ jobs: [job], connect: createPgPoolConnector(pool) });
    const keys = 500;
    const started = Date.now();

    // Every producer enqueues every key, in its own transactions, all at once.
    const results = await Promise.all(
      Array.from({ length: PRODUCERS }, async () => {
        let created = 0;

        for (let batch = 0; batch < keys; batch += 50) {
          created += await outbox.transaction(driver.db, async (_tx, enqueue) => {
            let n = 0;

            for (let i = batch; i < batch + 50; i++) {
              const r = await enqueue(job, { payload: i, dedupeKey: `k:${i}` });
              expect(['created', 'exists']).toContain(r.status);
              n += r.status === 'created' ? 1 : 0;
            }

            return n;
          });
        }

        return created;
      }),
    );
    const elapsedMs = Date.now() - started;

    expect(results.reduce((a, b) => a + b, 0)).toBe(keys);
    expect((await statusCounts())['pending']).toBe(keys);

    summary['dedupe race'] = {
      producers: PRODUCERS,
      enqueues: PRODUCERS * keys,
      created: keys,
      time: seconds(elapsedMs),
      'enqueues/s': Math.round((PRODUCERS * keys) / (elapsedMs / 1_000)),
    };
  });

  it(`${JOBS} jobs across ${WORKERS} workers × ${LANES} lanes: every job settles exactly as its handler decided`, async () => {
    const calls = new Map<number, number>();
    let running = 0;
    let peakRunning = 0;
    // Deterministic per payload: 1 in 20 retries once, 1 in 50 fails, the rest succeed.
    const job = defineJob({
      kind: 'load.mixed',
      timeoutMs: 5_000,
      payload: z.number(),
      handle: async (i) => {
        const call = (calls.get(i) ?? 0) + 1;
        calls.set(i, call);
        running++;
        peakRunning = Math.max(peakRunning, running);

        try {
          await sleep(HANDLER_MS);
        } finally {
          running--;
        }

        if (i % 50 === 1) return fail({ code: 'rejected' });
        if (i % 20 === 0 && call === 1) return retry({ code: 'busy', after: 0 });

        return ok();
      },
    });
    const events = eventCounter();
    const outboxes = Array.from({ length: WORKERS }, () =>
      createOutbox({
        jobs: [job],
        connect: createPgPoolConnector(pool),
        lanes: LANES,
        events: events.sink,
      }),
    );

    const enqueueStarted = Date.now();

    await Promise.all(
      Array.from({ length: PRODUCERS }, async (_, p) => {
        for (let start = p * 100; start < JOBS; start += PRODUCERS * 100) {
          await outboxes[0]!.transaction(driver.db, async (_tx, enqueue) => {
            for (let i = start; i < Math.min(start + 100, JOBS); i++) {
              await enqueue(job, { payload: i, dedupeKey: `j:${i}` });
            }
          });
        }
      }),
    );
    const enqueueMs = Date.now() - enqueueStarted;

    const { elapsedMs, claimedPerWorker, peakConnections } = await drainAll(outboxes);

    const failing = Array.from({ length: JOBS }, (_, i) => i).filter((i) => i % 50 === 1);
    const retried = Array.from({ length: JOBS }, (_, i) => i).filter(
      (i) => i % 20 === 0 && i % 50 !== 1,
    );

    // Final states are exactly what the handlers decided.
    expect(await statusCounts()).toEqual({
      succeeded: JOBS - failing.length,
      dead: failing.length,
    });

    // No lease expired, so at-least-once never had to repeat a handler.
    for (let i = 0; i < JOBS; i++) {
      expect(calls.get(i), `job ${i}`).toBe(retried.includes(i) ? 2 : 1);
    }

    // The ledger agrees with the handlers: one closed attempt per call, none left open.
    const [ledger] = await query<{ attempts: number; open: number }>(
      `SELECT count(*)::int AS attempts, count(*) FILTER (WHERE finished_at IS NULL)::int AS open
         FROM "outbox"."attempt"`,
    );
    expect(ledger).toEqual({ attempts: JOBS + retried.length, open: 0 });

    // Contention must never surface as a runtime fault.
    for (const bad of ['settle_failed', 'lease_lost', 'infra_error', 'hook_failed', 'claim_late']) {
      expect(events.count(bad), bad).toBe(0);
    }

    // Work spread across workers instead of piling on one.
    expect(claimedPerWorker.filter((n) => n > 0).length).toBeGreaterThan(WORKERS / 2);
    // One connection per lane, never more: a leak would climb past this toward the pool's max.
    expect(peakConnections).toBeLessThanOrEqual(WORKERS * LANES);

    const [duration] = await query<{ p50: number; p99: number }>(
      `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) AS p50,
              percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_ms) AS p99
         FROM "outbox"."attempt"`,
    );

    summary['mixed outcomes'] = {
      jobs: JOBS,
      'workers × lanes': `${WORKERS} × ${LANES}`,
      enqueue: seconds(enqueueMs),
      drain: seconds(elapsedMs),
      'jobs/s': Math.round(JOBS / (elapsedMs / 1_000)),
      'attempt p50 / p99': `${Math.round(duration!.p50)} / ${Math.round(duration!.p99)} ms`,
      'peak handlers': peakRunning,
      'peak connections': `${peakConnections} / ${WORKERS * LANES} lanes`,
    };
  });

  it(`a crash backlog of ${CRASHED} expired leases recovers within the budget, then runs`, async () => {
    const calls = new Map<number, number>();
    const job = defineJob({
      kind: 'load.crashed',
      timeoutMs: 1_000,
      payload: z.number(),
      // A crash retries with backoff; keep it short so the drain doesn't idle for 30 s.
      retry: { backoff: { baseMs: 100, maxMs: 500 } },
      handle: async (i) => {
        calls.set(i, (calls.get(i) ?? 0) + 1);

        return ok();
      },
    });
    const events = eventCounter();
    const outbox = createOutbox({
      jobs: [job],
      connect: createPgPoolConnector(pool),
      events: events.sink,
    });

    await outbox.transaction(driver.db, async (_tx, enqueue) => {
      for (let i = 0; i < CRASHED; i++) {
        await enqueue(job, { payload: i, dedupeKey: `c:${i}` });
      }
    });

    // A worker that died mid-job: it claimed everything, then never settled. Age the leases
    // past their end, as the passing of time would.
    await claimDue(driver.db, {
      kinds: [{ kind: job.kind, leaseMs: job.leaseMs, fits: true }],
      limit: CRASHED,
      trigger: 'sweep',
      leaseToken: '00000000-0000-4000-8000-00000000dead',
      now: null,
    });
    await driver.pool.query(
      `UPDATE "outbox"."job" SET leased_until = now() - interval '1 second' WHERE status = 'processing'`,
    );
    expect((await statusCounts())['processing']).toBe(CRASHED);

    // One recovery pass with a short budget stops at it, leaving the rest for the next tick.
    // Recovery keeps a 10 s settle window in reserve, so 11 s leaves about 1 s of work.
    const budgetMs = 11_000;
    const first = Date.now();
    const partial = await outbox[RUNTIME].recover(CRASHED, { deadlineAt: Date.now() + budgetMs });
    const firstMs = Date.now() - first;
    const firstPass = partial.retrying + partial.dead;

    expect(firstMs).toBeLessThan(budgetMs);
    expect(firstPass).toBeGreaterThan(0);
    // A machine fast enough to clear the whole backlog in ~1 s never needs to stop.
    expect(partial.reachedDeadline || firstPass === CRASHED).toBe(true);

    // Then recovery passes until none are left, as successive cron ticks would.
    const started = Date.now();
    let passes = 1;

    while ((await statusCounts())['processing'] ?? 0) {
      await outbox[RUNTIME].recover(CRASHED, { deadlineAt: Date.now() + 60_000 });
      passes++;
    }

    const recoverMs = Date.now() - started + firstMs;
    const { elapsedMs } = await drainAll([outbox]);

    expect(await statusCounts()).toEqual({ succeeded: CRASHED });
    // The crashed attempt never ran a handler here; each job ran exactly once after recovery.
    for (let i = 0; i < CRASHED; i++) {
      expect(calls.get(i), `job ${i}`).toBe(1);
    }

    const [ledger] = await query<{ expired: number }>(
      `SELECT count(*)::int AS expired FROM "outbox"."attempt" WHERE outcome = 'lease_expired'`,
    );
    expect(ledger?.expired).toBe(CRASHED);
    expect(events.count('settle_failed')).toBe(0);

    summary['crash recovery'] = {
      'expired leases': CRASHED,
      'first pass (11 s budget)': `${firstPass} in ${seconds(firstMs)}`,
      'stopped at budget': String(partial.reachedDeadline),
      passes,
      'recover total': seconds(recoverMs),
      'recovered/s': Math.round(CRASHED / (recoverMs / 1_000)),
      'then drain': seconds(elapsedMs),
    };
  });
});
