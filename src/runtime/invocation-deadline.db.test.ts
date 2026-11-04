// Third audit: one invocation deadline bounds
// every awaited dependency, a late claim never starts user code, and recovery has a time budget.
import { PgDialect } from 'drizzle-orm/pg-core';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import type { LaneConnection, OutboxDb } from './types';

import { createScheduledHandler } from '../cloudflare/scheduled-handler';
import { defineJob } from '../core/define-job';
import { ok, retry } from '../core/outcome';
import { resolveRetryPolicy } from '../core/retry-policy';
import { decideTransition } from '../core/transition';
import { claimDue, getJob } from '../store/outbox-store';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { createOutbox, RUNTIME } from './create-outbox';
import {
  laneConnector,
  recordingEvents,
  recordingQueue,
  testClock,
} from './runtime-harness.test-support';

let driver: TestDriver;

beforeAll(async () => {
  driver = await testDrivers()[0]!.create();
});

beforeEach(() => driver.reset());

afterAll(() => driver.close());

const dialect = new PgDialect();
const render = (query: Parameters<OutboxDb['execute']>[0]) =>
  dialect.sqlToQuery(query as never).sql;
const never = () => new Promise<never>(() => {});
const BUDGET = { maxJobs: 10, deadlineMs: 180_000 };

/** A connector whose statements matching `pattern` get `intercept` instead of (or around) the DB. */
function intercepting(
  pattern: RegExp,
  intercept: (run: () => Promise<unknown>) => Promise<unknown>,
) {
  const base = laneConnector(driver);
  const connect = async (): Promise<LaneConnection> => {
    const c = await base.connect();
    const wrap = (db: OutboxDb): OutboxDb => ({
      execute: (query) =>
        (pattern.test(render(query))
          ? intercept(() => Promise.resolve(db.execute(query)))
          : db.execute(query)) as never,
      transaction: (fn) => db.transaction((tx) => fn(wrap(tx))),
    });

    return { ...c, db: wrap(c.db) };
  };

  return { connect, ends: base.ends };
}

async function idOf(result: Promise<{ status: string } & Partial<{ id: string }>>) {
  const r = await result;

  if (!r.id) {
    throw new Error(`enqueue failed: ${r.status}`);
  }

  return r.id;
}

describe('A1: a claim that arrives too late never starts the handler', () => {
  const lateJob = (calls: { n: number }) =>
    defineJob({
      kind: 'a1.late',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => {
        calls.n++;

        return ok();
      },
    });

  it('settles a late drain claim as an uncharged claim_late retry', async () => {
    const clock = testClock();
    const calls = { n: 0 };
    const job = lateJob(calls);
    // The claim lands, then its reply takes 40 s: more than the 30 s lease margin.
    const late = intercepting(/FROM claimed/, async (run) => {
      const rows = await run();
      clock.advance(40_000);

      return rows;
    });
    const outbox = createOutbox({ jobs: [job], clock, connect: late.connect });
    const id = await idOf(outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'late' }));

    const report = await outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 36_000 }, 'sweep');

    expect(calls.n).toBe(0);
    expect(report).toMatchObject({ claimed: 1, retrying: 1, settleFailed: 0 });
    const stored = await getJob(driver.db, { id });
    expect(stored).toMatchObject({ status: 'retrying', lastErrorCode: 'claim_late' });
    expect(stored?.attempts).toEqual([
      expect.objectContaining({ outcome: 'lease_expired', code: 'claim_late' }),
    ]);
  });

  it('applies the same check to a targeted nudge claim', async () => {
    const clock = testClock();
    const calls = { n: 0 };
    const job = lateJob(calls);
    const late = intercepting(/FROM claimed|claim_results|AS result/, async (run) => {
      const rows = await run();
      clock.advance(40_000);

      return rows;
    });
    const outbox = createOutbox({
      jobs: [job],
      clock,
      connect: late.connect,
      queue: recordingQueue().queue,
    });
    const id = await idOf(outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'nudge' }));

    await outbox[RUNTIME].processNudge([id], { maxJobs: 1, deadlineMs: 600_000 });

    expect(calls.n).toBe(0);
    expect((await getJob(driver.db, { id }))?.status).toBe('retrying');
  });

  it('still runs a promptly claimed job (control)', async () => {
    const calls = { n: 0 };
    const job = lateJob(calls);
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      connect: laneConnector(driver).connect,
    });
    await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'prompt' });

    const report = await outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 36_000 }, 'sweep');

    expect(calls.n).toBe(1);
    expect(report.succeeded).toBe(1);
  });

  it('never charges an attempt or a crash, however often a claim is late', () => {
    const transition = decideTransition({
      outcome: { type: 'claim_late' },
      job: { id: 'j', spentAttempts: 0, codeStreak: 1_000, lastErrorCode: 'claim_late' },
      policy: resolveRetryPolicy({ maxCrashes: 3 }),
      timing: { graceMs: 5_000 },
    });

    expect(transition).toMatchObject({
      to: 'retrying',
      code: 'claim_late',
      attemptOutcome: 'lease_expired',
    });
  });
});

describe('A2: every awaited dependency is bounded', () => {
  it('bounds post-settle nudge bookkeeping and destroys the lane it stalled', async () => {
    const clock = testClock();
    const events = recordingEvents();
    const stalled = intercepting(/nudge_sent_at = /, () => never());
    const job = defineJob({
      kind: 'a2.stamp',
      timeoutMs: 100_000,
      payload: z.number(),
      handle: async () => {
        // ~50 ms (real time) of settlement budget left.
        clock.advance(100_000 + 5_000 + 30_000 - 2_000 - 50);

        return retry({ code: 'busy' });
      },
    });
    const outbox = createOutbox({
      jobs: [job],
      clock,
      events: events.sink,
      recordNudges: true,
      queue: recordingQueue().queue,
      connect: stalled.connect,
    });
    await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'stamp' });

    const started = Date.now();
    const report = await outbox[RUNTIME].drain(BUDGET, 'sweep');

    expect(Date.now() - started).toBeLessThan(5_000);
    expect(report.retrying).toBe(1);
    expect(stalled.ends).toEqual([{ connection: 1, how: 'destroy' }]);
    expect(events.of('infra_error')).toContainEqual(
      expect.objectContaining({ operation: 'record_nudge' }),
    );
  });

  it('bounds the first connection acquisition and destroys a late connection', async () => {
    let arrive!: (c: LaneConnection) => void;
    let destroyed = false;
    const job = defineJob({
      kind: 'a2.connect',
      timeoutMs: 10,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      connect: () =>
        new Promise<LaneConnection>((resolve) => {
          arrive = resolve;
        }),
    });

    const started = Date.now();
    await expect(outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 200 }, 'sweep')).rejects.toThrow(
      /deadline/,
    );
    expect(Date.now() - started).toBeLessThan(2_000);

    arrive({
      db: driver.db as unknown as OutboxDb,
      close: async () => undefined,
      destroy: async () => {
        destroyed = true;
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(destroyed).toBe(true);
  });

  it('bounds a stalled claim query by the drain deadline and retires its lane', async () => {
    const clock = testClock();
    // The claim stalls once the drain's 36 s are all but spent (the job's 35 s lease fit at claim time).
    const stalled = intercepting(/FROM claimed/, () => {
      clock.advance(36_000 - 50);

      return never();
    });
    const job = defineJob({
      kind: 'a2.claim',
      timeoutMs: 10,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({ jobs: [job], clock, connect: stalled.connect });
    await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'claim' });

    const started = Date.now();
    await expect(
      outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 36_000 }, 'sweep'),
    ).rejects.toThrow(/deadline/);

    expect(Date.now() - started).toBeLessThan(2_000);
    expect(stalled.ends).toEqual([{ connection: 1, how: 'destroy' }]);
  });

  it('bounds a continuation send like a targeted nudge', async () => {
    const clock = testClock();
    const events = recordingEvents();
    const job = defineJob({
      kind: 'a2.cont',
      timeoutMs: 10,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock,
      events: events.sink,
      connect: laneConnector(driver).connect,
      queue: { send: () => never() },
    });

    const started = Date.now();
    const sent = await outbox[RUNTIME].continueDrain('default', {
      deadlineAt: clock.nowMs() + 100,
    });

    expect(Date.now() - started).toBeLessThan(2_000);
    expect(sent).toBe(false);
    expect(events.of('continuation_failed')).toHaveLength(1);
  });

  it('bounds a healthy close that never returns, then destroys the session', async () => {
    const base = laneConnector(driver);
    let destroyed = false;
    const job = defineJob({
      kind: 'a2.close',
      timeoutMs: 10,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      connect: async () => {
        const c = await base.connect();

        return {
          db: c.db,
          close: () => never(),
          destroy: async () => {
            destroyed = true;
          },
        };
      },
    });

    const started = Date.now();
    await outbox[RUNTIME].drain(BUDGET, 'sweep');

    expect(Date.now() - started).toBeLessThan(5_000);
    expect(destroyed).toBe(true);
  });
});

describe('A3: recovery shares a time budget', () => {
  async function expiredLeases(count: number) {
    const clock = testClock();
    const job = defineJob({
      kind: 'a3.recovery',
      timeoutMs: 100,
      payload: z.number(),
      handle: async () => ok(),
    });
    const events = recordingEvents();
    const outbox = createOutbox({
      jobs: [job],
      clock,
      events: events.sink,
      connect: laneConnector(driver).connect,
      // A slow-but-completing hook: each settlement is individually well inside its cap.
      onTransition: async () => {
        clock.advance(20_000);
      },
    });

    for (let i = 0; i < count; i++) {
      await outbox.enqueue(driver.db, job, { payload: i, dedupeKey: `r:${i}` });
    }

    await claimDue(driver.db, {
      kinds: [{ kind: job.kind, leaseMs: job.leaseMs, fits: true }],
      limit: count,
      trigger: 'sweep',
      leaseToken: '00000000-0000-4000-8000-000000000001',
      now: clock.dbNow(),
    });
    clock.advance(job.leaseMs + 1);

    return { clock, outbox, events };
  }

  it('stops recovering at its deadline and leaves the rest for the next sweep', async () => {
    const { clock, outbox } = await expiredLeases(10);
    const start = clock.nowMs();

    const report = await outbox[RUNTIME].recover(50, { deadlineAt: start + 60_000 });

    expect(report.retrying).toBeGreaterThan(0);
    expect(report.retrying).toBeLessThan(10);
    expect(clock.nowMs() - start).toBeLessThanOrEqual(60_000);
  });

  it('keeps a cron tick inside the invocation: recovery yields to the drain phase', async () => {
    const { clock, outbox, events } = await expiredLeases(46);
    const start = clock.nowMs();

    await createScheduledHandler(() => outbox)({}, {}, {});

    // 46 × 20 s would be 920 s; recovery's share of the tick ends far earlier.
    expect(clock.nowMs() - start).toBeLessThanOrEqual(840_000);
    expect(events.of('sweep_finished')).toHaveLength(1);
    expect(events.of('sweep_finished')[0]!.recovered).toBeLessThan(46);
  });
});

describe('A2: operator actions are bounded too', () => {
  it('bounds an operator action whose SQL stalls, and destroys its connection', async () => {
    const stalled = intercepting(/UPDATE "outbox"."job" SET status = 'cancelled'/, () => never());
    const job = defineJob({
      kind: 'a2.op',
      timeoutMs: 10,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      connect: stalled.connect,
      operationTimeoutMs: 200,
    });
    const id = await idOf(outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'op' }));

    const started = Date.now();
    await expect(outbox.cancel({ id }, 'operator')).rejects.toThrow(/deadline/);

    expect(Date.now() - started).toBeLessThan(2_000);
    expect(stalled.ends).toEqual([{ connection: 1, how: 'destroy' }]);
  });

  it('bounds an operator action waiting for a connection', async () => {
    const job = defineJob({
      kind: 'a2.op-connect',
      timeoutMs: 10,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      connect: () => never(),
      operationTimeoutMs: 200,
    });

    const started = Date.now();
    await expect(outbox.retryNow('00000000-0000-4000-8000-000000000001')).rejects.toThrow(
      /deadline/,
    );
    expect(Date.now() - started).toBeLessThan(2_000);
  });

  it('rejects a non-positive operationTimeoutMs', () => {
    const job = defineJob({
      kind: 'a2.op-cfg',
      timeoutMs: 10,
      payload: z.number(),
      handle: async () => ok(),
    });

    expect(() =>
      createOutbox({ jobs: [job], connect: laneConnector(driver).connect, operationTimeoutMs: 0 }),
    ).toThrow(/operationTimeoutMs/);
  });
});
