// R2/R3 (re-audit): bookkeeping reuses the lane, and every
// settlement runs under one absolute deadline enforced by native timeouts plus a runtime watchdog.
import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import type { LaneConnection, OutboxDb } from './types';

import { defineJob } from '../core/define-job';
import { ok, retry } from '../core/outcome';
import { queryRows } from '../drizzle/catalog';
import { verifyOutboxSchema } from '../drizzle/verify-schema';
import { MIN_SERVER_VERSION_NUM, serverVersionGuard } from '../sql/migrations';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { createOutbox, RUNTIME } from './create-outbox';
import {
  connectionReset,
  jobRow,
  laneConnector,
  pgFault,
  recordingEvents,
  recordingQueue,
  singleSlotConnector,
  SqlFaults,
  testClock,
} from './runtime-harness.test-support';

let driver: TestDriver;

beforeAll(async () => {
  driver = await testDrivers()[0]!.create();
});

beforeEach(() => driver.reset());

afterAll(() => driver.close());

const BUDGET = { maxJobs: 10, deadlineMs: 180_000 };
const SETTLE = /settled AS \(/;
const TIMEOUTS =
  /set_config\('transaction_timeout', '(\d+)ms', true\).*set_config\('statement_timeout', '(\d+)ms', true\).*set_config\('lock_timeout', '(\d+)ms', true\)/;

const timeoutsOf = (statements: readonly string[]) =>
  statements.flatMap((s) => {
    const m = TIMEOUTS.exec(s);

    return m ? [{ transaction: Number(m[1]), statement: Number(m[2]), lock: Number(m[3]) }] : [];
  });

/** A clock whose sleeps pass time, so retry delays spend the settlement budget. */
function passingClock() {
  const clock = testClock();
  clock.sleep = async (ms: number) => clock.advance(ms);

  return clock;
}

async function enqueue(
  outbox: ReturnType<typeof createOutbox>,
  job: Parameters<ReturnType<typeof createOutbox>['enqueue']>[1],
  key = 'k',
) {
  const result = await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: key });

  if (!('id' in result)) {
    throw new Error(`enqueue failed: ${result.status}`);
  }

  return result.id;
}

describe('R2: bookkeeping never needs a second connection', () => {
  it('stamps a retry nudge on the lane that settled it, inside a one-client pool', async () => {
    const pool = singleSlotConnector(driver);
    const job = defineJob({
      kind: 'r2.retry',
      timeoutMs: 100,
      payload: z.number(),
      handle: async () => retry({ code: 'busy' }),
    });
    const outbox = createOutbox({
      jobs: [job],
      connect: pool.connect,
      recordNudges: true,
      queue: recordingQueue().queue,
      clock: testClock(),
    });
    const id = await enqueue(outbox, job);

    const report = await outbox[RUNTIME].drain(BUDGET, 'sweep');

    expect(report.retrying).toBe(1);
    expect(pool.log).not.toContain('connect:exhausted');
    expect((await jobRow(driver, id))?.nudge_sent_at).not.toBeNull();
  });

  it('retryNow stamps its nudge on its own connection', async () => {
    const pool = singleSlotConnector(driver);
    const job = defineJob({
      kind: 'r2.now',
      timeoutMs: 100,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      connect: pool.connect,
      recordNudges: true,
      queue: recordingQueue().queue,
      clock: testClock(),
    });
    const id = await enqueue(outbox, job);
    pool.log.length = 0;

    expect(await outbox.retryNow(id)).toBe('rescheduled');
    expect(pool.log).toEqual(['connect', 'close']);
    expect((await jobRow(driver, id))?.nudge_sent_at).not.toBeNull();
  });

  it('destroys a runaway lane connection before acquiring its replacement', async () => {
    const pool = singleSlotConnector(driver);
    let calls = 0;
    const job = defineJob({
      kind: 'r2.runaway',
      timeoutMs: 1,
      payload: z.number(),
      // The first attempt ignores its signal; the second job behaves.
      handle: () => (++calls === 1 ? new Promise<never>(() => {}) : Promise.resolve(ok())),
    });
    const outbox = createOutbox({ jobs: [job], connect: pool.connect, clock: testClock() });
    const first = await enqueue(outbox, job, 'a');
    const second = await enqueue(outbox, job, 'b');

    const report = await outbox[RUNTIME].drain(BUDGET, 'sweep');

    expect(pool.log).toEqual(['connect', 'destroy', 'connect', 'close']);
    expect(report).toMatchObject({ retrying: 1, succeeded: 1 });
    expect((await jobRow(driver, first))?.status).toBe('retrying');
    expect((await jobRow(driver, second))?.status).toBe('succeeded');
    // Each connection ends exactly once.
    expect(pool.ends.map((e) => e.connection)).toEqual([1, 2]);
  });
});

describe('R3: one settlement deadline', () => {
  it('runs even a plain settle in a transaction with timeouts derived from the remaining budget', async () => {
    const faults = new SqlFaults();
    const clock = testClock();
    const job = defineJob({
      kind: 'r3.plain',
      timeoutMs: 100_000,
      payload: z.number(),
      handle: async () => {
        // Leaves 10 s of the 135 s lease.
        clock.advance(125_000);

        return ok();
      },
    });
    const outbox = createOutbox({
      jobs: [job],
      clock,
      connect: laneConnector(driver, faults).connect,
    });
    await enqueue(outbox, job);

    const report = await outbox[RUNTIME].drain(BUDGET, 'sweep');

    expect(report.succeeded).toBe(1);
    const [timeouts] = timeoutsOf(faults.statements);
    // Never past the lease (less a safety margin), and capped below the 30 s lease margin.
    expect(timeouts).toBeDefined();
    expect(timeouts!.transaction).toBeLessThanOrEqual(10_000);
    expect(timeouts!.transaction).toBeGreaterThan(0);
    expect(timeouts!.statement).toBe(timeouts!.transaction);
    expect(timeouts!.lock).toBeLessThanOrEqual(timeouts!.transaction);
  });

  it('caps a fresh settlement well inside the 30 s lease margin', async () => {
    const faults = new SqlFaults();
    const job = defineJob({
      kind: 'r3.fresh',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      connect: laneConnector(driver, faults).connect,
    });
    await enqueue(outbox, job);

    await outbox[RUNTIME].drain(BUDGET, 'sweep');

    const [timeouts] = timeoutsOf(faults.statements);
    expect(timeouts!.transaction).toBeLessThanOrEqual(25_000);
    expect(timeouts!.lock).toBeLessThanOrEqual(5_000);
  });

  it('shares one deadline across retries: timeouts shrink and retries stop when it is spent', async () => {
    const faults = new SqlFaults();
    const clock = passingClock();
    const events = recordingEvents();
    const job = defineJob({
      kind: 'r3.retries',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock,
      events: events.sink,
      connect: laneConnector(driver, faults).connect,
    });
    const id = await enqueue(outbox, job);
    faults.failNext(SETTLE, () => pgFault('40P01', 'deadlock detected'), 100);
    const start = clock.nowMs();

    const report = await outbox[RUNTIME].drain(BUDGET, 'sweep');

    expect(report.settleFailed).toBe(1);
    expect((await jobRow(driver, id))?.status).toBe('processing');
    // Retry sleeps advance the clock: the whole settlement stayed inside its budget.
    expect(clock.nowMs() - start).toBeLessThanOrEqual(25_000);
    const transactions = timeoutsOf(faults.statements).map((t) => t.transaction);
    expect(transactions.length).toBeGreaterThan(1);
    expect(transactions.every((ms, i) => i === 0 || ms < transactions[i - 1]!)).toBe(true);
    expect(events.of('settle_failed')).toHaveLength(1);
  });

  it('a watchdog bounds application code waiting outside SQL and destroys the connection', async () => {
    const clock = testClock();
    const events = recordingEvents();
    const base = laneConnector(driver);
    const connect = async (): Promise<LaneConnection> => {
      const c = await base.connect();
      // The settle transaction never starts: a stalled pool or proxy, invisible to SQL timeouts.
      const db = new Proxy(c.db, {
        get: (target, prop) =>
          prop === 'transaction'
            ? () => new Promise<never>(() => {})
            : (Reflect.get(target, prop, target) as unknown),
      }) as OutboxDb;

      return { ...c, db };
    };
    const job = defineJob({
      kind: 'r3.watchdog',
      timeoutMs: 100_000,
      payload: z.number(),
      handle: async () => {
        // Leave ~50 ms (real time) of settlement budget.
        clock.advance(100_000 + 5_000 + 30_000 - 2_000 - 50);

        return ok();
      },
    });
    const outbox = createOutbox({ jobs: [job], clock, events: events.sink, connect });
    const id = await enqueue(outbox, job);
    await enqueue(outbox, job, 'second');

    const started = Date.now();
    const report = await outbox[RUNTIME].drain(BUDGET, 'sweep');

    expect(Date.now() - started).toBeLessThan(5_000);
    expect(report.settleFailed).toBe(1);
    // The lane is retired: its connection was destroyed, and it claims nothing more.
    expect(report.claimed).toBe(1);
    expect(base.ends).toEqual([{ connection: 1, how: 'destroy' }]);
    expect(events.of('settle_failed')).toEqual([
      expect.objectContaining({
        jobId: id,
        dbCode: null,
        error: expect.objectContaining({ message: expect.stringMatching(/deadline/) }),
      }),
    ]);
    expect((await jobRow(driver, id))?.status).toBe('processing');
  });

  it('bounds the post-settle queue send by the same deadline', async () => {
    const clock = testClock();
    const events = recordingEvents();
    const job = defineJob({
      kind: 'r3.send',
      timeoutMs: 100_000,
      payload: z.number(),
      handle: async () => {
        clock.advance(100_000 + 5_000 + 30_000 - 2_000 - 50);

        return retry({ code: 'busy' });
      },
    });
    const outbox = createOutbox({
      jobs: [job],
      clock,
      events: events.sink,
      connect: laneConnector(driver).connect,
      queue: { send: () => new Promise<never>(() => {}) },
    });
    const id = await enqueue(outbox, job);

    const started = Date.now();
    const report = await outbox[RUNTIME].drain(BUDGET, 'sweep');

    expect(Date.now() - started).toBeLessThan(5_000);
    expect(report.retrying).toBe(1);
    expect((await jobRow(driver, id))?.status).toBe('retrying');
    expect(events.of('nudge_failed')).toEqual([
      expect.objectContaining({
        ids: [id],
        error: expect.objectContaining({ message: expect.stringMatching(/deadline/) }),
      }),
    ]);
  });

  it('reads back a commit whose reply was lost to a timeout instead of assuming rollback', async () => {
    const faults = new SqlFaults();
    const events = recordingEvents();
    const job = defineJob({
      kind: 'r3.ambiguous',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      events: events.sink,
      connect: laneConnector(driver, faults).connect,
    });
    const id = await enqueue(outbox, job);
    // transaction_timeout terminates the session (FATAL 25P04) — possibly after COMMIT landed.
    faults.failAfterNextCommit(() =>
      pgFault('25P04', 'terminating connection due to transaction timeout'),
    );

    const report = await outbox[RUNTIME].drain(BUDGET, 'sweep');

    expect(report).toMatchObject({ succeeded: 1, leaseLost: 0, settleFailed: 0 });
    expect(events.of('lease_lost')).toEqual([]);
    expect((await jobRow(driver, id))?.status).toBe('succeeded');
  });

  it('reads back a commit whose reply was lost to a dropped connection', async () => {
    const faults = new SqlFaults();
    const job = defineJob({
      kind: 'r3.dropped',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      connect: laneConnector(driver, faults).connect,
    });
    const id = await enqueue(outbox, job);
    faults.failAfterNextCommit(connectionReset);

    const report = await outbox[RUNTIME].drain(BUDGET, 'sweep');

    expect(report).toMatchObject({ succeeded: 1, leaseLost: 0 });
    expect((await jobRow(driver, id))?.status).toBe('succeeded');
  });
});

describe('minimum PostgreSQL version', () => {
  it('requires PostgreSQL 17', () => {
    expect(MIN_SERVER_VERSION_NUM).toBe(170_000);
  });

  it('accepts this engine and rejects an older one clearly at setup', async () => {
    await expect(
      driver.db.execute(sql.raw(serverVersionGuard(MIN_SERVER_VERSION_NUM))),
    ).resolves.toBeDefined();
    await expect(driver.db.execute(sql.raw(serverVersionGuard(990_000)))).rejects.toMatchObject({
      cause: { message: expect.stringMatching(/requires PostgreSQL 99 or newer/) },
    });
  });

  it('reports an unsupported server from verifyOutboxSchema', async () => {
    expect(await verifyOutboxSchema(driver.db)).toEqual([]);

    const old: typeof driver.db = {
      ...driver.db,
      execute: async (query: Parameters<typeof driver.db.execute>[0]) => {
        const rows = await queryRows<Record<string, unknown>>(driver.db, query as never);

        return {
          rows: rows.map((r) => ('server_version_num' in r ? { server_version_num: 160_004 } : r)),
        } as never;
      },
    } as never;

    expect(await verifyOutboxSchema(old)).toContainEqual({
      type: 'unsupported_version',
      minimum: 170_000,
      actual: 160_004,
    });
  });
});
