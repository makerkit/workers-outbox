// Regressions for the first audit (F1–F6, C2, C3).
import { sql, type SQL } from 'drizzle-orm';
import { PgDialect } from 'drizzle-orm/pg-core';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import type { LaneConnection, OutboxDb } from './types';

import { createQueueHandler } from '../cloudflare/queue-handler';
import { defineJob, MAX_TIMEOUT_MS } from '../core/define-job';
import { ok } from '../core/outcome';
import { claimDue, getJob } from '../store/outbox-store';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { createOutbox, RUNTIME } from './create-outbox';
import { HANDOFF_COOLDOWN_MS } from './runtime';
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

const budget = { maxJobs: 10, deadlineMs: 180_000 };

const nudge = (ids: string[]) => ({
  body: { v: 1, type: 'nudge', ids },
  attempts: 1,
  ack() {},
  retry() {},
});

const drainMessage = {
  body: { v: 1, type: 'drain', lane: 'default' },
  attempts: 1,
  ack() {},
  retry() {},
};

function idOf(result: { status: string } & Partial<{ id: string }>) {
  if (!result.id) {
    throw new Error(`enqueue failed: ${result.status}`);
  }

  return result.id;
}

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('F1: the longest legal job fits the default queue budget', () => {
  it('runs a MAX_TIMEOUT_MS job through cron handoff and one default nudge', async () => {
    let calls = 0;
    const job = defineJob({
      kind: 'audit.long',
      timeoutMs: MAX_TIMEOUT_MS,
      payload: z.number(),
      handle: async () => {
        calls++;

        return ok();
      },
    });
    const queue = recordingQueue();
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      queue: queue.queue,
      connect: laneConnector(driver).connect,
    });
    const id = idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'long' }));

    await outbox[RUNTIME].drain(budget, 'sweep');
    await outbox[RUNTIME].handoff(budget.deadlineMs);
    await createQueueHandler(() => outbox)({ queue: 'q', messages: [nudge([id])] }, {}, {});

    expect(calls).toBe(1);
    expect((await getJob(driver.db, { id }))?.status).toBe('succeeded');
  });

  it('rejects a nudge budget that cannot hold the longest legal lease', () => {
    expect(() =>
      createQueueHandler(() => undefined as never, {
        nudgeBudget: { maxJobs: 1, deadlineMs: 600_000 },
      }),
    ).toThrow(/nudgeBudget/);
  });

  it('rejects an invocation budget beyond the 15-minute consumer limit', () => {
    expect(() =>
      createQueueHandler(() => undefined as never, { invocationBudgetMs: 900_000 }),
    ).toThrow(/invocationBudgetMs/);
  });
});

describe('handoff cooldown', () => {
  it('re-sends a long job only once per cooldown, even with recordNudges off', async () => {
    const clock = testClock();
    const job = defineJob({
      kind: 'audit.long-cooldown',
      timeoutMs: MAX_TIMEOUT_MS,
      payload: z.number(),
      handle: async () => ok(),
    });
    const queue = recordingQueue();
    const outbox = createOutbox({
      jobs: [job],
      clock,
      queue: queue.queue,
      connect: laneConnector(driver).connect,
    });
    const id = idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'cooldown' }));
    const nudges = () =>
      queue.sent.filter(({ message }) => message.type === 'nudge' && message.ids.includes(id))
        .length;
    const enqueueNudges = nudges();

    await outbox[RUNTIME].handoff(budget.deadlineMs);
    // The next tick: the first handoff may still be queued, so it is not re-sent.
    clock.advance(5 * 60_000);
    await outbox[RUNTIME].handoff(budget.deadlineMs);

    expect(nudges() - enqueueNudges).toBe(1);

    clock.advance(HANDOFF_COOLDOWN_MS);
    await outbox[RUNTIME].handoff(budget.deadlineMs);

    // Past the cooldown a lost or dead-lettered handoff is sent again.
    expect(nudges() - enqueueNudges).toBe(2);
  });
});

describe('F2: the stored payload is parsed exactly once', () => {
  it('applies a transform once', async () => {
    let received: number | undefined;
    const job = defineJob({
      kind: 'audit.transform',
      timeoutMs: 1_000,
      payload: z.number().transform((n) => n + 1),
      handle: async (n) => {
        received = n;

        return ok();
      },
    });
    const outbox = createOutbox({ jobs: [job], connect: laneConnector(driver).connect });

    await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'transform' });
    await outbox[RUNTIME].drain(budget, 'sweep');

    expect(received).toBe(2);
  });

  it('runs a string-to-number transform instead of rejecting its own output', async () => {
    let received: unknown;
    const job = defineJob({
      kind: 'audit.parse',
      timeoutMs: 1_000,
      payload: z.string().transform((s) => Number.parseInt(s, 10)),
      handle: async (n) => {
        received = n;

        return ok();
      },
    });
    const outbox = createOutbox({ jobs: [job], connect: laneConnector(driver).connect });

    await outbox.enqueue(driver.db, job, { payload: '41', dedupeKey: 'parse' });
    const report = await outbox[RUNTIME].drain(budget, 'sweep');

    expect(report.succeeded).toBe(1);
    expect(received).toBe(41);
  });

  it('rejects at enqueue an input that does not survive JSON storage', async () => {
    const job = defineJob({
      kind: 'audit.date',
      timeoutMs: 1_000,
      payload: z.object({ at: z.date() }),
      handle: async () => ok(),
    });
    const outbox = createOutbox({ jobs: [job], connect: laneConnector(driver).connect });

    const result = await outbox.enqueue(driver.db, job, {
      payload: { at: new Date() },
      dedupeKey: 'date',
    });

    expect(result.status).toBe('invalid_payload');
  });

  it('rejects at enqueue a payload over maxPayloadBytes, counted in UTF-8 bytes', async () => {
    const job = defineJob({
      kind: 'audit.big',
      timeoutMs: 1_000,
      payload: z.object({ text: z.string() }),
      handle: async () => ok(),
    });
    // {"text":"…"} is 11 bytes of JSON around the string.
    const outbox = createOutbox({
      jobs: [job],
      connect: laneConnector(driver).connect,
      maxPayloadBytes: 31,
    });

    // 20 ASCII bytes fit; 10 three-byte characters (30 bytes) don't, though only 10 code units.
    expect(
      await outbox.enqueue(driver.db, job, {
        payload: { text: 'a'.repeat(20) },
        dedupeKey: 'ascii',
      }),
    ).toMatchObject({ status: 'created' });
    const big = await outbox.enqueue(driver.db, job, {
      payload: { text: '€'.repeat(10) },
      dedupeKey: 'euro',
    });

    expect(big).toMatchObject({
      status: 'invalid_payload',
      issues: [{ message: expect.stringMatching(/41 bytes as JSON, over maxPayloadBytes \(31\)/) }],
    });
    expect(() =>
      createOutbox({ jobs: [job], connect: laneConnector(driver).connect, maxPayloadBytes: 0 }),
    ).toThrow(/maxPayloadBytes/);
  });

  it('hands a coerced Date to the handler', async () => {
    let received: unknown;
    const at = new Date('2026-01-02T03:04:05.000Z');
    const job = defineJob({
      kind: 'audit.coerce',
      timeoutMs: 1_000,
      payload: z.object({ at: z.coerce.date() }),
      handle: async (p) => {
        received = p.at;

        return ok();
      },
    });
    const outbox = createOutbox({ jobs: [job], connect: laneConnector(driver).connect });

    await outbox.enqueue(driver.db, job, { payload: { at }, dedupeKey: 'coerce' });
    await outbox[RUNTIME].drain(budget, 'sweep');

    expect(received).toEqual(at);
  });
});

describe('F3: one time budget per queue invocation', () => {
  it('stops claiming when the batch has spent the invocation budget, and re-nudges the rest', async () => {
    const clock = testClock();
    const start = clock.nowMs();
    const starts: number[] = [];
    const job = defineJob({
      kind: 'audit.batch',
      timeoutMs: 500_000,
      payload: z.number(),
      handle: async () => {
        starts.push(clock.nowMs() - start);
        clock.advance(490_000);

        return ok();
      },
    });
    const queue = recordingQueue();
    const outbox = createOutbox({
      jobs: [job],
      clock,
      queue: queue.queue,
      connect: laneConnector(driver).connect,
    });
    const ids: string[] = [];

    for (let i = 0; i < 3; i++) {
      ids.push(idOf(await outbox.enqueue(driver.db, job, { payload: i, dedupeKey: `batch:${i}` })));
    }

    queue.sent.length = 0;
    await createQueueHandler(() => outbox)(
      { queue: 'q', messages: ids.map((id) => nudge([id])) },
      {},
      {},
    );

    expect(starts).toEqual([0]);
    const renudged = queue.sent.flatMap((s) => (s.message.type === 'nudge' ? s.message.ids : []));
    expect(renudged.sort()).toEqual(ids.slice(1).sort());
  });

  it('re-chains a drain message that arrived after the budget was spent', async () => {
    const clock = testClock();
    const long = defineJob({
      kind: 'audit.slow',
      timeoutMs: 500_000,
      payload: z.number(),
      handle: async () => {
        clock.advance(835_000);

        return ok();
      },
    });
    const short = defineJob({
      kind: 'audit.short',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => ok(),
    });
    const queue = recordingQueue();
    const outbox = createOutbox({
      jobs: [long, short],
      clock,
      queue: queue.queue,
      connect: laneConnector(driver).connect,
    });
    const slowId = idOf(await outbox.enqueue(driver.db, long, { payload: 1, dedupeKey: 'slow' }));
    await outbox.enqueue(driver.db, short, { payload: 1, dedupeKey: 'short' });

    queue.sent.length = 0;
    await createQueueHandler(() => outbox)(
      { queue: 'q', messages: [nudge([slowId]), drainMessage] },
      {},
      {},
    );

    expect(queue.sent.some((s) => s.message.type === 'drain')).toBe(true);
  });
});

describe('F4: recovery settles arbitrarily old attempts', () => {
  it('saturates the attempt duration instead of overflowing int4', async () => {
    const clock = testClock();
    const events = recordingEvents();
    const job = defineJob({
      kind: 'audit.old',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock,
      events: events.sink,
      connect: laneConnector(driver).connect,
    });
    const id = idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'old' }));

    await claimDue(driver.db, {
      kinds: [{ kind: job.kind, leaseMs: job.leaseMs, fits: true }],
      limit: 1,
      trigger: 'sweep',
      leaseToken: '00000000-0000-4000-8000-000000000001',
      now: clock.dbNow(),
    });
    clock.advance(25 * 86_400_000);
    const report = await outbox[RUNTIME].recover(50);

    expect(report.settleFailed).toBe(0);
    const stored = await getJob(driver.db, { id });
    expect(stored?.status).toBe('retrying');
    expect(stored?.attempts[0]?.durationMs).toBe(2_147_483_647);
  });
});

describe('F5: ctx.db is revoked when the handler returns', () => {
  it('rejects a method captured while the handler was running', async () => {
    let late: (() => Promise<unknown>) | undefined;
    const job = defineJob({
      kind: 'audit.late',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async (_, ctx) => {
        const execute = (ctx as unknown as { db: OutboxDb }).db.execute;
        late = async () => execute(sql`SELECT 42 AS answer`);

        return ok();
      },
    });
    const outbox = createOutbox({ jobs: [job], connect: laneConnector(driver).connect });

    await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'late' });
    await outbox[RUNTIME].drain(budget, 'sweep');

    await expect(late!()).rejects.toThrow(/after the handler returned/);
  });

  it('rejects a transaction handle captured from ctx.db.transaction()', async () => {
    let captured: OutboxDb | undefined;
    const job = defineJob({
      kind: 'audit.tx',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async (_, ctx) => {
        await (ctx as unknown as { db: OutboxDb }).db.transaction(async (tx) => {
          captured = tx;
          await tx.execute(sql`SELECT 1`);
        });

        return ok();
      },
    });
    const outbox = createOutbox({ jobs: [job], connect: laneConnector(driver).connect });

    await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'tx' });
    const report = await outbox[RUNTIME].drain(budget, 'sweep');

    expect(report.succeeded).toBe(1);
    await expect(captured!.execute(sql`SELECT 42`)).rejects.toThrow(/after the handler returned/);
  });
});

describe('F6: payload validation is inside the attempt timeout', () => {
  it('times out a validator that outlives timeoutMs', async () => {
    let handled = false;
    const job = defineJob({
      kind: 'audit.validation',
      timeoutMs: 20,
      payload: {
        '~standard': {
          version: 1 as const,
          vendor: 'audit',
          validate: async (value: unknown) => {
            await delay(stored ? 200 : 0);

            return { value: value as number };
          },
        },
      },
      handle: async () => {
        handled = true;

        return ok();
      },
    });
    let stored = false;
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      connect: laneConnector(driver).connect,
    });
    const id = idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'validation' }));
    stored = true;

    const report = await outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 180_000 }, 'sweep');

    expect(report.retrying).toBe(1);
    expect(handled).toBe(false);
    expect((await getJob(driver.db, { id }))?.attempts[0]?.outcome).toBe('timeout');
  });

  it('turns a throwing validator into an invalid_payload retry', async () => {
    let stored = false;
    const job = defineJob({
      kind: 'audit.throws',
      timeoutMs: 1_000,
      payload: {
        '~standard': {
          version: 1 as const,
          vendor: 'audit',
          validate: (value: unknown) => {
            if (stored) {
              throw new Error('validator exploded');
            }

            return { value: value as number };
          },
        },
      },
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      connect: laneConnector(driver).connect,
    });
    const id = idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'throws' }));
    stored = true;

    const report = await outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 180_000 }, 'sweep');

    expect(report.errored).toBe(0);
    expect(report.retrying).toBe(1);
    expect((await getJob(driver.db, { id }))?.attempts[0]?.code).toBe('invalid_payload');
  });
});

describe('C2/C3: lane-parallel nudges', () => {
  it('C3: never claims more than maxJobs across lanes', async () => {
    let calls = 0;
    const job = defineJob({
      kind: 'audit.max',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => {
        calls++;

        return ok();
      },
    });
    const outbox = createOutbox({
      jobs: [job],
      lanes: 2,
      queue: recordingQueue().queue,
      connect: laneConnector(driver).connect,
    });
    const ids = [
      idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'a' })),
      idOf(await outbox.enqueue(driver.db, job, { payload: 2, dedupeKey: 'b' })),
    ];

    const report = await outbox[RUNTIME].processNudge(ids, { maxJobs: 1, deadlineMs: 180_000 });

    expect(report.claimed).toBe(1);
    expect(calls).toBe(1);
  });

  it('C2: a failing lane does not close a sibling lane that is still running', async () => {
    let siblingClosedEarly: boolean | undefined;
    const closed = new Set<number>();
    let opened = 0;
    const base = laneConnector(driver);
    const connect = async (): Promise<LaneConnection> => {
      const index = opened++;
      const lane = await base.connect();
      const db =
        index === 0
          ? (new Proxy(lane.db, {
              get: (target, prop) =>
                prop === 'execute'
                  ? () => Promise.reject(new Error('lane 0 down'))
                  : (Reflect.get(target, prop, target) as unknown),
            }) as OutboxDb)
          : lane.db;

      return {
        db,
        close: async () => {
          closed.add(index);
          await lane.close();
        },
        destroy: async () => {
          closed.add(index);
          await lane.destroy();
        },
      };
    };
    const job = defineJob({
      kind: 'audit.sibling',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => {
        await delay(50);
        siblingClosedEarly = closed.has(1);

        return ok();
      },
    });
    const outbox = createOutbox({ jobs: [job], lanes: 2, queue: recordingQueue().queue, connect });
    const ids = [
      idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'x' })),
      idOf(await outbox.enqueue(driver.db, job, { payload: 2, dedupeKey: 'y' })),
    ];

    await expect(
      outbox[RUNTIME].processNudge(ids, { maxJobs: 10, deadlineMs: 180_000 }),
    ).rejects.toThrow(/lane 0 down/);

    expect(siblingClosedEarly).toBe(false);
  });
});

describe('re-audit regressions', () => {
  it('R1: a validator that finishes after the timeout never starts the handler', async () => {
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    let called = false;
    let stored = false;
    const job = defineJob({
      kind: 'audit.late-validator',
      timeoutMs: 5,
      payload: {
        '~standard': {
          version: 1 as const,
          vendor: 'audit',
          validate: async (value: unknown) => {
            if (stored) {
              await blocked;
            }

            return { value: value as number };
          },
        },
      },
      handle: async () => {
        called = true;

        return ok();
      },
    });
    const outbox = createOutbox({
      jobs: [job],
      clock: testClock(),
      connect: laneConnector(driver).connect,
    });
    await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'late-validator' });
    stored = true;

    const report = await outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 180_000 }, 'sweep');
    release();
    await delay(10);

    expect(report.retrying).toBe(1);
    expect(called).toBe(false);
  });

  it('R4: eraseTenant rejects a non-positive batch instead of looping forever', async () => {
    const job = defineJob({
      kind: 'audit.erase',
      timeoutMs: 100,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({ jobs: [job], connect: laneConnector(driver).connect });

    await expect(outbox.eraseTenant('tenant', { batch: 0 })).rejects.toThrow(/batch/);
    await expect(outbox.eraseTenant('tenant', { batch: 1.5 })).rejects.toThrow(/batch/);
  });

  it('R5: retryNow routes its nudge without reading the attempt ledger', async () => {
    const job = defineJob({
      kind: 'audit.ledger',
      timeoutMs: 100,
      payload: z.number(),
      handle: async () => ok(),
    });
    const base = laneConnector(driver);
    const statements: string[] = [];
    const queue = recordingQueue();
    const outbox = createOutbox({
      jobs: [job],
      queue: queue.queue,
      connect: async () => {
        const c = await base.connect();

        return {
          ...c,
          db: new Proxy(c.db, {
            get(target, prop) {
              const value = Reflect.get(target, prop, target) as unknown;

              if (prop === 'execute') {
                return (query: Parameters<OutboxDb['execute']>[0]) => {
                  statements.push(new PgDialect().sqlToQuery(query as SQL).sql);

                  return target.execute(query);
                };
              }

              return typeof value === 'function' ? value.bind(target) : value;
            },
          }),
        };
      },
    });
    const id = idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'ledger' }));
    queue.sent.length = 0;

    expect(await outbox.retryNow(id)).toBe('rescheduled');

    expect(statements.some((s) => s.includes('"outbox"."attempt"'))).toBe(false);
    expect(statements).toHaveLength(1);
    expect(queue.sent.flatMap((s) => (s.message.type === 'nudge' ? s.message.ids : []))).toEqual([
      id,
    ]);
  });
});
