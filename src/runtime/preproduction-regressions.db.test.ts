// Regressions for the pre-production audit (A1, A3, A4, H1). A2 needs a real pool:
// see src/cloudflare/schedules.pg.test.ts.
import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import type { HandlerOutcome } from '../core/outcome';
import type { TestClock } from '../testing/test-clock';

import { DEFAULT_INVOCATION_BUDGET_MS } from '../cloudflare/limits';
import { createScheduledHandler } from '../cloudflare/scheduled-handler';
import { defineJob } from '../core/define-job';
import { ok } from '../core/outcome';
import { getJob } from '../store/outbox-store';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { createOutbox, RUNTIME } from './create-outbox';
import { DEFAULT_RECOVERY_BUDGET_MS } from './runtime';
import {
  laneConnector,
  recordingEvents,
  recordingQueue,
  testClock,
} from './runtime-harness.test-support';

const budget = { maxJobs: 10, deadlineMs: 180_000 };

function idOf(result: { status: string } & Partial<{ id: string }>) {
  if (!result.id) {
    throw new Error(`enqueue failed: ${result.status}`);
  }

  return result.id;
}

describe.each(testDrivers())('pre-production audit regressions: $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  /** Marks a job as claimed by a worker that died, its lease already expired. */
  async function crash(id: string, clock: TestClock) {
    const now = clock.current().toISOString();
    await driver.db.execute(sql`
      UPDATE outbox.job SET status = 'processing', started_attempts = 1,
        lease_token = gen_random_uuid(),
        leased_until = ${now}::timestamptz - interval '1 second', next_attempt_at = NULL
      WHERE id = ${id}`);
    await driver.db.execute(sql`
      INSERT INTO outbox.attempt (job_id, attempt_no, kind, lease_token, trigger, started_at)
      SELECT id, started_attempts, kind, lease_token, 'sweep', ${now}::timestamptz
      FROM outbox.job WHERE id = ${id}`);
  }

  describe('A1: schedules cannot consume recovery', () => {
    const cron = '* * * * *';

    function setup() {
      const clock = testClock();
      const events = recordingEvents();
      const job = defineJob({
        kind: 'audit.recovery',
        payload: z.number(),
        timeoutMs: 100,
        handle: async () => ok(),
      });
      const outbox = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        clock,
        events: events.sink,
      });

      return { clock, events, job, outbox };
    }

    it('recovers an expired lease on every tick despite slow successful schedules', async () => {
      const { clock, events, job, outbox } = setup();

      const tick = createScheduledHandler(() => outbox, {
        schedules: [1, 2, 3].map((n) => ({
          name: `slow-${n}`,
          cron,
          // Three successful 41 s callbacks: 123 s, more than recovery's whole 120 s allowance.
          run: async () => {
            clock.advance(41_000);
          },
        })),
      });

      for (let i = 0; i < 2; i++) {
        const id = idOf(await outbox.enqueue(driver.db, job, { payload: i, dedupeKey: `c${i}` }));
        await crash(id, clock);
        await tick({ cron, scheduledTime: clock.nowMs() }, {}, {});
        expect((await getJob(driver.db, { id }))?.status).toBe('succeeded');
      }

      expect(events.of('schedule_ran')).toHaveLength(6);
      expect(events.of('sweep_finished').map((e) => e.recovered)).toEqual([1, 1]);
    });

    it('holds recovery’s slice back from schedules that use their entire window', async () => {
      const { clock, events, job, outbox } = setup();
      const id = idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'c' }));
      await crash(id, clock);

      const startedAt = clock.nowMs();
      let schedulesDeadline = 0;

      const tick = createScheduledHandler(() => outbox, {
        schedules: [
          {
            name: 'greedy',
            cron,
            // Runs right up to the schedules' deadline, as a callback bounded by it can.
            run: async () => {
              schedulesDeadline =
                startedAt + DEFAULT_INVOCATION_BUDGET_MS - DEFAULT_RECOVERY_BUDGET_MS;
              clock.advance(schedulesDeadline - clock.nowMs() - 1);
            },
          },
        ],
      });

      await tick({ cron, scheduledTime: startedAt }, {}, {});

      expect(schedulesDeadline).toBeGreaterThan(startedAt);
      expect(events.of('schedule_ran')).toHaveLength(1);
      expect(events.of('sweep_finished').at(-1)?.recovered).toBe(1);
      expect((await getJob(driver.db, { id }))?.status).toBe('succeeded');
    });
  });

  describe('A1: recovery’s reserve', () => {
    const cron = '* * * * *';

    it('stops schedules short of the tick so recovery keeps its slice', async () => {
      const clock = testClock();
      const events = recordingEvents();
      const job = defineJob({
        kind: 'audit.reserve',
        payload: z.number(),
        timeoutMs: 100,
        handle: async () => ok(),
      });
      const outbox = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        clock,
        events: events.sink,
      });
      const id = idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'c' }));
      await crash(id, clock);

      // 15 × 59 s, each inside its 60 s operation limit, would overrun the whole 840 s tick.
      const tick = createScheduledHandler(() => outbox, {
        schedules: Array.from({ length: 15 }, (_, n) => ({
          name: `s${n}`,
          cron,
          run: async () => {
            clock.advance(59_000);
          },
        })),
      });

      await tick({ cron, scheduledTime: clock.nowMs() }, {}, {});

      // The 13th starts at 708 s, before the 720 s cutoff, and overruns it; the rest find the
      // cutoff passed and fail. Without the reserve all 15 run and recovery has no time left.
      expect(events.of('schedule_ran')).toHaveLength(13);
      expect(events.of('schedule_failed')).toHaveLength(2);
      expect(events.of('sweep_finished').at(-1)?.recovered).toBe(1);
      expect((await getJob(driver.db, { id }))?.status).toBe('succeeded');
    });

    it('rejects an invalid recoveryBudgetMs at creation', () => {
      for (const recoveryBudgetMs of [0, -1, 1.5, Number.NaN]) {
        expect(() =>
          createScheduledHandler(() => undefined as never, { recoveryBudgetMs }),
        ).toThrow(/recoveryBudgetMs/);
      }
    });
  });

  describe('A3: bulk results match committed mutations for any UUID case', () => {
    async function enqueued() {
      const job = defineJob({
        kind: 'audit.uuid',
        payload: z.number(),
        timeoutMs: 100,
        handle: async () => ok(),
      });
      const outbox = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        generateId: () => 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
      });
      const id = idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'uuid' }));

      return { outbox, id };
    }

    it('reports uppercase ids under the key the caller passed', async () => {
      const { outbox, id } = await enqueued();
      const upper = id.toUpperCase();

      expect(await outbox.cancelMany([upper], 'operator')).toEqual({ [upper]: 'cancelled' });
      expect((await getJob(driver.db, { id }))?.status).toBe('cancelled');
      expect(await outbox.replayMany([upper])).toEqual({ [upper]: 'replayed' });
      expect((await getJob(driver.db, { id }))?.status).toBe('pending');
    });

    it('gives case-equivalent ids the same result, mutating the row once', async () => {
      const { outbox, id } = await enqueued();
      const upper = id.toUpperCase();
      const mixed = id.replace(/[a-f]/g, (c, i: number) => (i % 2 ? c.toUpperCase() : c));

      expect(await outbox.cancelMany([id, upper, mixed, upper], 'operator')).toEqual({
        [id]: 'cancelled',
        [upper]: 'cancelled',
        [mixed]: 'cancelled',
      });

      const replayed = await outbox.replayMany([mixed, id]);
      expect(replayed).toEqual({ [mixed]: 'replayed', [id]: 'replayed' });
      expect((await getJob(driver.db, { id }))?.status).toBe('pending');
    });
  });

  describe('A4: kinds and lanes named like Object.prototype members', () => {
    it('tallies kind constructor as its own entry without touching Object', async () => {
      const before = Object.getOwnPropertyNames(Object).sort();
      const job = defineJob({
        kind: 'constructor',
        payload: z.number(),
        timeoutMs: 100,
        handle: async () => ok(),
      });
      const outbox = createOutbox({ jobs: [job], connect: laneConnector(driver).connect });
      await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'constructor' });

      const report = await outbox[RUNTIME].drain(budget, 'sweep');

      expect(report.succeeded).toBe(1);
      expect(Object.hasOwn(report.byKind, 'constructor')).toBe(true);
      expect(report.byKind['constructor']).toMatchObject({ claimed: 1, succeeded: 1 });
      expect(JSON.parse(JSON.stringify(report)).byKind).toEqual({
        constructor: expect.objectContaining({ claimed: 1, succeeded: 1 }),
      });
      expect(Object.getOwnPropertyNames(Object).sort()).toEqual(before);
    });

    it('treats a lane named constructor without its own queue as having none', async () => {
      const events = recordingEvents();
      const { queue } = recordingQueue();
      const job = defineJob({
        kind: 'audit.lane',
        lane: 'constructor',
        payload: z.number(),
        timeoutMs: 100,
        handle: async () => ok(),
      });
      const outbox = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        queue: { default: queue },
        events: events.sink,
      });

      await outbox.transaction(driver.db, (_tx, enqueue) =>
        enqueue(job, { payload: 1, dedupeKey: 'lane' }),
      );

      expect(events.of('nudge_skipped')).toMatchObject([
        { lane: 'constructor', reason: 'no_queue' },
      ]);
      expect(events.of('nudge_failed')).toEqual([]);
    });
  });

  describe('H1: malformed outcomes become a classified retry', () => {
    it.each<[string, unknown]>([
      ['cancel without reason', { type: 'cancel' }],
      ['retry without code', { type: 'retry' }],
      ['fail with a numeric code', { type: 'fail', code: 7 }],
      ['ok with a non-function commit', { type: 'ok', commit: 'nope' }],
      ['retry with an object message', { type: 'retry', code: 'x', message: {} }],
    ])('%s', async (_name, outcome) => {
      const events = recordingEvents();
      const job = defineJob({
        kind: 'audit.malformed',
        payload: z.number(),
        timeoutMs: 100,
        handle: async () => outcome as HandlerOutcome,
      });
      const outbox = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        events: events.sink,
      });
      const id = idOf(await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'bad' }));

      const report = await outbox[RUNTIME].drain(budget, 'sweep');

      expect(report.settleFailed).toBe(0);
      expect(report.retrying).toBe(1);
      expect(await getJob(driver.db, { id })).toMatchObject({
        status: 'retrying',
        lastErrorCode: 'invalid_outcome',
      });
    });
  });
});
