import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import type { OutboxMessage } from '../runtime/types';

import { defineJob } from '../core/define-job';
import { ok, retry } from '../core/outcome';
import { queryRows } from '../drizzle/catalog';
import { createOutbox } from '../runtime/create-outbox';
import {
  jobRow,
  laneConnector,
  recordingEvents,
  recordingQueue,
  SqlFaults,
  pgFault,
  testClock,
} from '../runtime/runtime-harness.test-support';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { createQueueHandler, type QueueMessage } from './queue-handler';
import { createScheduledHandler } from './scheduled-handler';

const payload = z.object({ n: z.number() });

const asMessage = (body: OutboxMessage): QueueMessage => ({
  body,
  attempts: 1,
  ack: () => undefined,
  retry: () => undefined,
});

describe.each(testDrivers())('lanes, reserved slices and liveness on $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  const ran: string[] = [];
  const job = (
    kind: string,
    options: {
      lane?: string;
      timeoutMs?: number;
      behavior?: () => ReturnType<typeof ok> | ReturnType<typeof retry>;
    } = {},
  ) =>
    defineJob({
      kind,
      payload,
      timeoutMs: options.timeoutMs ?? 1_000,
      ...(options.lane ? { lane: options.lane } : {}),
      handle: async (_, ctx) => {
        ran.push(`${kind}:${ctx.jobId}`);

        return options.behavior ? options.behavior() : ok();
      },
    });

  beforeEach(() => {
    ran.length = 0;
  });

  describe('lane routing', () => {
    // port: nudge-bulk-jobs:181
    it('nudges each job on its own lane, and refuses a lane with no queue instead of reporting success', async () => {
      const urgent = job('urgent.job');
      const bulk = job('bulk.job', { lane: 'bulk' });
      const recorded = recordingEvents();
      const bulkQueue = recordingQueue();
      const outbox = createOutbox({
        jobs: [urgent, bulk],
        connect: laneConnector(driver).connect,
        queue: { bulk: bulkQueue.queue },
        recordNudges: true,
        events: recorded.sink,
        clock: testClock(),
      });
      const a = await outbox.enqueue(driver.db, urgent, { payload: { n: 1 }, dedupeKey: 'a' });
      const b = await outbox.enqueue(driver.db, bulk, { payload: { n: 2 }, dedupeKey: 'b' });

      await outbox.nudge([a, b] as { id: string; kind: string }[]);

      expect(bulkQueue.sent.map((s) => s.message)).toEqual([
        { v: 1, type: 'nudge', ids: [(b as { id: string }).id] },
      ]);
      expect(recorded.of('nudge_skipped')).toEqual([
        {
          type: 'nudge_skipped',
          ids: [(a as { id: string }).id],
          lane: 'default',
          reason: 'no_queue',
        },
      ]);
      expect(await jobRow(driver, (a as { id: string }).id)).toMatchObject({
        nudge_sent_at: null,
        last_nudge_error: expect.stringContaining('no_queue'),
      });
      expect(await jobRow(driver, (b as { id: string }).id)).toMatchObject({
        nudge_sent_at: expect.anything(),
        last_nudge_error: null,
      });
    });

    it('a consumer bound to one lane never runs another lane’s job, even when nudged with its id', async () => {
      const urgent = job('urgent.job');
      const bulk = job('bulk.job', { lane: 'bulk' });
      const recorded = recordingEvents();
      const outbox = createOutbox({
        jobs: [urgent, bulk],
        connect: laneConnector(driver).connect,
        events: recorded.sink,
        clock: testClock(),
      });
      const a = (await outbox.enqueue(driver.db, urgent, {
        payload: { n: 1 },
        dedupeKey: 'a',
      })) as { id: string };

      await createQueueHandler(() => outbox, { lane: 'bulk' })(
        { queue: 'bulk', messages: [asMessage({ v: 1, type: 'nudge', ids: [a.id] })] },
        {},
        {},
      );

      expect(ran).toEqual([]);
      expect(recorded.of('nudge_miss')).toEqual([
        { type: 'nudge_miss', jobId: a.id, result: 'unregistered_kind' },
      ]);
      expect(await jobRow(driver, a.id)).toMatchObject({ status: 'pending' });
    });
  });

  it('chains a sweep continuation on every lane that still has a backlog', async () => {
    const urgent = job('urgent.job');
    const bulk = job('bulk.job', { lane: 'bulk' });
    const queues = { default: recordingQueue(), bulk: recordingQueue() };
    const outbox = createOutbox({
      jobs: [urgent, bulk],
      connect: laneConnector(driver).connect,
      queue: { default: queues.default.queue, bulk: queues.bulk.queue },
      clock: testClock(),
    });
    for (let i = 0; i < 3; i++) {
      await outbox.enqueue(driver.db, bulk, { payload: { n: i }, dedupeKey: `b${i}` });
    }

    await createScheduledHandler(() => outbox, { budget: { maxJobs: 1, deadlineMs: 180_000 } })(
      {},
      {},
      {},
    );

    expect(queues.bulk.sent.map((s) => s.message)).toEqual([{ v: 1, type: 'drain', lane: 'bulk' }]);
    expect(queues.default.sent).toEqual([]);
  });

  it('reports no continuation when the lane has no queue to continue on', async () => {
    const j = job('a.job');
    const recorded = recordingEvents();
    const outbox = createOutbox({
      jobs: [j],
      connect: laneConnector(driver).connect,
      events: recorded.sink,
      clock: testClock(),
    });
    for (let i = 0; i < 3; i++) {
      await outbox.enqueue(driver.db, j, { payload: { n: i }, dedupeKey: `c${i}` });
    }

    await createScheduledHandler(() => outbox, { budget: { maxJobs: 1, deadlineMs: 180_000 } })(
      {},
      {},
      {},
    );

    expect(recorded.of('sweep_finished')[0]?.continued).toBe(false);
  });

  describe('reserved sweep slices', () => {
    const seedBacklog = async (count: number) =>
      driver.db.execute(
        sql`INSERT INTO "outbox"."job" (id, kind, dedupe_key, payload, available_at, created_at)
            SELECT gen_random_uuid(), 'bulk.job', 'bulk-' || g, '{"n":1}', now() - interval '1 hour', now() - interval '1 hour'
            FROM generate_series(1, ${count}) g`,
      );

    // port: cron-handlers:186
    it('drains a reserved slice before the catch-all, so an older backlog cannot starve it', async () => {
      const bulk = job('bulk.job');
      const schedule = job('schedule.job');
      const outbox = createOutbox({
        jobs: [bulk, schedule],
        connect: laneConnector(driver).connect,
        clock: testClock(),
      });
      await seedBacklog(60);
      const s = (await outbox.enqueue(driver.db, schedule, {
        payload: { n: 1 },
        dedupeKey: 's',
      })) as { id: string };

      await createScheduledHandler(() => outbox, {
        budget: { maxJobs: 50, deadlineMs: 180_000 },
        reserved: [
          {
            name: 'schedule',
            kinds: ['schedule.job'],
            budget: { maxJobs: 10, deadlineMs: 45_000 },
          },
        ],
      })({}, {}, {});

      expect(ran[0]).toBe(`schedule.job:${s.id}`);
      expect(ran.filter((r) => r.startsWith('bulk.job'))).toHaveLength(50);
    });

    // port: cron-handlers:228
    it('a failing reserved slice is reported and the catch-all still runs', async () => {
      const faults = new SqlFaults();
      const bulk = job('bulk.job');
      const schedule = job('schedule.job');
      const recorded = recordingEvents();
      const outbox = createOutbox({
        jobs: [bulk, schedule],
        connect: laneConnector(driver, faults).connect,
        events: recorded.sink,
        clock: testClock(),
      });
      await seedBacklog(2);
      faults.failNext(/picked AS/, () => pgFault('XX000', 'slice broke'));

      await createScheduledHandler(() => outbox, {
        reserved: [
          {
            name: 'schedule',
            kinds: ['schedule.job'],
            budget: { maxJobs: 10, deadlineMs: 45_000 },
          },
        ],
      })({}, {}, {});

      expect(recorded.of('phase_failed').map((e) => e.phase)).toEqual(['reserved']);
      expect(ran).toHaveLength(2);
    });
  });

  describe('liveness', () => {
    it('hands a due job whose lease exceeds the sweep budget to its queue instead of running it', async () => {
      const long = job('long.job', { timeoutMs: 300_000 });
      const { queue, sent } = recordingQueue();
      const outbox = createOutbox({
        jobs: [long],
        connect: laneConnector(driver).connect,
        queue,
        clock: testClock(),
      });
      const l = (await outbox.enqueue(driver.db, long, {
        payload: { n: 1 },
        dedupeKey: 'long',
      })) as { id: string };

      await createScheduledHandler(() => outbox)({}, {}, {});

      expect(ran).toEqual([]);
      expect(sent.map((s) => s.message)).toEqual([{ v: 1, type: 'nudge', ids: [l.id] }]);
    });

    it('with every enqueue and retry nudge lost, short and long jobs still finish from cron (plus its handoffs)', async () => {
      let flaky = 0;
      const short = job('short.job', {
        behavior: () => (flaky++ === 0 ? retry({ code: 'once', after: 1_000 }) : ok()),
      });
      const long = job('long.job', { timeoutMs: 300_000 });
      const clock = testClock();
      const delivered: OutboxMessage[] = [];
      let sweeping = false;
      const outbox = createOutbox({
        jobs: [short, long],
        connect: laneConnector(driver).connect,
        clock,
        queue: {
          send: async (body) => {
            if (sweeping) {
              delivered.push(body as OutboxMessage);
            }
          },
        },
      });
      const a = (await outbox.enqueue(driver.db, short, {
        payload: { n: 1 },
        dedupeKey: 'short',
      })) as { id: string };
      const b = (await outbox.enqueue(driver.db, long, {
        payload: { n: 2 },
        dedupeKey: 'long',
      })) as { id: string };
      const tick = createScheduledHandler(() => outbox);
      const consume = createQueueHandler(() => outbox);

      for (let i = 0; i < 3; i++) {
        sweeping = true;
        await tick({}, {}, {});
        sweeping = false;
        const batch = delivered
          .splice(0)
          .filter((m) => m.type === 'nudge')
          .map(asMessage);
        await consume({ queue: 'outbox', messages: batch }, {}, {});
        clock.advance(5 * 60_000);
      }

      expect(await jobRow(driver, a.id)).toMatchObject({ status: 'succeeded' });
      expect(await jobRow(driver, b.id)).toMatchObject({ status: 'succeeded' });
    });
  });

  describe('backlog monitor', () => {
    // port: outbox-depth.monitor:312
    // port: outbox-depth.monitor:335
    // port: outbox-depth.monitor:410
    // port: outbox-depth.monitor:426
    // port: outbox-depth.monitor:462
    it('runs last in the tick and grades the backlog per lane (bulk never pages)', async () => {
      const urgent = job('urgent.job');
      const bulk = job('bulk.job', { lane: 'bulk' });
      const recorded = recordingEvents();
      const outbox = createOutbox({
        jobs: [urgent, bulk],
        connect: laneConnector(driver).connect,
        events: recorded.sink,
        clock: testClock(),
      });
      await driver.db.execute(
        sql`INSERT INTO "outbox"."job" (id, kind, dedupe_key, payload, available_at)
            SELECT gen_random_uuid(), CASE WHEN g <= 10 THEN 'urgent.job' ELSE 'bulk.job' END, 'k' || g, '{"n":1}', now() + interval '1 day'
            FROM generate_series(1, 40) g`,
      );
      await driver.db.execute(
        sql`UPDATE "outbox"."job" SET available_at = now() - interval '2 hours'`,
      );

      await createScheduledHandler(() => outbox, {
        budget: { maxJobs: 0, deadlineMs: 180_000 },
        backlog: {
          thresholds: { maxDue: 5, maxAged: 5 },
          lanes: { bulk: { maxSeverity: 'warning' } },
        },
      })({}, {}, {});

      const types = recorded.events.map((e) => e.type);
      expect(types.indexOf('backlog')).toBeGreaterThan(types.lastIndexOf('cleanup'));
      const report = recorded.of('backlog')[0]?.report;
      expect(report?.lanes.find((l) => l.lane === 'default')?.severity).toBe('critical');
      expect(report?.lanes.find((l) => l.lane === 'bulk')?.severity).toBe('warning');
      const lanes = report?.lanes ?? [];
      expect(lanes.find((l) => l.lane === 'default')?.due).toBe(10);
      expect(lanes.find((l) => l.lane === 'bulk')?.due).toBe(30);
      expect(report?.totals.due).toBe(40);
      expect(report?.alert).not.toBeNull();
    });
  });

  describe('tenant scope on the outbox', () => {
    it('cancel and replay with a tenant scope never touch another tenant’s job', async () => {
      const j = job('t.job');
      const outbox = createOutbox({
        jobs: [j],
        connect: laneConnector(driver).connect,
        clock: testClock(),
      });
      const mine = (await outbox.enqueue(driver.db, j, {
        payload: { n: 1 },
        dedupeKey: 'm',
        tenantKey: 'org-a',
      })) as { id: string };

      expect(await outbox.cancel({ id: mine.id }, 'x', { tenantKey: 'org-b' })).toBe('not_found');
      expect(await outbox.cancel({ id: mine.id }, 'x', { tenantKey: 'org-a' })).toBe('cancelled');
      expect(await outbox.replay(mine.id, { tenantKey: 'org-b' })).toBe('not_found');
      expect(await outbox.replay(mine.id, { tenantKey: 'org-a' })).toBe('replayed');
      const [row] = await queryRows<{ status: string }>(
        driver.db,
        sql`SELECT status FROM "outbox"."job" WHERE id = ${mine.id}`,
      );
      expect(row?.status).toBe('pending');
    });
  });
});
