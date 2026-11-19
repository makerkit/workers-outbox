import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import { defineJob } from '../core/define-job';
import { ok } from '../core/outcome';
import { createOutbox } from '../runtime/create-outbox';
import {
  laneConnector,
  recordingEvents,
  recordingQueue,
  testClock,
} from '../runtime/runtime-harness.test-support';
import { getJob } from '../store/outbox-store';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { createQueueHandler } from './queue-handler';
import { createScheduledHandler } from './scheduled-handler';

describe.each(testDrivers())('pre-production liveness: $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });
  beforeEach(() => driver.reset());
  afterAll(() => driver.close());

  it('runs a reserved cron-only job before a slow queue handoff can spend its claim window', async () => {
    const clock = testClock();
    const order: string[] = [];
    const billing = defineJob({
      kind: 'billing',
      payload: z.number(),
      timeoutMs: 9_500,
      handle: async () => {
        order.push('billing');
        return ok();
      },
    });
    const queued = defineJob({
      kind: 'export',
      lane: 'bulk',
      payload: z.number(),
      timeoutMs: 1_000,
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [billing, queued],
      clock,
      connect: laneConnector(driver).connect,
      queue: {
        bulk: {
          send: async () => {
            order.push('send');
            clock.advance(750);
          },
        },
      },
    });
    const row = await outbox.enqueue(driver.db, billing, { payload: 1, dedupeKey: 'billing' });
    if (row.status !== 'created') throw new Error('expected a billing job');
    await outbox.enqueue(driver.db, queued, { payload: 2, dedupeKey: 'export' });
    await createScheduledHandler(() => outbox, {
      invocationBudgetMs: 45_000,
      reserved: [
        { name: 'billing', kinds: [billing.kind], budget: { maxJobs: 1, deadlineMs: 45_000 } },
      ],
    })({}, {}, {});
    expect(order).toEqual(['billing', 'send']);
    expect(await getJob(driver.db, { id: row.id })).toMatchObject({
      status: 'succeeded',
      startedAttempts: 1,
    });
  });

  it('hands off the remainder after the sweep spends its own budget', async () => {
    const clock = testClock();
    const queue = recordingQueue();
    const job = defineJob({
      kind: 'audit.sweep-window',
      payload: z.number(),
      timeoutMs: 60_000,
      handle: async () => {
        clock.advance(60_000);
        return ok();
      },
    });
    const outbox = createOutbox({
      jobs: [job],
      connect: laneConnector(driver).connect,
      clock,
      queue: queue.queue,
    });
    await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'first' });
    const next = await outbox.enqueue(driver.db, job, { payload: 2, dedupeKey: 'next' });
    if (!('id' in next)) throw new Error('enqueue refused');
    await createScheduledHandler(() => outbox, { budget: { maxJobs: 10, deadlineMs: 100_000 } })(
      {},
      {},
      {},
    );
    expect(
      queue.sent.some(({ message }) => message.type === 'nudge' && message.ids.includes(next.id)),
    ).toBe(true);
    expect(await getJob(driver.db, { id: next.id })).toMatchObject({ status: 'pending' });
  });

  it('hands off jobs when reserved work leaves too little time for the catch-all sweep', async () => {
    const clock = testClock();
    const queue = recordingQueue();
    const medium = defineJob({
      kind: 'audit.reserved-medium',
      payload: z.number(),
      timeoutMs: 60_000,
      handle: async () => ok(),
    });
    const short = defineJob({
      kind: 'audit.reserved-short',
      payload: z.number(),
      timeoutMs: 30_000,
      handle: async () => {
        clock.advance(30_000);
        return ok();
      },
    });
    const outbox = createOutbox({
      jobs: [medium, short],
      connect: laneConnector(driver).connect,
      clock,
      queue: queue.queue,
    });
    const next = await outbox.enqueue(driver.db, medium, { payload: 1, dedupeKey: 'medium' });
    if (!('id' in next)) throw new Error('enqueue refused');
    for (let i = 0; i < 5; i++)
      await outbox.enqueue(driver.db, short, { payload: i, dedupeKey: `short-${i}` });
    const start = clock.nowMs();
    await createScheduledHandler(() => outbox, {
      invocationBudgetMs: 180_000,
      reserved: [
        { name: 'short', kinds: [short.kind], budget: { maxJobs: 10, deadlineMs: 180_000 } },
      ],
    })({}, {}, {});
    expect(clock.nowMs() - start).toBeLessThan(180_000);
    expect(
      queue.sent.some(({ message }) => message.type === 'nudge' && message.ids.includes(next.id)),
    ).toBe(true);
  });

  it.each([
    {
      label: '180 s invocation',
      invocationBudgetMs: 180_000,
      scheduleCount: 2,
      scheduleMs: 45_000,
      timeoutMs: 60_000,
    },
    {
      label: 'default 840 s invocation',
      invocationBudgetMs: 840_000,
      scheduleCount: 13,
      scheduleMs: 55_000,
      timeoutMs: 100_000,
    },
  ])(
    'F1: repeated shortened sweeps hand off a due job with its correct lane ($label)',
    async ({ invocationBudgetMs, scheduleCount, scheduleMs, timeoutMs }) => {
      const clock = testClock();
      const queue = recordingQueue();
      const events = recordingEvents();
      let calls = 0;
      const job = defineJob({
        kind: 'audit.medium',
        lane: 'bulk',
        payload: z.number(),
        timeoutMs, // Fits the configured 180 s sweep, but not the window left by schedules.
        handle: async () => {
          calls++;
          return ok();
        },
      });
      const outbox = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        clock,
        queue: { bulk: queue.queue },
        events: events.sink,
      });
      const created = await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'lost-nudge' });
      if (!('id' in created)) throw new Error('enqueue refused');
      const cron = '*/5 * * * *';
      const tick = createScheduledHandler(() => outbox, {
        invocationBudgetMs,
        schedules: Array.from({ length: scheduleCount }, (_, n) => ({
          name: `slow-${n}`,
          cron,
          // Each transaction stays under operationTimeoutMs; the last may cross the cutoff.
          run: async () => {
            clock.advance(scheduleMs);
          },
        })),
      });

      for (let i = 0; i < 3; i++) {
        await tick({ cron, scheduledTime: clock.nowMs() }, {}, {});
        clock.advance(300_000);
      }

      expect(calls).toBe(0);
      expect(queue.sent.length).toBeGreaterThan(0);
      expect(
        queue.sent.every(
          ({ message }) => message.type === 'nudge' && message.ids.includes(created.id),
        ),
      ).toBe(true);
      expect(events.of('schedule_ran')).toHaveLength(scheduleCount * 3);
      expect(events.of('schedule_failed')).toEqual([]);
      expect(events.of('phase_failed')).toEqual([]);
      expect(await getJob(driver.db, { id: created.id })).toMatchObject({
        status: 'pending',
        startedAttempts: 0,
      });
      expect(events.of('sweep_finished').map((e) => e.report.reachedDeadline)).toEqual([
        true,
        true,
        true,
      ]);
      // Deliver only the messages actually produced by the scheduled handler.
      await createQueueHandler(() => outbox, { lane: 'bulk' })(
        {
          queue: 'bulk',
          messages: queue.sent.map(({ message }) => ({
            body: message,
            attempts: 1,
            ack() {},
            retry() {
              throw new Error('unexpected queue retry');
            },
          })),
        },
        {},
        {},
      );
      expect(calls).toBe(1);
      expect(await getJob(driver.db, { id: created.id })).toMatchObject({ status: 'succeeded' });
    },
  );
});
