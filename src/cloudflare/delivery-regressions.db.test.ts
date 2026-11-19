import { v7 as uuidv7 } from 'uuid';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import { createQueueHandler } from '../cloudflare/queue-handler';
import { createScheduledHandler } from '../cloudflare/scheduled-handler';
import { defineJob, MAX_LEASE_MS } from '../core/define-job';
import { ok } from '../core/outcome';
import { createOutbox } from '../runtime/create-outbox';
import {
  laneConnector,
  recordingEvents,
  recordingQueue,
  testClock,
} from '../runtime/runtime-harness.test-support';
import { claimDue, getJob } from '../store/outbox-store';
import { testDrivers, type TestDriver } from '../test-support/drivers';

describe.each(testDrivers())('delivery on $name', ({ create }) => {
  let driver: TestDriver;
  beforeAll(async () => {
    driver = await create();
  });
  beforeEach(() => driver.reset());
  afterAll(() => driver.close());

  it('only the owning deployment recovers a lease until the version gate expires', async () => {
    const clock = testClock();
    const connect = laneConnector(driver).connect;
    const definition = {
      kind: 'review.version',
      payload: z.number(),
      timeoutMs: 1_000,
      handle: async () => ok(),
    };
    const oldKind = defineJob({ ...definition, retry: { maxCrashes: 1 } });
    const newKind = defineJob({ ...definition, retry: { maxCrashes: 5 } });
    const mineMs = clock.nowMs();
    const oldOutbox = createOutbox({
      jobs: [oldKind],
      clock,
      connect,
      version: { timestamp: mineMs - 60_000 },
    });
    const newOutbox = createOutbox({
      jobs: [newKind],
      clock,
      connect,
      version: { timestamp: mineMs },
    });
    const oldTick = createScheduledHandler(() => oldOutbox);
    const newTick = createScheduledHandler(() => newOutbox);

    async function crash(key: string) {
      const row = await newOutbox.enqueue(driver.db, newKind, { dedupeKey: key, payload: 0 });
      if (row.status !== 'created') throw new Error('expected a fresh job');
      // The old deployment cannot claim this pending job.
      await oldTick({}, {}, {});
      expect(await getJob(driver.db, { id: row.id })).toMatchObject({
        status: 'pending',
        startedAttempts: 0,
      });
      // This is the production claim statement. Losing the invoking process now leaves its real open attempt behind.
      const claimed = await claimDue(driver.db, {
        kinds: [{ kind: newKind.kind, leaseMs: newKind.leaseMs, fits: true }],
        now: clock.dbNow(),
        version: { mineMs, gateMs: 6 * 3_600_000 },
        trigger: 'sweep',
        limit: 1,
        leaseToken: uuidv7(),
      });
      expect(claimed.map((c) => c.id)).toEqual([row.id]);
      clock.advance(newKind.leaseMs + 1);
      return row.id;
    }

    const id = await crash('new-owner');
    await oldTick({}, {}, {});
    expect(await getJob(driver.db, { id })).toMatchObject({
      status: 'processing',
      startedAttempts: 1,
    });
    await newTick({}, {}, {});
    expect(await getJob(driver.db, { id })).toMatchObject({
      status: 'succeeded',
      startedAttempts: 2,
    });

    const rollback = await crash('rollback');
    clock.advance(6 * 3_600_000);
    await oldTick({}, {}, {});
    expect(await getJob(driver.db, { id: rollback })).toMatchObject({
      status: 'dead',
      terminalReason: 'crash_loop',
    });
  });

  it.each([false, true])(
    'runs a cron-only 100-second lease in a 120-second tick (mixed queues %s)',
    async (mixed) => {
      const clock = testClock();
      const events = recordingEvents();
      const job = defineJob({
        kind: 'review.cron-only',
        payload: z.number(),
        timeoutMs: 65_000,
        handle: async () => ok(),
      });
      const long = defineJob({
        kind: 'review.queued',
        lane: 'bulk',
        payload: z.number(),
        timeoutMs: 180_000,
        handle: async () => ok(),
      });
      const queue = recordingQueue();
      const outbox = createOutbox({
        jobs: mixed ? [job, long] : [job],
        ...(mixed ? { queue: { bulk: queue.queue } } : {}),
        connect: laneConnector(driver).connect,
        clock,
        events: events.sink,
      });
      const row = await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'once' });
      if (row.status !== 'created') throw new Error('expected a fresh job');
      const queued = mixed
        ? await outbox.enqueue(driver.db, long, { payload: 2, dedupeKey: 'long' })
        : undefined;
      await createScheduledHandler(() => outbox, { invocationBudgetMs: 120_000 })({}, {}, {});
      expect(events.of('phase_failed')).toEqual([]);
      expect(events.of('nudge_skipped')).toEqual([]);
      if (queued?.status === 'created') {
        expect(queue.sent.filter(({ message }) => message.type === 'nudge')).toMatchObject([
          { message: { ids: [queued.id] } },
        ]);
      }
      expect(await getJob(driver.db, { id: row.id })).toMatchObject({
        status: 'succeeded',
        startedAttempts: 1,
      });
    },
  );

  it('a maximum-size job completes after checkout at the minimum accepted queue budget', async () => {
    const clock = testClock();
    const events = recordingEvents();
    const queue = recordingQueue();
    const connector = laneConnector(driver);
    let checkoutMs = 21_000;
    const job = defineJob({
      kind: 'review.export',
      payload: z.number(),
      timeoutMs: 600_000,
      handle: async () => ok(),
    });
    const outbox = createOutbox({
      jobs: [job],
      connect: async () => {
        const lane = await connector.connect();
        clock.advance(checkoutMs);
        return lane;
      },
      queue: queue.queue,
      events: events.sink,
      clock,
    });
    const row = await outbox.transaction(driver.db, (_tx, enqueue) =>
      enqueue(job, { payload: 1, dedupeKey: 'export' }),
    );
    if (row.status !== 'created') throw new Error('expected a fresh job');
    async function deliver(deadlineMs: number) {
      const envelope = queue.sent.shift();
      if (!envelope) throw new Error('delivery was never sent');
      let acked = false;
      await createQueueHandler(() => outbox, {
        invocationBudgetMs: deadlineMs,
        nudgeBudget: { maxJobs: 1, deadlineMs },
      })(
        {
          queue: 'default',
          messages: [
            {
              body: envelope.message,
              attempts: 1,
              ack: () => {
                acked = true;
              },
              retry: () => {
                throw new Error('unexpected retry');
              },
            },
          ],
        },
        {},
        {},
      );
      expect(acked).toBe(true);
    }
    // After an unusually slow checkout there really is too little time to settle.
    await deliver(MAX_LEASE_MS);
    expect(events.of('nudge_miss').map((event) => event.result)).toEqual(['over_budget']);
    expect(queue.sent).toMatchObject([{ message: { ids: [row.id] }, delaySeconds: 5 }]);
    expect(await getJob(driver.db, { id: row.id })).toMatchObject({
      status: 'pending',
      startedAttempts: 0,
    });
    clock.advance(5_000);
    checkoutMs = 1;
    await deliver(MAX_LEASE_MS);
    expect(queue.sent).toEqual([]);
    expect(await getJob(driver.db, { id: row.id })).toMatchObject({
      status: 'succeeded',
      startedAttempts: 1,
    });
  });

  it('retention preserves other registries’ dedupe records until their own cutoff', async () => {
    const clock = testClock();
    const connect = laneConnector(driver).connect;
    let invoices = 0;
    const billing = defineJob({
      kind: 'billing.invoice',
      payload: z.number(),
      timeoutMs: 1_000,
      handle: async () => {
        invoices++;
        return ok();
      },
    });
    const mail = defineJob({
      kind: 'mail.send',
      payload: z.number(),
      timeoutMs: 1_000,
      handle: async () => ok(),
    });
    const billingOutbox = createOutbox({ jobs: [billing], clock, connect });
    const mailOutbox = createOutbox({ jobs: [mail], clock, connect });
    const day = 86_400_000;
    const billingTick = createScheduledHandler(() => billingOutbox, {
      retention: { succeededRetentionMs: 7 * day },
    });
    const mailTick = createScheduledHandler(() => mailOutbox, {
      retention: { succeededRetentionMs: day },
    });
    const enqueue = () =>
      billingOutbox.enqueue(driver.db, billing, { dedupeKey: 'invoice-12', payload: 12 });
    const original = await enqueue();
    if (original.status !== 'created') throw new Error('expected a fresh invoice');
    await billingTick({}, {}, {});
    clock.advance(2 * day);
    await billingTick({}, {}, {});
    expect(await enqueue()).toMatchObject({ status: 'exists', id: original.id });
    await mailTick({}, {}, {});
    expect(await enqueue()).toMatchObject({ status: 'exists', id: original.id });
    expect(invoices).toBe(1);
    clock.advance(6 * day);
    await billingTick({}, {}, {});
    expect(await getJob(driver.db, { id: original.id })).toBeNull();
  });
});
