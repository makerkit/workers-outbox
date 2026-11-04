import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import { defineJob } from '../core/define-job';
import { fail, ok, retry, type HandlerOutcome } from '../core/outcome';
import { createOutbox, RUNTIME } from '../runtime/create-outbox';
import {
  runawayCrash,
  jobRow,
  laneConnector,
  pgFault,
  recordingEvents,
  recordingQueue,
  SqlFaults,
  testClock,
} from '../runtime/runtime-harness.test-support';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { decodeMessage } from './message-codec';
import { createQueueHandler, messageRetryDelaySeconds, type QueueMessage } from './queue-handler';

function message(body: unknown, attempts = 1) {
  const state: { acked: number; retried: { delaySeconds?: number }[] } = { acked: 0, retried: [] };
  const msg: QueueMessage & { state: typeof state } = {
    body,
    attempts,
    ack: () => {
      state.acked++;
    },
    retry: (options) => {
      state.retried.push(options ?? {});
    },
    state,
  };

  return msg;
}

describe.each(testDrivers())('queue handler on $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  function setup(
    options: {
      behavior?: () => Promise<HandlerOutcome>;
      faults?: SqlFaults;
      noQueue?: boolean;
      queueFails?: boolean;
    } = {},
  ) {
    const calls: string[] = [];
    const job = defineJob({
      kind: 'test.job',
      payload: z.object({ n: z.number() }),
      timeoutMs: 1_000,
      retry: { maxAttempts: 5, backoff: { baseMs: 10_000, maxMs: 60_000 } },
      handle: async (_, ctx) => {
        calls.push(ctx.jobId);

        return options.behavior ? options.behavior() : ok();
      },
    });
    const recorded = recordingEvents();
    const { queue, sent } = recordingQueue(
      options.queueFails ? { fail: () => new Error('queue down') } : {},
    );
    const clock = testClock();
    const lanes = laneConnector(driver, options.faults);
    const outbox = createOutbox({
      jobs: [job],
      connect: lanes.connect,
      events: recorded.sink,
      clock,
      ...(options.noQueue ? {} : { queue }),
    });
    const handler = createQueueHandler(() => outbox);
    const enqueue = async (n = 1) => {
      const r = await outbox.enqueue(driver.db, job, {
        payload: { n },
        dedupeKey: `k:${Math.random()}`,
      });

      return (r as { id: string }).id;
    };
    const run = (...messages: QueueMessage[]) => handler({ queue: 'outbox', messages }, {}, {});

    return { job, calls, ...recorded, sent, clock, outbox, handler, enqueue, run };
  }

  describe('nudges', () => {
    // port: submission-outbox-queue.server:14
    // port: submission-outbox-queue.server:394
    it('acks every nudge whether it ran a job or missed, and reports misses', async () => {
      const t = setup();
      const due = await t.enqueue();
      const missing = '0190b2a0-0000-7000-8000-000000000000';
      const m = message({ v: 1, type: 'nudge', ids: [due, missing] });

      await t.run(m);

      expect(m.state).toEqual({ acked: 1, retried: [] });
      expect(t.calls).toEqual([due]);
      expect(t.of('nudge_miss')).toEqual([
        { type: 'nudge_miss', jobId: missing, result: 'not_found' },
      ]);
    });

    // port: process-submission-outbox.service:2840
    // port: submission-outbox-queue.server:369
    it('claims exactly the nudged ids and never a substitute, even when other jobs are due', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      const other = await t.enqueue(1);
      const nudged = await t.enqueue(2);

      await t.run(message({ v: 1, type: 'nudge', ids: [nudged] }));

      expect(t.calls).toEqual([nudged]);
      expect(await jobRow(driver, other)).toMatchObject({ status: 'pending' });
      const claims = faults.statements.filter((s) => s.includes('picked AS'));
      expect(claims.length).toBeGreaterThan(0);
      expect(claims.every((s) => s.includes('req AS'))).toBe(true);
    });

    // port: submission-outbox-queue.server:86
    // port: submission-outbox-queue.server:465
    it('acks the message and sends a delayed targeted nudge when the job settles retrying', async () => {
      const t = setup({ behavior: async () => retry({ code: 'busy', after: 12_300 }) });
      const id = await t.enqueue();
      const m = message({ v: 1, type: 'nudge', ids: [id] });

      await t.run(m);

      expect(m.state.acked).toBe(1);
      expect(t.sent).toEqual([{ message: { v: 1, type: 'nudge', ids: [id] }, delaySeconds: 13 }]);
    });

    // port: submission-outbox-queue.server:210
    // port: submission-outbox-queue.server:233
    it('retries the message with a delay when the database is unreachable, after reporting the error', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      const id = await t.enqueue();
      faults.failNext(/req AS/, () => pgFault('08006', 'connection failure'));
      const m = message({ v: 1, type: 'nudge', ids: [id] }, 3);

      await t.run(m);

      expect(m.state).toEqual({
        acked: 0,
        retried: [{ delaySeconds: messageRetryDelaySeconds(3) }],
      });
      expect(t.of('message_retried')).toEqual([
        expect.objectContaining({
          messageType: 'nudge',
          attempts: 3,
          error: expect.objectContaining({ stack: expect.any(String) }),
        }),
      ]);
      expect(t.calls).toEqual([]);
    });

    it('treats a handler throw as a job outcome, never a message retry', async () => {
      const t = setup({
        behavior: async () => {
          throw new Error('provider exploded');
        },
      });
      const id = await t.enqueue();
      const m = message({ v: 1, type: 'nudge', ids: [id] });

      await t.run(m);

      expect(m.state).toEqual({ acked: 1, retried: [] });
      expect(await jobRow(driver, id)).toMatchObject({
        status: 'retrying',
        last_error_code: 'unexpected',
      });
    });

    // port: submission-outbox-queue.server:261
    it('retries only the failing message; the rest of the batch still acks', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      const a = await t.enqueue(1);
      const b = await t.enqueue(2);
      faults.failNext(/req AS/, () => pgFault('08006'));
      const first = message({ v: 1, type: 'nudge', ids: [a] });
      const second = message({ v: 1, type: 'nudge', ids: [b] });

      await t.run(first, second);

      expect(first.state.retried).toHaveLength(1);
      expect(second.state).toEqual({ acked: 1, retried: [] });
      expect(t.calls).toEqual([b]);
    });

    // port: submission-outbox-queue.server:289
    // port: submission-outbox-queue.server:819
    it('reports each received message, and transition events carry their trigger', async () => {
      const t = setup();
      const id = await t.enqueue();

      await t.run(message({ v: 1, type: 'nudge', ids: [id] }, 2));

      expect(t.of('message_received')).toEqual([
        { type: 'message_received', messageType: 'nudge', ids: [id], attempts: 2 },
      ]);
      expect(t.of('transition')[0]).toMatchObject({ trigger: 'nudge', kind: 'test.job' });
    });

    // port: submission-outbox-queue.server:332
    it('acks and reports unreadable bodies without claiming anything', async () => {
      const t = setup();
      await t.enqueue();
      const bodies = [
        null,
        'text',
        { v: 2, type: 'nudge', ids: ['x'] },
        { v: 1, type: 'nudge', ids: [] },
        { v: 1, type: 'other' },
      ];
      const messages = bodies.map((b) => message(b));

      await t.run(...messages);

      expect(messages.every((m) => m.state.acked === 1)).toBe(true);
      expect(t.of('message_rejected').map((e) => e.reason)).toEqual([
        'malformed',
        'malformed',
        'unknown_version',
        'malformed',
        'malformed',
      ]);
      expect(t.calls).toEqual([]);
    });
  });

  describe('drain continuations', () => {
    const drainMessage = () => message({ v: 1, type: 'drain', lane: 'default' });

    // port: submission-outbox-queue.server:439
    // port: submission-outbox-queue.server:563
    it('a drain message runs a bounded drain and sends no continuation once nothing is due', async () => {
      const t = setup();
      const ids = [await t.enqueue(1), await t.enqueue(2)];

      await t.run(drainMessage());

      expect(t.calls.sort()).toEqual(ids.sort());
      expect(t.of('continuation_sent')).toEqual([]);
    });

    // port: submission-outbox-queue.server:602
    // port: dispatch-sweep:372
    it('chains exactly one continuation while a backlog remains', async () => {
      const t = setup();
      for (let i = 0; i < 60; i++) {
        await t.enqueue(i);
      }

      await t.run(drainMessage());

      expect(t.calls).toHaveLength(50);
      expect(t.sent).toEqual([
        { message: { v: 1, type: 'drain', lane: 'default' }, delaySeconds: undefined },
      ]);
    });

    // port: submission-outbox-queue.server:639
    it('sends no continuation when the only backlog is not due yet', async () => {
      const t = setup({ behavior: async () => retry({ code: 'later', after: 3_600_000 }) });
      await t.enqueue();

      await t.run(drainMessage());

      expect(t.of('continuation_sent')).toEqual([]);
    });

    // port: submission-outbox-queue.server:671
    it('without a queue binding, a needed continuation is reported and the handler resolves', async () => {
      const t = setup({ noQueue: true });
      for (let i = 0; i < 51; i++) {
        await t.enqueue(i);
      }
      const m = drainMessage();

      await t.run(m);

      expect(m.state.acked).toBe(1);
      expect(t.of('continuation_skipped')).toEqual([
        { type: 'continuation_skipped', lane: 'default', reason: 'no_queue' },
      ]);
    });

    // port: submission-outbox-queue.server:704
    it('a failed continuation or nudge send is only an event: cron is the backstop', async () => {
      const t = setup({ queueFails: true, behavior: async () => retry({ code: 'busy' }) });
      for (let i = 0; i < 51; i++) {
        await t.enqueue(i);
      }
      const m = drainMessage();

      await expect(t.run(m)).resolves.toBeUndefined();

      expect(m.state.acked).toBe(1);
      expect(t.of('continuation_failed')).toHaveLength(1);
      expect(t.of('nudge_failed').length).toBeGreaterThan(0);
    });

    // port: submission-outbox-queue.server:739
    it('a database outage during a drain retries the message and sends no continuation', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      await t.enqueue();
      faults.failNext(/picked AS/, () => pgFault('08006'));
      const m = drainMessage();

      await t.run(m);

      expect(m.state.retried).toHaveLength(1);
      expect(t.of('continuation_sent')).toEqual([]);
    });

    // port: submission-outbox-queue.server:778
    it('acks a drain message whatever its jobs did', async () => {
      const t = setup({ behavior: async () => fail({ code: 'nope' }) });
      await t.enqueue();
      const m = drainMessage();

      await t.run(m);

      expect(m.state).toEqual({ acked: 1, retried: [] });
    });

    // port: dispatch-sweep:392
    it('a drain whose every job errored in the runtime sends no continuation', async () => {
      // Every job runs away and its lane's reconnect fails: an infra error per job.
      const crash = runawayCrash(laneConnector(driver).connect);
      const exploding = defineJob({
        kind: 'explode.job',
        payload: z.unknown(),
        timeoutMs: 1,
        handle: () => crash.runaway(),
      });
      const recorded = recordingEvents();
      const { queue, sent } = recordingQueue();
      const outbox = createOutbox({
        jobs: [exploding],
        connect: crash.connect,
        events: recorded.sink,
        queue,
        clock: testClock(),
      });
      await driver.db.execute(
        sql`INSERT INTO "outbox"."job" (id, kind, dedupe_key, payload, available_at) SELECT gen_random_uuid(), 'explode.job', 'x' || g, '{}', now() - interval '1 hour' FROM generate_series(1, 3) g`,
      );

      await createQueueHandler(() => outbox)(
        { queue: 'outbox', messages: [drainMessage()] },
        {},
        {},
      );

      expect(crash.crashes()).toBe(3);
      expect(sent).toEqual([]);
      expect(outbox[RUNTIME]).toBeDefined();
    });
  });

  describe('nudge sender', () => {
    // port: nudge-bulk-jobs:48
    it('sends ids-only messages, chunked to stay far below the message size limit', async () => {
      const t = setup();
      const ids = Array.from({ length: 205 }, (_, i) => `id-${i}`);

      await t.outbox.nudge(ids);

      expect(t.sent.map((s) => (s.message as { ids: string[] }).ids.length)).toEqual([100, 100, 5]);
      expect(t.sent[0]?.message).toEqual({ v: 1, type: 'nudge', ids: ids.slice(0, 100) });
    });

    // port: submission-outbox-queue.server:108
    // port: submission-outbox-queue.server:128
    // port: process-submission-outbox.service:752
    // port: submission-outbox-queue.server:145
    it('clamps retry nudge delays to the queue range [0, 86 400] s, and a non-finite delay to 0', async () => {
      const t = setup();
      const runtime = t.outbox[RUNTIME];

      await runtime.nudge(['a'], 200_000);
      await runtime.nudge(['b'], -5);
      await runtime.nudge(['c'], Number.NaN);

      expect(t.sent.map((s) => s.delaySeconds)).toEqual([86_400, undefined, undefined]);
    });
  });

  describe('message codec', () => {
    // port: nudge-bulk-jobs:75
    it('round-trips nudge and drain messages with only their defined fields', () => {
      expect(decodeMessage({ v: 1, type: 'nudge', ids: ['a'], extra: 'dropped' })).toEqual({
        ok: true,
        message: { v: 1, type: 'nudge', ids: ['a'] },
      });
      expect(
        decodeMessage(JSON.parse(JSON.stringify({ v: 1, type: 'drain', lane: 'bulk' }))),
      ).toEqual({
        ok: true,
        message: { v: 1, type: 'drain', lane: 'bulk' },
      });
      expect(decodeMessage({ v: 1, type: 'nudge', ids: [1] })).toMatchObject({
        ok: false,
        reason: 'malformed',
      });
    });
  });
});
