import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import type { TransitionHook } from './runtime';
import type { OutboxDb } from './types';

import { computeBackoff } from '../core/backoff';
import { defineJob, type JobContext } from '../core/define-job';
import { cancel, fail, ok, retry, type HandlerOutcome } from '../core/outcome';
import { queryRows } from '../drizzle/catalog';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { createOutbox, RUNTIME } from './create-outbox';
import {
  runawayCrash,
  attemptRows,
  jobRow,
  laneConnector,
  pgFault,
  recordingEvents,
  recordingQueue,
  SqlFaults,
  connectionReset,
  testClock,
} from './runtime-harness.test-support';

const BUDGET = { maxJobs: 50, deadlineMs: 180_000 };
const SETTLE = /settled AS \(/;

type Behavior = (payload: { n: number }, ctx: JobContext) => Promise<HandlerOutcome>;

function makeJob(
  kind = 'test.job',
  options: {
    timeoutMs?: number;
    retry?: Parameters<typeof defineJob>[0]['retry'];
    classify?: (e: unknown) => HandlerOutcome;
  } = {},
) {
  const state = { behavior: (async () => ok()) as Behavior, calls: 0 };
  const job = defineJob({
    kind,
    payload: z.object({ n: z.number() }),
    timeoutMs: options.timeoutMs ?? 1_000,
    retry: options.retry ?? { maxAttempts: 3, backoff: { baseMs: 1_000, maxMs: 60_000 } },
    ...(options.classify ? { classify: options.classify } : {}),
    handle: async (payload, ctx) => {
      state.calls++;

      return state.behavior(payload, ctx);
    },
  });

  return { job, state };
}

describe.each(testDrivers())('runtime on $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  function setup(
    options: {
      hook?: TransitionHook;
      faults?: SqlFaults;
      warnAtStreak?: number;
      jobOptions?: Parameters<typeof makeJob>[1];
      kind?: string;
      redactError?: (m: string) => string;
    } = {},
  ) {
    const { job, state } = makeJob(options.kind, options.jobOptions);
    const { queue, sent } = recordingQueue();
    const recorded = recordingEvents();
    const clock = testClock();
    const lanes = laneConnector(driver, options.faults);
    const outbox = createOutbox({
      jobs: [job],
      connect: lanes.connect,
      queue,
      events: recorded.sink,
      clock,
      ...(options.hook ? { onTransition: options.hook } : {}),
      ...(options.warnAtStreak !== undefined ? { warnAtStreak: options.warnAtStreak } : {}),
      ...(options.redactError ? { redactError: options.redactError } : {}),
    });
    const runtime = outbox[RUNTIME];

    const enqueue = async (n = 1, dedupeKey = `k:${Math.random()}`) => {
      const result = await outbox.enqueue(driver.db, job, { payload: { n }, dedupeKey });

      if (result.status !== 'created') {
        throw new Error(`enqueue failed: ${result.status}`);
      }

      return result.id;
    };

    return { job, state, queue, sent, ...recorded, clock, lanes, outbox, runtime, enqueue };
  }

  describe('outcomes', () => {
    // port: submission-job.repository.db:114
    // port: process-submission-outbox.service:71
    it('settles ok() as succeeded and closes the attempt', async () => {
      const t = setup();
      const id = await t.enqueue();

      const report = await t.runtime.drain(BUDGET, 'sweep');

      expect(report).toMatchObject({ claimed: 1, succeeded: 1 });
      expect(await jobRow(driver, id)).toMatchObject({
        status: 'succeeded',
        started_attempts: 1,
        finished_attempts: 1,
        spent_attempts: 1,
      });
      expect(await attemptRows(driver, id)).toEqual([
        expect.objectContaining({ outcome: 'succeeded', finished_at: expect.anything() }),
      ]);
    });

    // port: process-submission-outbox.service:51
    it('does nothing when no job is due', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });

      expect(await t.runtime.drain(BUDGET, 'sweep')).toMatchObject({ claimed: 0, succeeded: 0 });
      expect(faults.statements.some((s) => SETTLE.test(s))).toBe(false);
    });

    // port: submission-job.repository.db:196
    // port: process-submission-outbox.service:290
    it('settles cancel({ reason }) as cancelled with the reason', async () => {
      const t = setup();
      t.state.behavior = async () => cancel({ reason: 'integration_disabled' });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(await jobRow(driver, id)).toMatchObject({
        status: 'cancelled',
        terminal_reason: 'integration_disabled',
      });
      expect((await attemptRows(driver, id))[0]).toMatchObject({ outcome: 'cancelled' });
    });

    // port: submission-job.repository.db:276
    // port: process-submission-outbox.service:583
    // port: process-submission-outbox.service:2372
    it('settles a retry on the database clock and sends a delayed targeted nudge', async () => {
      const t = setup();
      t.state.behavior = async () => retry({ code: 'http_503', message: 'unavailable' });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      const row = await jobRow(driver, id);
      const delayMs = computeBackoff(t.job.retry, id, 1);

      expect(row).toMatchObject({
        status: 'retrying',
        last_error_code: 'http_503',
        last_error_message: 'unavailable',
      });
      expect(new Date(String(row!['next_attempt_at'])).getTime()).toBe(
        t.clock.current().getTime() + delayMs,
      );
      expect((await attemptRows(driver, id))[0]).toMatchObject({
        outcome: 'retry',
        code: 'http_503',
      });
      expect(t.sent).toEqual([
        { message: { v: 1, type: 'nudge', ids: [id] }, delaySeconds: Math.ceil(delayMs / 1_000) },
      ]);
    });

    // port: process-submission-outbox.service:1898
    // port: process-submission-outbox.service:1942
    // port: process-submission-outbox.service:2445
    it('dead-letters fail() on the first attempt with its code, reporting exactly one dead event', async () => {
      const t = setup();
      t.state.behavior = async () => fail({ code: 'http_404' });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(await jobRow(driver, id)).toMatchObject({
        status: 'dead',
        terminal_reason: 'failed',
        last_error_code: 'http_404',
      });
      expect(t.of('transition')).toEqual([
        expect.objectContaining({ to: 'dead', reason: 'failed', severity: 'error' }),
      ]);
      expect(t.sent).toEqual([]);
    });

    // port: process-submission-outbox.service:829
    it('retries a payload this deploy cannot parse a few times before dead-lettering it', async () => {
      // During a gradual deploy an old Worker may claim a job enqueued in a newer shape.
      const t = setup({ jobOptions: { retry: { maxAttempts: 10 } } });
      const id = await t.enqueue();
      await driver.db.execute(
        sql`UPDATE "outbox"."job" SET payload = '{"n":"not a number"}' WHERE id = ${id}`,
      );

      for (let i = 0; i < 3; i++) {
        await t.runtime.drain(BUDGET, 'sweep');
        t.clock.advance(3_600_000);
      }

      expect(t.state.calls).toBe(0);
      expect(t.of('transition').map((e) => e.to)).toEqual(['retrying', 'retrying', 'dead']);
      expect(await jobRow(driver, id)).toMatchObject({
        status: 'dead',
        terminal_reason: 'exhausted',
        last_error_code: 'invalid_payload',
      });
    });
  });

  describe('thrown errors', () => {
    // port: token-replay-window.dispatcher:94
    it('persists the classified code of a thrown error', async () => {
      const t = setup({ jobOptions: { classify: () => retry({ code: 'token_replay' }) } });
      t.state.behavior = async () => {
        throw new Error('token already used');
      };
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(await jobRow(driver, id)).toMatchObject({
        status: 'retrying',
        last_error_code: 'token_replay',
      });
      expect((await attemptRows(driver, id))[0]).toMatchObject({ code: 'token_replay' });
    });

    // port: token-replay-window.dispatcher:134
    it('retries an unclassified throw with the default code and the default backoff', async () => {
      const t = setup();
      t.state.behavior = async () => {
        throw new Error('socket hang up');
      };
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(await jobRow(driver, id)).toMatchObject({
        status: 'retrying',
        last_error_code: 'unexpected',
        last_error_message: 'socket hang up',
      });
      expect(t.of('transition')[0]).toMatchObject({
        delayMs: computeBackoff(t.job.retry, id, 1),
        severity: 'warn',
      });
    });

    // port: process-submission-outbox.service:534
    it('honours a classify() that cancels instead of failing', async () => {
      const t = setup({ jobOptions: { classify: () => cancel({ reason: 'resource_gone' }) } });
      t.state.behavior = async () => {
        throw new Error('404');
      };
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(await jobRow(driver, id)).toMatchObject({
        status: 'cancelled',
        terminal_reason: 'resource_gone',
      });
    });

    // port: process-submission-outbox.service:2406
    // port: process-submission-outbox.service:2491
    it('honours a returned outcome as-is; classify runs only for throws', async () => {
      let classified = 0;
      const t = setup({
        jobOptions: {
          classify: () => {
            classified++;

            return fail({ code: 'never' });
          },
        },
      });
      t.state.behavior = async () => retry({ code: 'not_ready' });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(classified).toBe(0);
      expect(await jobRow(driver, id)).toMatchObject({
        status: 'retrying',
        last_error_code: 'not_ready',
      });
      expect(t.of('handler_threw')).toEqual([]);
      expect(t.of('transition')[0]).toMatchObject({ to: 'retrying', severity: 'info' });
    });

    // port: process-submission-outbox.service:1432
    // port: process-submission-outbox.service:1477
    it('reports the unredacted error with stack and cause in the event, but persists a sanitized message', async () => {
      const t = setup({ redactError: (m) => m.replace(/secret-\w+/g, '[redacted]') });
      t.state.behavior = async () => {
        throw new Error('call failed secret-abc\nsecond line', { cause: new Error('ECONNRESET') });
      };
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(t.of('handler_threw')[0]?.error).toMatchObject({
        message: 'call failed secret-abc\nsecond line',
        stack: expect.any(String),
        cause: { message: 'ECONNRESET' },
      });
      expect(await jobRow(driver, id)).toMatchObject({
        last_error_message: 'call failed [redacted] second line',
      });
    });
  });

  describe('budgets and events', () => {
    // port: process-submission-outbox.service:1570
    // port: process-submission-outbox.service:1523
    it('reports the final exhausted retry as dead, distinct from the retrying events before it', async () => {
      const t = setup();
      t.state.behavior = async () => retry({ code: 'http_503' });
      const id = await t.enqueue();

      for (let attempt = 0; attempt < 3; attempt++) {
        await t.runtime.drain(BUDGET, 'sweep');
        t.clock.advance(120_000);
      }

      expect(await jobRow(driver, id)).toMatchObject({
        status: 'dead',
        terminal_reason: 'exhausted',
      });
      expect(t.of('transition').map((e) => [e.to, e.severity])).toEqual([
        ['retrying', 'info'],
        ['retrying', 'info'],
        ['dead', 'error'],
      ]);
    });

    // port: process-submission-outbox.service:1617
    // port: process-submission-outbox.service:2208
    // port: process-submission-outbox.service:2245
    it('carries attempt, budget and streak on retrying events so sinks can escalate', async () => {
      const t = setup();
      t.state.behavior = async () => retry({ code: 'http_503' });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');
      t.clock.advance(120_000);
      await t.runtime.drain(BUDGET, 'sweep');

      expect(t.of('transition')[1]).toMatchObject({
        jobId: id,
        to: 'retrying',
        attemptNo: 2,
        attemptsUsed: 2,
        maxAttempts: 3,
        code: 'http_503',
        codeStreak: 2,
      });
    });

    // port: process-submission-outbox.service:1838
    it('dead-letters a retry whose per-code cap is reached and reports it', async () => {
      const t = setup({ jobOptions: { retry: { maxAttempts: 10, perCode: { rate_limited: 2 } } } });
      t.state.behavior = async () => retry({ code: 'rate_limited' });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');
      t.clock.advance(3_600_000);
      await t.runtime.drain(BUDGET, 'sweep');

      expect(await jobRow(driver, id)).toMatchObject({
        status: 'dead',
        terminal_reason: 'exhausted',
        code_streak: 2,
      });
      expect(t.of('transition').at(-1)).toMatchObject({ to: 'dead', reason: 'exhausted' });
    });

    // port: process-submission-outbox.service:959
    // port: process-submission-outbox.service:1024
    // port: process-submission-outbox.service:1128
    it('emits attempt_threshold exactly once, when the same-code streak reaches the threshold', async () => {
      const t = setup({ warnAtStreak: 2, jobOptions: { retry: { maxAttempts: 10 } } });
      const codes = ['a', 'b', 'b', 'b'];
      let call = 0;
      t.state.behavior = async () => retry({ code: codes[call++] ?? 'b' });
      await t.enqueue();

      for (let i = 0; i < 4; i++) {
        await t.runtime.drain(BUDGET, 'sweep');
        t.clock.advance(3_600_000);
      }

      expect(t.of('attempt_threshold')).toEqual([
        expect.objectContaining({ code: 'b', codeStreak: 2, threshold: 2 }),
      ]);
    });

    // port: process-submission-outbox.service:1076
    it('emits no attempt_threshold events unless opted in', async () => {
      const t = setup({ jobOptions: { retry: { maxAttempts: 10 } } });
      t.state.behavior = async () => retry({ code: 'b' });
      await t.enqueue();

      for (let i = 0; i < 3; i++) {
        await t.runtime.drain(BUDGET, 'sweep');
        t.clock.advance(3_600_000);
      }

      expect(t.of('attempt_threshold')).toEqual([]);
    });
  });

  describe('timeouts', () => {
    // port: process-submission-outbox.service:2890
    it('retries a handler that never resolves as a timeout and discards its late result', async () => {
      const t = setup({ jobOptions: { timeoutMs: 20 } });
      let seenSignal: AbortSignal | undefined;
      t.state.behavior = (_, ctx) => {
        seenSignal = ctx.signal;

        return new Promise(() => undefined);
      };
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(seenSignal?.aborted).toBe(true);
      expect(await jobRow(driver, id)).toMatchObject({
        status: 'retrying',
        last_error_code: 'timeout',
      });
      expect((await attemptRows(driver, id))[0]).toMatchObject({ outcome: 'timeout' });
      expect(t.of('handler_timeout')).toHaveLength(1);
      expect(t.lanes.opened.length).toBeGreaterThan(1);
    });

    // port: process-submission-outbox.service:2921
    it('settles a handler that finishes before its deadline as succeeded', async () => {
      const t = setup({ jobOptions: { timeoutMs: 1_000 } });
      t.state.behavior = async () => {
        await new Promise((resolve) => setTimeout(resolve, 5));

        return ok();
      };
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(await jobRow(driver, id)).toMatchObject({ status: 'succeeded' });
      expect(t.of('handler_timeout')).toEqual([]);
    });

    // port: process-submission-outbox.service:2954
    // port: process-submission-outbox.service:3014
    it("honours each kind's own timeout", async () => {
      const short = makeJob('short.job', { timeoutMs: 10 });
      const long = makeJob('long.job', { timeoutMs: 1_000 });
      const slow: Behavior = async () => {
        await new Promise((resolve) => setTimeout(resolve, 60));

        return ok();
      };
      short.state.behavior = slow;
      long.state.behavior = slow;
      const recorded = recordingEvents();
      const outbox = createOutbox({
        jobs: [short.job, long.job],
        connect: laneConnector(driver).connect,
        events: recorded.sink,
        clock: testClock(),
      });
      const a = await outbox.enqueue(driver.db, short.job, {
        payload: { n: 1 },
        dedupeKey: 'short',
      });
      const b = await outbox.enqueue(driver.db, long.job, { payload: { n: 1 }, dedupeKey: 'long' });

      await outbox[RUNTIME].drain(BUDGET, 'sweep');

      expect(await jobRow(driver, (a as { id: string }).id)).toMatchObject({
        status: 'retrying',
        last_error_code: 'timeout',
      });
      expect(await jobRow(driver, (b as { id: string }).id)).toMatchObject({ status: 'succeeded' });
    });
  });

  describe('lease fence', () => {
    const stealLease = (id: string) =>
      driver.db.execute(
        sql`UPDATE "outbox"."job" SET lease_token = gen_random_uuid() WHERE id = ${id}`,
      );

    // port: submission-job-transition.service:629
    // port: submission-job-transition.service:977
    // port: process-submission-outbox.service:681
    // port: process-submission-outbox.service:2637
    it('a stale dead settle is lease_lost: no dead event, no hook, the peer state stands', async () => {
      const hooked: string[] = [];
      const t = setup({ hook: async (_, e) => void hooked.push(e.transition.to) });
      let id = '';
      t.state.behavior = async () => {
        await stealLease(id);

        return fail({ code: 'boom' });
      };
      id = await t.enqueue();

      const report = await t.runtime.drain(BUDGET, 'sweep');

      expect(report).toMatchObject({ leaseLost: 1, dead: 0 });
      expect(hooked).toEqual([]);
      expect(t.of('transition')).toEqual([]);
      expect(t.of('lease_lost')).toEqual([expect.objectContaining({ jobId: id })]);
      expect(await jobRow(driver, id)).toMatchObject({ status: 'processing' });
    });

    // port: submission-job-transition.service:850
    // port: process-submission-outbox.service:643
    it('a stale retry settle is lease_lost and sends no redelivery nudge', async () => {
      const t = setup();
      let id = '';
      t.state.behavior = async () => {
        await stealLease(id);

        return retry({ code: 'http_503' });
      };
      id = await t.enqueue();

      expect(await t.runtime.drain(BUDGET, 'sweep')).toMatchObject({ leaseLost: 1, retrying: 0 });
      expect(t.sent).toEqual([]);
    });

    // port: submission-job-transition.service:1102
    // port: process-submission-outbox.service:2744
    it('a stale cancel settle is lease_lost and the hook never fires', async () => {
      const hooked: string[] = [];
      const t = setup({ hook: async (_, e) => void hooked.push(e.transition.to) });
      let id = '';
      t.state.behavior = async () => {
        await stealLease(id);

        return cancel({ reason: 'x' });
      };
      id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(hooked).toEqual([]);
    });

    // port: submission-job-transition.service:496
    // port: submission-job-transition.service:928
    // port: process-submission-outbox.service:110
    it('a settle after the job left processing (cancelled externally) is lease_lost; the job stays cancelled', async () => {
      const t = setup();
      let id = '';
      t.state.behavior = async () => {
        await driver.db.execute(
          sql`UPDATE "outbox"."job" SET status = 'cancelled', lease_token = NULL, leased_until = NULL, terminal_reason = 'operator', finished_at = now() WHERE id = ${id}`,
        );

        return ok();
      };
      id = await t.enqueue();

      expect(await t.runtime.drain(BUDGET, 'sweep')).toMatchObject({ succeeded: 0, leaseLost: 1 });
      expect(await jobRow(driver, id)).toMatchObject({
        status: 'cancelled',
        terminal_reason: 'operator',
      });
      expect(t.of('transition')).toEqual([]);
    });
  });

  describe('onTransition hook', () => {
    // port: submission-job-transition.service:228
    it('runs the hook and the settle in one transaction, hook first, emitting no warning on the happy path', async () => {
      let seenInside: unknown;
      const t = setup({
        hook: async (tx: OutboxDb, e) => {
          seenInside = await queryRows(
            tx,
            sql`SELECT status FROM "outbox"."job" WHERE id = ${e.job.id}`,
          );
        },
      });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(JSON.stringify(seenInside)).toContain('processing');
      expect(await jobRow(driver, id)).toMatchObject({ status: 'succeeded' });
      expect(t.events.filter((e) => e.type !== 'transition' && e.type !== 'enqueued')).toEqual([]);
    });

    it('rolls the hook back with the settle when the lease is lost after the hook ran', async () => {
      const t = setup({
        hook: async (tx: OutboxDb, e) => {
          await tx.execute(
            sql`UPDATE "outbox"."job" SET lease_token = gen_random_uuid() WHERE id = ${e.job.id}`,
          );
        },
      });
      const id = await t.enqueue();
      const before = await (async () => {
        await t.runtime.drain(BUDGET, 'sweep');

        return jobRow(driver, id);
      })();

      expect(t.of('lease_lost')).toHaveLength(1);
      expect(t.of('transition')).toEqual([]);
      const [attempt] = await attemptRows(driver, id);
      expect(before).toMatchObject({ status: 'processing', lease_token: attempt?.['lease_token'] });
    });

    // port: process-submission-outbox.service:1217
    // port: process-submission-outbox.service:2727
    it('fires once per applied transition with the job', async () => {
      const seen: { id: string; to: string }[] = [];
      const t = setup({
        hook: async (_, e) => void seen.push({ id: e.job.id, to: e.transition.to }),
      });
      t.state.behavior = async () => cancel({ reason: 'x' });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(seen).toEqual([{ id, to: 'cancelled' }]);
    });

    // port: submission-job-transition.service:525
    // port: process-submission-outbox.service:2764
    it('a hook throw rolls the transition back, emits hook_failed, then spends a retry without the hook', async () => {
      const t = setup({
        hook: async (tx) => {
          await tx.execute(sql`SELECT 1`);
          throw new Error('app table write failed');
        },
      });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(t.of('hook_failed')).toEqual([
        expect.objectContaining({ jobId: id, to: 'succeeded' }),
      ]);
      expect(t.of('transition')).toEqual([
        expect.objectContaining({ to: 'retrying', code: 'hook_failed' }),
      ]);
      expect(await jobRow(driver, id)).toMatchObject({
        status: 'retrying',
        last_error_code: 'hook_failed',
      });
    });

    it('a hook that always fails ends in dead(exhausted) instead of looping', async () => {
      const t = setup({
        hook: async () => {
          throw new Error('always');
        },
      });
      const id = await t.enqueue();

      for (let i = 0; i < 4; i++) {
        await t.runtime.drain(BUDGET, 'sweep');
        t.clock.advance(3_600_000);
      }

      expect(await jobRow(driver, id)).toMatchObject({
        status: 'dead',
        terminal_reason: 'exhausted',
        last_error_code: 'hook_failed',
      });
    });
  });

  describe('settle faults', () => {
    // port: submission-job-transition.service:582
    // port: submission-job-transition.service:768
    // port: submission-job-transition.service:808
    // port: process-submission-outbox.service:156
    it('retries a transient settle fault as SQL only: the handler runs once', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      faults.failNext(SETTLE, () => pgFault('40P01', 'deadlock detected'), 2);
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(t.state.calls).toBe(1);
      expect(await jobRow(driver, id)).toMatchObject({ status: 'succeeded' });
      expect(t.of('settle_retried')).toEqual([
        expect.objectContaining({ dbCode: '40P01', retry: 1 }),
        expect.objectContaining({ dbCode: '40P01', retry: 2 }),
      ]);
    });

    // port: submission-job-transition.service:550
    // port: process-submission-outbox.service:259
    it('reports a permanent settle fault with its SQLSTATE and does not retry it', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      faults.failNext(SETTLE, () => pgFault('23514', 'check violation'));
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(t.of('settle_retried')).toEqual([]);
      expect(t.of('settle_failed')).toEqual([
        expect.objectContaining({ jobId: id, dbCode: '23514', transient: false }),
      ]);
      expect(await jobRow(driver, id)).toMatchObject({ status: 'processing' });
    });

    // port: process-submission-outbox.service:210
    // port: dispatch-sweep:176
    it('leaves a job whose settle keeps failing in processing for recovery, never re-running the handler', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      faults.failNext(SETTLE, () => pgFault('40001'), 7);
      const id = await t.enqueue();

      const report = await t.runtime.drain(BUDGET, 'sweep');

      expect(t.state.calls).toBe(1);
      expect(report).toMatchObject({ succeeded: 0, settleFailed: 1 });
      expect(t.of('settle_failed')).toEqual([
        expect.objectContaining({ dbCode: '40001', transient: true }),
      ]);
      expect(await jobRow(driver, id)).toMatchObject({ status: 'processing' });

      t.clock.advance(t.job.leaseMs + 1);
      await t.runtime.recover(10);
      expect(await jobRow(driver, id)).toMatchObject({
        status: 'retrying',
        last_error_code: 'lease_expired',
      });
    });
  });

  describe('enqueue', () => {
    it('gives each tenant its own job for the same dedupe key', async () => {
      const t = setup();
      const a = await t.outbox.enqueue(driver.db, t.job, {
        payload: { n: 1 },
        dedupeKey: 'shared',
        tenantKey: 'org-a',
      });
      const b = await t.outbox.enqueue(driver.db, t.job, {
        payload: { n: 2 },
        dedupeKey: 'shared',
        tenantKey: 'org-b',
      });

      expect(a).toMatchObject({ status: 'created' });
      expect(b).toMatchObject({ status: 'created' });
      expect((a as { id: string }).id).not.toBe((b as { id: string }).id);
    });

    it('rejects an empty tenantKey, which would collide with tenant-less jobs', async () => {
      const t = setup();

      await expect(
        t.outbox.enqueue(driver.db, t.job, { payload: { n: 1 }, dedupeKey: 'k', tenantKey: '' }),
      ).rejects.toThrow(/tenantKey/);
    });

    it('reports a same-tenant key held by another kind as a kind conflict', async () => {
      const t = setup();
      const other = makeJob('other.job');
      const outbox = createOutbox({
        jobs: [t.job, other.job],
        connect: laneConnector(driver).connect,
        clock: testClock(),
      });
      const a = await outbox.enqueue(driver.db, t.job, {
        payload: { n: 1 },
        dedupeKey: 'k',
        tenantKey: 'org-a',
      });

      expect(
        await outbox.enqueue(driver.db, other.job, {
          payload: { n: 1 },
          dedupeKey: 'k',
          tenantKey: 'org-a',
        }),
      ).toEqual({
        status: 'dedupe_conflict',
        reason: 'kind',
        id: (a as { id: string }).id,
        existingKind: t.job.kind,
      });
    });
  });

  describe('operator actions', () => {
    it('retryNow makes a waiting job due, and bulk cancel/replay report per id', async () => {
      const t = setup();
      t.state.behavior = async () => retry({ code: 'busy', after: 3_600_000 });
      const a = await t.enqueue(1);
      const b = await t.enqueue(2);
      await t.runtime.drain(BUDGET, 'sweep');

      expect(await t.outbox.retryNow(a)).toBe('rescheduled');
      expect(await t.outbox.cancelMany([a, b], 'operator')).toEqual({
        [a]: 'cancelled',
        [b]: 'cancelled',
      });
      expect(await t.outbox.replayMany([a, b])).toEqual({ [a]: 'replayed', [b]: 'replayed' });
    });

    it('retryNow nudges the job so it runs without waiting for cron', async () => {
      const t = setup();
      t.state.behavior = async () => retry({ code: 'busy', after: 3_600_000 });
      const a = await t.enqueue(1);
      await t.runtime.drain(BUDGET, 'sweep');
      t.sent.length = 0;

      await t.outbox.retryNow(a);

      expect(t.sent).toEqual([
        { message: { v: 1, type: 'nudge', ids: [a] }, delaySeconds: undefined },
      ]);
    });

    it('bulk cancel and replay are one statement each and report every id, including misses', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      const a = await t.enqueue(1);
      const missing = '0190b2a0-0000-7000-8000-000000000000';
      const before = faults.statements.length;

      expect(await t.outbox.cancelMany([a, missing], 'operator')).toEqual({
        [a]: 'cancelled',
        [missing]: 'not_found',
      });
      expect(faults.statements.length - before).toBe(1);
      expect(await t.outbox.replayMany([a, missing])).toEqual({
        [a]: 'replayed',
        [missing]: 'not_found',
      });
      expect(faults.statements.length - before).toBe(2);
    });

    it('eraseTenant deletes every job of one tenant and nothing else', async () => {
      const t = setup();
      const mine = (await t.outbox.enqueue(driver.db, t.job, {
        payload: { n: 1 },
        dedupeKey: 'm',
        tenantKey: 'org-a',
      })) as { id: string };
      const other = (await t.outbox.enqueue(driver.db, t.job, {
        payload: { n: 1 },
        dedupeKey: 'o',
        tenantKey: 'org-b',
      })) as { id: string };

      expect(await t.outbox.eraseTenant('org-a')).toBe(1);
      expect(await jobRow(driver, mine.id)).toBeUndefined();
      expect(await jobRow(driver, other.id)).toBeDefined();
    });
  });

  describe('fenced commit: ok({ commit })', () => {
    const ledger = sql`CREATE TABLE IF NOT EXISTS ledger (job_id uuid PRIMARY KEY, n int NOT NULL)`;
    const ledgerRows = async () =>
      (await queryRows<{ job_id: string }>(driver.db, sql`SELECT job_id FROM ledger`)).length;
    const writeLedger = (id: string) => async (tx: OutboxDb) => {
      await tx.execute(sql`INSERT INTO ledger (job_id, n) VALUES (${id}, 1)`);
    };

    beforeEach(async () => {
      await driver.db.execute(ledger);
      await driver.db.execute(sql`TRUNCATE ledger`);
    });

    it('commits the handler write and succeeded together', async () => {
      const t = setup();
      t.state.behavior = async (_, ctx) => ok({ commit: writeLedger(ctx.jobId) });
      const id = await t.enqueue();

      expect(await t.runtime.drain(BUDGET, 'sweep')).toMatchObject({ succeeded: 1 });
      expect(await jobRow(driver, id)).toMatchObject({ status: 'succeeded' });
      expect(await ledgerRows()).toBe(1);
    });

    it('writes nothing when the lease was lost before the commit', async () => {
      const t = setup();
      t.state.behavior = async (_, ctx) => {
        await driver.db.execute(
          sql`UPDATE "outbox"."job" SET lease_token = gen_random_uuid() WHERE id = ${ctx.jobId}`,
        );

        return ok({ commit: writeLedger(ctx.jobId) });
      };
      await t.enqueue();

      expect(await t.runtime.drain(BUDGET, 'sweep')).toMatchObject({ leaseLost: 1, succeeded: 0 });
      expect(await ledgerRows()).toBe(0);
      expect(t.of('transition')).toEqual([]);
    });

    it('rolls the commit back when the lease is lost at the final write', async () => {
      const t = setup();
      t.state.behavior = async (_, ctx) =>
        ok({
          commit: async (tx) => {
            await writeLedger(ctx.jobId)(tx);
            await tx.execute(
              sql`UPDATE "outbox"."job" SET lease_token = gen_random_uuid() WHERE id = ${ctx.jobId}`,
            );
          },
        });
      await t.enqueue();

      expect(await t.runtime.drain(BUDGET, 'sweep')).toMatchObject({ leaseLost: 1 });
      expect(await ledgerRows()).toBe(0);
    });

    it('classifies a commit that throws with its own error, writing nothing', async () => {
      const t = setup({ jobOptions: { classify: () => retry({ code: 'commit_broke' }) } });
      t.state.behavior = async (_, ctx) =>
        ok({
          commit: async (tx) => {
            await writeLedger(ctx.jobId)(tx);
            throw new Error('constraint in app code');
          },
        });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(await jobRow(driver, id)).toMatchObject({
        status: 'retrying',
        last_error_code: 'commit_broke',
      });
      expect(await ledgerRows()).toBe(0);
    });

    it('re-runs the commit in a fresh transaction after a transient fault, leaving one write', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      faults.failNext(SETTLE, () => pgFault('40P01'));
      t.state.behavior = async (_, ctx) => ok({ commit: writeLedger(ctx.jobId) });
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(t.state.calls).toBe(1);
      expect(await jobRow(driver, id)).toMatchObject({ status: 'succeeded' });
      expect(await ledgerRows()).toBe(1);
    });

    it('closes ctx.db once the handler has returned', async () => {
      const t = setup();
      let leaked: OutboxDb | undefined;
      t.state.behavior = async (_, ctx) => {
        leaked = (ctx as unknown as { db: OutboxDb }).db;

        return ok();
      };
      await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      await expect(leaked!.execute(sql`SELECT 1`)).rejects.toThrow(/after the handler returned/);
    });
  });

  describe('deploy-version gating', () => {
    const V1 = '2026-01-01T00:00:00Z';
    const V2 = '2026-01-02T00:00:00Z';

    it('a job enqueued by a newer deploy is skipped by the older one and run by the newer', async () => {
      const { job, state } = makeJob();
      const clock = testClock();
      const newer = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        clock,
        version: { timestamp: V2 },
      });
      const older = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        clock,
        version: { timestamp: V1 },
      });
      await newer.enqueue(driver.db, job, { payload: { n: 1 }, dedupeKey: 'v' });

      expect(await older[RUNTIME].drain(BUDGET, 'sweep')).toMatchObject({ claimed: 0 });
      expect(await newer[RUNTIME].drain(BUDGET, 'sweep')).toMatchObject({ succeeded: 1 });
      expect(state.calls).toBe(1);
    });

    it('re-nudges a gated job instead of counting it as a miss', async () => {
      const { job } = makeJob();
      const clock = testClock();
      const { queue, sent } = recordingQueue();
      const recorded = recordingEvents();
      const newer = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        clock,
        version: { timestamp: V2 },
      });
      const older = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        clock,
        queue,
        events: recorded.sink,
        version: { timestamp: V1 },
      });
      const created = (await newer.enqueue(driver.db, job, {
        payload: { n: 1 },
        dedupeKey: 'v',
      })) as { id: string };

      await older[RUNTIME].processNudge([created.id], BUDGET);

      expect(recorded.of('nudge_miss')).toEqual([
        { type: 'nudge_miss', jobId: created.id, result: 'newer_version' },
      ]);
      expect(sent).toEqual([
        { message: { v: 1, type: 'nudge', ids: [created.id] }, delaySeconds: 5 },
      ]);
    });

    it('rejects an unparsable version timestamp at construction', () => {
      const { job } = makeJob();

      expect(() =>
        createOutbox({
          jobs: [job],
          connect: laneConnector(driver).connect,
          version: { timestamp: 'soon' },
        }),
      ).toThrow(/version/);
    });
  });

  describe('connection drops', () => {
    it('retries a settle whose connection dropped on a fresh connection, running the handler once', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      faults.failNext(SETTLE, connectionReset);
      const id = await t.enqueue();
      const lanesBefore = t.lanes.opened.length;

      await t.runtime.drain(BUDGET, 'sweep');

      expect(t.state.calls).toBe(1);
      expect(await jobRow(driver, id)).toMatchObject({ status: 'succeeded' });
      expect(t.of('settle_retried')).toEqual([expect.objectContaining({ dbCode: null, retry: 1 })]);
      expect(t.lanes.opened.length).toBeGreaterThan(lanesBefore + 1);
    });

    it('keeps retrying a settle through a longer outage (a Hyperdrive failover), with growing delays', async () => {
      const faults = new SqlFaults();
      const slept: number[] = [];
      const t = setup({ faults });
      t.clock.sleep = async (ms: number) => void slept.push(ms);
      faults.failNext(SETTLE, connectionReset, 5);
      const id = await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(await jobRow(driver, id)).toMatchObject({ status: 'succeeded' });
      expect(t.of('settle_retried')).toHaveLength(5);
      expect(slept.reduce((a, b) => a + b, 0)).toBeGreaterThanOrEqual(10_000);
      expect(slept.reduce((a, b) => a + b, 0)).toBeLessThan(30_000); // inside the lease margin
      expect(slept.every((ms, i) => i === 0 || ms >= slept[i - 1]! * 0.5)).toBe(true);
    });

    it('bounds lock waits in a hooked settle with a lock_timeout', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults, hook: async () => undefined });
      await t.enqueue();

      await t.runtime.drain(BUDGET, 'sweep');

      expect(
        faults.statements.some((statement) =>
          /set_config\('lock_timeout', '\d+ms', true\)/.test(statement),
        ),
      ).toBe(true);
    });

    it('reports a settle that committed before its reply was lost as applied, not lease_lost', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      // Every settle is a transaction: the COMMIT lands, then its reply is lost.
      faults.failAfterNextCommit(connectionReset);
      const id = await t.enqueue();

      const report = await t.runtime.drain(BUDGET, 'sweep');

      expect(report).toMatchObject({ succeeded: 1, leaseLost: 0 });
      expect(t.of('lease_lost')).toEqual([]);
      expect(t.of('transition')).toEqual([expect.objectContaining({ jobId: id, to: 'succeeded' })]);
    });
  });

  describe('registry scoping', () => {
    // port: submission-job.repository.claim.db:186
    // port: process-submission-outbox.service:872
    // port: dispatch-sweep:200
    it('two outboxes with disjoint jobs each claim only their own kinds; unknown kinds are never claimed', async () => {
      const a = makeJob('a.job');
      const b = makeJob('b.job');
      const outboxA = createOutbox({
        jobs: [a.job],
        connect: laneConnector(driver).connect,
        clock: testClock(),
      });
      const outboxB = createOutbox({
        jobs: [b.job],
        connect: laneConnector(driver).connect,
        clock: testClock(),
      });
      const bJob = await outboxB.enqueue(driver.db, b.job, { payload: { n: 1 }, dedupeKey: 'b' });
      const aJob = await outboxA.enqueue(driver.db, a.job, { payload: { n: 1 }, dedupeKey: 'a' });
      await driver.db.execute(
        sql`INSERT INTO "outbox"."job" (id, kind, dedupe_key, payload, available_at) VALUES (gen_random_uuid(), 'orphan.kind', 'orphan', '{}', now() - interval '1 hour')`,
      );

      expect(await outboxA[RUNTIME].drain(BUDGET, 'sweep')).toMatchObject({ claimed: 1 });
      expect(a.state.calls).toBe(1);
      expect(b.state.calls).toBe(0);
      expect(await jobRow(driver, (bJob as { id: string }).id)).toMatchObject({
        status: 'pending',
      });

      await outboxB[RUNTIME].drain(BUDGET, 'sweep');
      expect(await jobRow(driver, (aJob as { id: string }).id)).toMatchObject({
        status: 'succeeded',
      });
      expect(await jobRow(driver, (bJob as { id: string }).id)).toMatchObject({
        status: 'succeeded',
      });

      const [orphan] = await queryRows(
        driver.db,
        sql`SELECT status FROM "outbox"."job" WHERE kind = 'orphan.kind'`,
      );
      expect(orphan).toMatchObject({ status: 'pending' });
    });
  });

  describe('isolation', () => {
    // port: dispatch-sweep:328
    // port: dispatch-sweep:271
    it('one job crashing the runtime does not stop the next one, and only terminal states count as completed', async () => {
      // The first job runs away and its lane's reconnect fails: an infra error mid-job.
      let calls = 0;
      const crash = runawayCrash(laneConnector(driver).connect);
      const exploding = defineJob({
        kind: 'explode.job',
        payload: z.unknown(),
        timeoutMs: 1,
        handle: () => (++calls === 1 ? crash.runaway() : Promise.resolve(retry({ code: 'later' }))),
      });
      const recorded = recordingEvents();
      const outbox = createOutbox({
        jobs: [exploding],
        connect: crash.connect,
        events: recorded.sink,
        clock: testClock(),
      });
      await driver.db.execute(
        sql`INSERT INTO "outbox"."job" (id, kind, dedupe_key, payload, available_at, created_at) VALUES (gen_random_uuid(), 'explode.job', 'x1', '{}', now() - interval '1 hour', now() - interval '1 second'), (gen_random_uuid(), 'explode.job', 'x2', '{}', now() - interval '1 hour', now())`,
      );

      const report = await outbox[RUNTIME].drain(BUDGET, 'sweep');

      expect(recorded.of('infra_error')).toEqual([
        expect.objectContaining({ operation: 'run_job' }),
      ]);
      expect(report).toMatchObject({
        claimed: 2,
        retrying: 1,
        succeeded: 0,
        dead: 0,
        cancelled: 0,
      });
    });
  });

  describe('nudge', () => {
    // port: nudge-bulk-jobs:155
    it('nudge([]) sends and emits nothing', async () => {
      const t = setup();

      await t.outbox.nudge([]);

      expect(t.sent).toEqual([]);
      expect(t.events).toEqual([]);
    });

    // port: nudge-bulk-jobs:136
    it('without a queue it emits nudge_skipped and writes nothing', async () => {
      const recorded = recordingEvents();
      const { job } = makeJob();
      const outbox = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        events: recorded.sink,
      });

      await outbox.nudge(['a']);

      expect(recorded.of('nudge_skipped')).toEqual([
        { type: 'nudge_skipped', ids: ['a'], lane: 'default', reason: 'no_queue' },
      ]);
    });

    // port: nudge-bulk-jobs:84
    // port: nudge-bulk-jobs:93
    // port: job-outbox-nudge.repository.db:86
    it('records sent and failed chunks; one failure does not stop the others', async () => {
      const { job } = makeJob();
      const recorded = recordingEvents();
      let calls = 0;
      const { queue, sent } = recordingQueue({
        fail: () => (calls++ === 0 ? new Error('queue down') : undefined),
      });
      const outbox = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        events: recorded.sink,
        queue,
      });
      const ids = Array.from({ length: 150 }, (_, i) => `id-${i}`);

      await outbox.nudge(ids);

      expect(recorded.of('nudge_failed')).toEqual([
        expect.objectContaining({ ids: ids.slice(0, 100) }),
      ]);
      expect(recorded.of('nudge_sent')).toEqual([expect.objectContaining({ ids: ids.slice(100) })]);
      expect(sent).toHaveLength(1);
    });

    // port: nudge-bulk-jobs:118
    it('contains a synchronous throw from queue.send', async () => {
      const { job } = makeJob();
      const recorded = recordingEvents();
      const outbox = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        events: recorded.sink,
        queue: {
          send: () => {
            throw new Error('sync boom');
          },
        },
      });

      await expect(outbox.nudge(['a'])).resolves.toBeUndefined();
      expect(recorded.of('nudge_failed')).toHaveLength(1);
    });

    // port: nudge-bulk-jobs:165
    // port: nudge-bulk-jobs:212
    it('a throwing event sink cannot reject nudge() on success or failure', async () => {
      const { job } = makeJob();
      const sink = () => {
        throw new Error('sink down');
      };
      const ok1 = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        events: sink,
        queue: recordingQueue().queue,
      });
      const bad = createOutbox({
        jobs: [job],
        connect: laneConnector(driver).connect,
        events: sink,
        queue: recordingQueue({ fail: () => new Error('x') }).queue,
      });

      await expect(ok1.nudge(['a'])).resolves.toBeUndefined();
      await expect(bad.nudge(['a'])).resolves.toBeUndefined();
    });
  });

  describe('connections', () => {
    it('closes the lanes that opened when another lane fails to connect', async () => {
      const { job } = makeJob();
      let opened = 0;
      let closed = 0;
      const outbox = createOutbox({
        jobs: [job],
        lanes: 3,
        connect: async () => {
          opened++;

          if (opened === 2) {
            throw new Error('no connection slot');
          }

          return {
            db: driver.db as unknown as OutboxDb,
            close: async () => void closed++,
            destroy: async () => void closed++,
          };
        },
      });

      await expect(outbox[RUNTIME].drain(BUDGET, 'sweep')).rejects.toThrow('no connection slot');
      expect(closed).toBe(opened - 1);
    });

    it('an explicit tenantKey: undefined means every tenant, like omitting it', async () => {
      const t = setup();
      const id = await t.enqueue();

      expect(await t.outbox.cancel({ id }, 'x', { tenantKey: undefined })).toBe('cancelled');
    });
  });

  describe('construction', () => {
    // port: submission-job-transition.service:1230
    it('createOutbox performs no I/O until a method needs it', async () => {
      const { job } = makeJob();
      let connects = 0;

      createOutbox({
        jobs: [job],
        connect: async () => {
          connects++;

          return laneConnector(driver).connect();
        },
      });

      expect(connects).toBe(0);
    });
  });
});
