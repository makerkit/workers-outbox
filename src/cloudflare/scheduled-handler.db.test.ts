import { sql } from 'drizzle-orm';
import { PgDialect } from 'drizzle-orm/pg-core';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import type { TransitionHook } from '../runtime/runtime';

import { defineJob, type JobContext } from '../core/define-job';
import { cancel, ok, retry, type HandlerOutcome } from '../core/outcome';
import { queryRows } from '../drizzle/catalog';
import { createOutbox } from '../runtime/create-outbox';
import {
  runawayCrash,
  attemptRows,
  jobRow,
  laneConnector,
  pgFault,
  recordingEvents,
  recordingQueue,
  SqlFaults,
  testClock,
} from '../runtime/runtime-harness.test-support';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { createScheduledHandler, DEFAULT_SWEEP_BUDGET } from './scheduled-handler';

const LIST_EXPIRED = /status = 'processing' AND leased_until </;
const CLAIM_DUE = /picked AS \(\s*SELECT j\.id FROM "outbox"\."job" j\s*WHERE/;
const CLEANUP = /doomed AS/;

describe.each(testDrivers())('scheduled handler on $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  function setup(
    options: {
      behavior?: (payload: { n: number }, ctx: JobContext) => Promise<HandlerOutcome>;
      faults?: SqlFaults;
      hook?: TransitionHook;
      timeoutMs?: number;
      maxCrashes?: number;
      budget?: { maxJobs: number; deadlineMs: number };
    } = {},
  ) {
    const clock = testClock();
    const calls: { id: string; attempt: number }[] = [];
    const job = defineJob({
      kind: 'test.job',
      payload: z.object({ n: z.number() }),
      timeoutMs: options.timeoutMs ?? 1_000,
      retry: { maxAttempts: 5, maxCrashes: options.maxCrashes ?? 3 },
      handle: async (payload, ctx) => {
        calls.push({ id: ctx.jobId, attempt: ctx.attempt });

        return options.behavior ? options.behavior(payload, ctx) : ok();
      },
    });
    const recorded = recordingEvents();
    const { queue, sent } = recordingQueue();
    const outbox = createOutbox({
      jobs: [job],
      connect: laneConnector(driver, options.faults).connect,
      events: recorded.sink,
      queue,
      clock,
      ...(options.hook ? { onTransition: options.hook } : {}),
    });
    const tick = createScheduledHandler(
      () => outbox,
      options.budget ? { budget: options.budget } : {},
    );
    const run = () => tick({ cron: '*/5 * * * *' }, {}, {});
    const enqueue = async (n = 1) => {
      const r = await outbox.enqueue(driver.db, job, {
        payload: { n },
        dedupeKey: `k:${Math.random()}`,
      });

      return (r as { id: string }).id;
    };
    const summary = () => recorded.of('sweep_finished').at(-1);

    const crashOnce = async (id: string) => {
      await driver.db.execute(
        sql`UPDATE "outbox"."job" SET status = 'processing', started_attempts = started_attempts + 1,
              lease_token = gen_random_uuid(), leased_until = ${clock.current().toISOString()}::timestamptz - interval '1 second',
              next_attempt_at = NULL
            WHERE id = ${id}`,
      );
      await driver.db.execute(
        sql`INSERT INTO "outbox"."attempt" (job_id, attempt_no, kind, lease_token, trigger, started_at)
            SELECT id, started_attempts, kind, lease_token, 'sweep', ${clock.current().toISOString()}::timestamptz FROM "outbox"."job" WHERE id = ${id}`,
      );
    };

    return { job, calls, clock, ...recorded, sent, outbox, run, enqueue, summary, crashOnce };
  }

  describe('summary', () => {
    // port: dispatch-sweep:108
    it('a tick with nothing due claims nothing and reports a zero summary', async () => {
      const t = setup();

      await t.run();

      expect(t.summary()).toMatchObject({
        cron: '*/5 * * * *',
        recovered: 0,
        deleted: 0,
        continued: false,
        report: {
          claimed: 0,
          succeeded: 0,
          byKind: {},
          errored: 0,
          reachedLimit: false,
          reachedDeadline: false,
        },
      });
    });

    // port: dispatch-sweep:126
    // port: dispatch-sweep:156
    // port: dispatch-sweep:348
    it('tallies outcomes per kind, counting cancelled apart from succeeded', async () => {
      const t = setup({
        behavior: async ({ n }) =>
          n === 1 ? ok() : n === 2 ? cancel({ reason: 'x' }) : retry({ code: 'busy' }),
      });
      await t.enqueue(1);
      await t.enqueue(2);
      await t.enqueue(3);

      await t.run();

      expect(t.summary()?.report).toMatchObject({
        claimed: 3,
        succeeded: 1,
        cancelled: 1,
        retrying: 1,
        byKind: { 'test.job': { claimed: 3, succeeded: 1, cancelled: 1, retrying: 1, dead: 0 } },
      });
    });

    // port: dispatch-sweep:219
    it('counts runtime errors with up to five message samples', async () => {
      // Every job runs away and its lane's reconnect fails: an infra error per job.
      const crash = runawayCrash(laneConnector(driver).connect);
      const exploding = defineJob({
        kind: 'explode.job',
        payload: z.unknown(),
        timeoutMs: 1,
        handle: () => crash.runaway(),
      });
      const recorded = recordingEvents();
      const outbox = createOutbox({
        jobs: [exploding],
        connect: crash.connect,
        events: recorded.sink,
        clock: testClock(),
      });
      await driver.db.execute(
        sql`INSERT INTO "outbox"."job" (id, kind, dedupe_key, payload, available_at) SELECT gen_random_uuid(), 'explode.job', 'x' || g, '{}', now() - interval '1 hour' FROM generate_series(1, 7) g`,
      );

      await createScheduledHandler(() => outbox)({}, {}, {});

      const report = recorded.of('sweep_finished')[0]?.report;
      expect(report?.errored).toBe(7);
      expect(report?.erroredSamples).toHaveLength(5);
      expect(report?.erroredSamples[0]).toMatch(/reconnect crashed/);
    });
  });

  describe('budgets', () => {
    // port: dispatch-sweep:291
    // port: cron-handlers:831
    it('stops at its job budget and says so', async () => {
      const t = setup({ budget: { maxJobs: 3, deadlineMs: 180_000 } });
      for (let i = 0; i < 5; i++) {
        await t.enqueue(i);
      }

      await t.run();

      expect(t.calls).toHaveLength(3);
      expect(t.summary()?.report).toMatchObject({
        claimed: 3,
        reachedLimit: true,
        reachedDeadline: false,
      });
      expect(t.summary()?.continued).toBe(true);
    });

    // port: dispatch-sweep:307
    it('stops at the first empty claim', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      await t.enqueue();

      await t.run();

      const claims = faults.statements.filter((s) => CLAIM_DUE.test(s));
      expect(claims).toHaveLength(2);
      expect(t.summary()?.report).toMatchObject({
        claimed: 1,
        reachedLimit: false,
        reachedDeadline: false,
      });
    });

    // port: dispatch-sweep:406
    // port: cron-handlers:798
    it('stops claiming once its time budget is spent and says so', async () => {
      const t = setup({
        budget: { maxJobs: 50, deadlineMs: 100_000 },
        behavior: async () => {
          t.clock.advance(40_000);

          return ok();
        },
      });
      for (let i = 0; i < 5; i++) {
        await t.enqueue(i);
      }

      await t.run();

      expect(t.calls).toHaveLength(2);
      expect(t.summary()?.report).toMatchObject({ reachedDeadline: true, reachedLimit: false });
    });

    // port: dispatch-sweep:433
    it('reports no deadline when the backlog empties first', async () => {
      const t = setup();
      await t.enqueue();

      await t.run();

      expect(t.summary()?.report).toMatchObject({ reachedDeadline: false, reachedLimit: false });
    });

    // port: dispatch-sweep:442
    // port: cron-handlers:775
    it('applies the default sweep budget (50 jobs / 180 s) when none is given', async () => {
      const t = setup();
      for (let i = 0; i < 55; i++) {
        await t.enqueue(i);
      }

      await t.run();

      expect(DEFAULT_SWEEP_BUDGET).toEqual({ maxJobs: 50, deadlineMs: 180_000 });
      expect(t.calls).toHaveLength(50);
    });

    // port: cron-handlers:852
    it('claims a job only if its lease fits the remaining budget', async () => {
      const t = setup({ timeoutMs: 600_000, budget: { maxJobs: 50, deadlineMs: 180_000 } });
      const id = await t.enqueue();

      await t.run();

      expect(t.calls).toEqual([]);
      expect(await jobRow(driver, id)).toMatchObject({ status: 'pending' });
      expect(() =>
        defineJob({
          kind: 'too.long',
          payload: z.object({}),
          timeoutMs: 600_001,
          handle: async () => ok(),
        }),
      ).toThrow(/timeoutMs/);
    });
  });

  describe('phases', () => {
    // port: cron-handlers:137
    // port: cron-handlers:157
    // port: cron-handlers:350
    it('runs recover, then drain, then cleanup, in that order', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      await t.enqueue();

      await t.run();

      const phases = faults.statements
        .map((s) =>
          LIST_EXPIRED.test(s)
            ? 'recover'
            : CLAIM_DUE.test(s)
              ? 'drain'
              : CLEANUP.test(s)
                ? 'cleanup'
                : null,
        )
        .filter((phase) => phase !== null);
      const order = phases.filter((phase, i) => phase !== phases[i - 1]);
      expect(order).toEqual(['recover', 'drain', 'cleanup']);
    });

    // port: cron-handlers:498
    // port: cron-handlers:591
    // port: cron-handlers:560
    it('a failing recover phase is reported and drain and cleanup still run', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      const id = await t.enqueue();
      faults.failNext(LIST_EXPIRED, () => pgFault('XX000', 'recovery broke'));

      await t.run();

      expect(t.of('phase_failed')).toEqual([
        expect.objectContaining({
          phase: 'recover',
          cron: '*/5 * * * *',
          error: expect.objectContaining({ message: 'Failed query' }),
        }),
      ]);
      expect(await jobRow(driver, id)).toMatchObject({ status: 'succeeded' });
      expect(faults.statements.some((s) => CLEANUP.test(s))).toBe(true);
    });

    // port: cron-handlers:464
    it('a failing drain is reported and cleanup still runs', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      await t.enqueue();
      faults.failNext(CLAIM_DUE, () => pgFault('XX000'));

      await t.run();

      expect(t.of('phase_failed').map((e) => e.phase)).toEqual(['drain']);
      expect(faults.statements.some((s) => CLEANUP.test(s))).toBe(true);
    });

    // port: cron-handlers:524
    // port: cron-handlers:742
    it('a failing cleanup is reported and the handler still resolves', async () => {
      const faults = new SqlFaults();
      const t = setup({ faults });
      faults.failNext(CLEANUP, () => pgFault('XX000'));

      await expect(t.run()).resolves.toBeUndefined();

      expect(t.of('phase_failed').map((e) => e.phase)).toEqual(['cleanup']);
      expect(t.summary()).toBeDefined();
    });

    // port: cleanup-scheduler.service.db:71
    it('deletes finished jobs past retention inline, idempotently and in bounded batches', async () => {
      const t = setup();
      const id = await t.enqueue();
      await t.run();

      t.clock.advance(8 * 86_400_000);
      await t.run();
      await t.run();

      expect(await jobRow(driver, id)).toBeUndefined();
      expect(t.of('sweep_finished').map((e) => e.deleted)).toEqual([0, 1, 0]);
      const [count] = await queryRows<{ n: number }>(
        driver.db,
        sql`SELECT count(*)::int AS n FROM "outbox"."job"`,
      );
      expect(count?.n).toBe(0);
    });
  });

  describe('recovery', () => {
    // port: stuck-job-recovery:54
    it('re-arms a crashed job as retrying, closes its attempt and runs the hook in the same transaction, before the job write', async () => {
      const hooked: { to: string; seen: string }[] = [];
      const t = setup({
        hook: async (tx, e) => {
          const rows = await queryRows<{ status: string }>(
            tx,
            sql`SELECT status FROM "outbox"."job" WHERE id = ${e.job.id}`,
          );
          hooked.push({ to: e.transition.to, seen: rows[0]?.status ?? 'none' });
        },
        behavior: async () => retry({ code: 'stop', after: 3_600_000 }),
      });
      const id = await t.enqueue();
      await t.crashOnce(id);

      await t.run();

      expect(hooked[0]).toEqual({ to: 'retrying', seen: 'processing' });
      expect(await jobRow(driver, id)).not.toMatchObject({ status: 'processing' });
      expect((await attemptRows(driver, id))[0]).toMatchObject({ outcome: 'lease_expired' });
      expect(t.of('transition')[0]).toMatchObject({
        to: 'retrying',
        code: 'lease_expired',
        recovered: true,
        trigger: 'recovery',
        attemptNo: 1,
        tenantKey: null,
      });
    });

    // port: stuck-job-recovery:242
    it('re-arms one job and crash-loops another in the same pass', async () => {
      const t = setup({ maxCrashes: 2 });
      const fresh = await t.enqueue(1);
      const looping = await t.enqueue(2);
      await driver.db.execute(
        sql`UPDATE "outbox"."job" SET last_error_code = 'lease_expired', code_streak = 1 WHERE id = ${looping}`,
      );
      await t.crashOnce(fresh);
      await t.crashOnce(looping);

      await t.run();

      expect(await jobRow(driver, looping)).toMatchObject({
        status: 'dead',
        terminal_reason: 'crash_loop',
      });
      expect(await jobRow(driver, fresh)).toMatchObject({ status: 'succeeded' });
      expect(t.summary()?.recovered).toBe(2);
    });

    // port: stuck-job-recovery:196
    it('crash-loops a job to dead on recovery, stamping finished_at and firing the hook', async () => {
      const hooked: string[] = [];
      const t = setup({ maxCrashes: 1, hook: async (_, e) => void hooked.push(e.transition.to) });
      const id = await t.enqueue();
      await t.crashOnce(id);

      await t.run();

      expect(await jobRow(driver, id)).toMatchObject({
        status: 'dead',
        terminal_reason: 'crash_loop',
        finished_at: expect.anything(),
        next_attempt_at: null,
      });
      expect(hooked).toEqual(['dead']);
      expect(t.calls).toEqual([]);
    });

    // port: stuck-job-recovery:493
    it('a job that settles between listing and recovery is lease_lost: nothing overwritten, no hook', async () => {
      const hooked: string[] = [];
      const clock = testClock();
      const job = defineJob({
        kind: 'test.job',
        payload: z.object({ n: z.number() }),
        timeoutMs: 1_000,
        handle: async () => ok(),
      });
      let targetId = '';
      const connect = async () => {
        const lane = await laneConnector(driver).connect();
        const execute = lane.db.execute.bind(lane.db);

        return {
          ...lane,
          db: {
            ...lane.db,
            transaction: lane.db.transaction.bind(lane.db),
            execute: async (query: Parameters<typeof execute>[0]) => {
              const result = await execute(query);

              if (LIST_EXPIRED.test(new PgDialect().sqlToQuery(query).sql)) {
                await driver.db.execute(
                  sql`UPDATE "outbox"."job" SET status = 'succeeded', lease_token = NULL, leased_until = NULL,
                        finished_attempts = finished_attempts + 1, spent_attempts = spent_attempts + 1, finished_at = now()
                      WHERE id = ${targetId}`,
                );
              }

              return result;
            },
          },
        };
      };
      const recorded = recordingEvents();
      const outbox = createOutbox({
        jobs: [job],
        connect,
        events: recorded.sink,
        clock,
        onTransition: async (_, e) => void hooked.push(e.transition.to),
      });
      const created = (await outbox.enqueue(driver.db, job, {
        payload: { n: 1 },
        dedupeKey: 'race',
      })) as { id: string };
      targetId = created.id;
      await driver.db.execute(
        sql`UPDATE "outbox"."job" SET status = 'processing', started_attempts = 1, lease_token = gen_random_uuid(),
              leased_until = ${clock.current().toISOString()}::timestamptz - interval '1 second' WHERE id = ${targetId}`,
      );

      await createScheduledHandler(() => outbox)({}, {}, {});

      expect(await jobRow(driver, targetId)).toMatchObject({ status: 'succeeded' });
      expect(hooked).toEqual([]);
      expect(recorded.of('lease_lost')).toEqual([expect.objectContaining({ jobId: targetId })]);
    });

    // port: stuck-job-recovery.db:95
    it('a recovered job is re-claimed as attempt 2 and succeeds, leaving both attempts in the ledger', async () => {
      const t = setup();
      const id = await t.enqueue();
      await t.crashOnce(id);

      await t.run();

      expect(t.calls).toEqual([{ id, attempt: 2 }]);
      expect(await jobRow(driver, id)).toMatchObject({
        status: 'succeeded',
        started_attempts: 2,
        finished_attempts: 2,
        spent_attempts: 1,
      });
      expect((await attemptRows(driver, id)).map((a) => a['outcome'])).toEqual([
        'lease_expired',
        'succeeded',
      ]);
    });
  });
});
