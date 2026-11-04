import { sql } from 'drizzle-orm';
import { v7 as uuidv7 } from 'uuid';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import type { Transition } from '../core/transition';

import { retry } from '../core/outcome';
import { resolveRetryPolicy } from '../core/retry-policy';
import { decideTransition } from '../core/transition';
import { queryRows } from '../drizzle/catalog';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { recordingRunner } from '../test-support/recording-runner';
import {
  at,
  CRM,
  dead,
  EMAIL,
  newJob,
  retrying,
  storeHelpers,
  succeeded,
  T0,
} from '../test-support/store-fixtures';
import {
  cancelJob,
  claimByIds,
  claimDue,
  DEFAULT_SUCCEEDED_RETENTION_MS,
  DEFAULT_TERMINAL_RETENTION_MS,
  deleteFinishedJobs,
  hasDueJobs,
  insertJob,
  listExpiredLeases,
  recordNudgeResult,
  replayJob,
  setSettlementTimeouts,
  settleJob,
  type KindFilter,
} from './outbox-store';

describe.each(testDrivers())('outbox store on $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  const { jobRow, attempts, enqueue, claimOne, settleAs } = storeHelpers(() => driver.db);

  describe('insertJob', () => {
    it('creates a pending job due at enqueue time plus delay', async () => {
      const job = newJob({ delayMs: 5_000, tenantKey: 'org-1', correlationId: 'req-1' });

      expect(await insertJob(driver.db, job, T0)).toEqual({
        id: job.id,
        kind: job.kind,
        tenantKey: 'org-1',
        created: true,
      });
      expect(await jobRow(job.id)).toMatchObject({
        status: 'pending',
        payload: { userId: 'u1' },
        tenant_key: 'org-1',
        correlation_id: 'req-1',
        available_at: at(5_000),
      });
    });

    it('returns the existing job for a duplicate dedupe key without changing it', async () => {
      const first = newJob({ dedupeKey: 'welcome:u1' });
      await insertJob(driver.db, first, T0);

      const second = await insertJob(
        driver.db,
        newJob({ dedupeKey: 'welcome:u1', payload: { other: true } }),
        T0,
      );

      expect(second).toEqual({ id: first.id, kind: first.kind, tenantKey: null, created: false });
      expect(await jobRow(first.id)).toMatchObject({ payload: { userId: 'u1' } });
    });

    it('scopes dedupe keys per tenant: the same key in two tenants is two jobs', async () => {
      const a = newJob({ dedupeKey: 'shared', tenantKey: 'org-a' });
      const b = newJob({ dedupeKey: 'shared', tenantKey: 'org-b' });

      expect(await insertJob(driver.db, a, T0)).toMatchObject({ id: a.id, created: true });
      expect(await insertJob(driver.db, b, T0)).toMatchObject({ id: b.id, created: true });
      expect(
        await insertJob(driver.db, newJob({ dedupeKey: 'shared', tenantKey: 'org-a' }), T0),
      ).toMatchObject({
        id: a.id,
        created: false,
      });
    });

    it('cancels by dedupe key only within the given tenant', async () => {
      const a = newJob({ dedupeKey: 'k', tenantKey: 'org-a' });
      const b = newJob({ dedupeKey: 'k', tenantKey: 'org-b' });
      await insertJob(driver.db, a, T0);
      await insertJob(driver.db, b, T0);

      expect(
        await cancelJob(driver.db, {
          ref: { dedupeKey: 'k', tenantKey: 'org-b' },
          reason: 'x',
          now: T0,
        }),
      ).toBe('cancelled');
      expect(await jobRow(a.id)).toMatchObject({ status: 'pending' });
      expect(await jobRow(b.id)).toMatchObject({ status: 'cancelled' });
    });

    it('reports the existing kind when a key is reused for a different kind', async () => {
      await insertJob(driver.db, newJob({ dedupeKey: 'shared' }), T0);

      expect(
        await insertJob(driver.db, newJob({ dedupeKey: 'shared', kind: CRM.kind }), T0),
      ).toMatchObject({
        kind: EMAIL.kind,
        created: false,
      });
    });

    it('writes nothing when the caller transaction rolls back', async () => {
      const job = newJob();

      await expect(
        driver.db.transaction(async (tx) => {
          await insertJob(tx, job, T0);
          throw new Error('business write failed');
        }),
      ).rejects.toThrow('business write failed');

      expect(await jobRow(job.id)).toBeUndefined();
    });
  });

  describe('claimDue', () => {
    it('claims a due job, takes a lease from the kind and opens an attempt', async () => {
      const { id } = await enqueue();
      const leaseToken = uuidv7();

      const claimed = await claimDue(driver.db, {
        kinds: [EMAIL],
        limit: 5,
        trigger: 'sweep',
        leaseToken,
        now: at(1),
      });

      expect(claimed).toEqual([
        expect.objectContaining({
          id,
          kind: EMAIL.kind,
          leaseToken,
          attemptNo: 1,
          payload: { userId: 'u1' },
        }),
      ]);
      expect(await jobRow(id)).toMatchObject({
        status: 'processing',
        started_attempts: 1,
        lease_token: leaseToken,
        leased_until: at(1 + EMAIL.leaseMs),
      });
      expect(await attempts(id)).toEqual([
        expect.objectContaining({
          attempt_no: 1,
          lease_token: leaseToken,
          trigger: 'sweep',
          started_at: at(1),
          outcome: null,
        }),
      ]);
    });

    // port: submission-job.repository.claim.db:538
    it('skips jobs that are not due yet', async () => {
      await enqueue({ delayMs: 10_000 });

      expect(await claimOne(at(9_999))).toBeUndefined();
      expect(await claimOne(at(10_000))).toBeDefined();
    });

    // port: submission-job.repository.claim.db:868
    it('orders by due time, so a due retry cannot starve fresh work or vice versa', async () => {
      const old = await enqueue({}, at(0));
      const fresh = await enqueue({}, at(1_000));

      const claimed = await claimOne(at(1_000));
      expect(claimed?.id).toBe(old.id);
      await settleJob(driver.db, {
        jobId: old.id,
        leaseToken: claimed!.leaseToken,
        transition: retrying(60_000),
        now: at(1_000),
      });

      expect((await claimOne(at(2_000)))?.id).toBe(fresh.id);
      expect(await claimOne(at(2_000))).toBeUndefined();
      expect((await claimOne(at(61_000)))?.id).toBe(old.id);
    });

    // port: submission-job.repository.claim.db:169
    it('only claims registered kinds whose lease fits the budget', async () => {
      await enqueue({ kind: 'unknown.kind' });
      const crm = await enqueue({ kind: CRM.kind });

      expect(await claimOne(T0, [EMAIL])).toBeUndefined();
      expect(await claimOne(T0, [EMAIL, { ...CRM, fits: false }])).toBeUndefined();
      expect((await claimOne(T0, [EMAIL, CRM]))?.id).toBe(crm.id);
    });

    it('respects the limit and never claims a job twice', async () => {
      await Promise.all([enqueue(), enqueue(), enqueue()]);

      const first = await claimDue(driver.db, {
        kinds: [EMAIL],
        limit: 2,
        trigger: 'drain',
        leaseToken: uuidv7(),
        now: T0,
      });
      const second = await claimDue(driver.db, {
        kinds: [EMAIL],
        limit: 2,
        trigger: 'drain',
        leaseToken: uuidv7(),
        now: T0,
      });

      expect(first).toHaveLength(2);
      expect(second).toHaveLength(1);
      expect(new Set([...first, ...second].map((j) => j.id)).size).toBe(3);
    });

    it('claims nothing when no kind fits', async () => {
      await enqueue();

      expect(
        await claimDue(driver.db, {
          kinds: [{ ...EMAIL, fits: false }],
          limit: 5,
          trigger: 'sweep',
          leaseToken: uuidv7(),
          now: T0,
        }),
      ).toEqual([]);
    });
  });

  describe('claimByIds', () => {
    it('claims exactly the requested ids and explains every miss', async () => {
      const due = await enqueue();
      const notDue = await enqueue({ delayMs: 60_000 });
      const unregistered = await enqueue({ kind: 'unknown.kind' });
      const overBudget = await enqueue({ kind: CRM.kind });
      const leased = await enqueue();
      await claimByIds(driver.db, {
        ids: [leased.id],
        kinds: [EMAIL],
        leaseToken: uuidv7(),
        now: T0,
      });
      const other = await enqueue();
      const missing = uuidv7();

      const results = await claimByIds(driver.db, {
        ids: [due.id, notDue.id, unregistered.id, overBudget.id, leased.id, missing, 'not-a-uuid'],
        kinds: [EMAIL, { ...CRM, fits: false }],
        leaseToken: uuidv7(),
        now: T0,
      });

      const byId = Object.fromEntries(results.map((r) => [r.id, r.result]));

      expect(byId).toEqual({
        [due.id]: 'claimed',
        [notDue.id]: 'not_due',
        [unregistered.id]: 'unregistered_kind',
        [overBudget.id]: 'over_budget',
        [leased.id]: 'leased',
        [missing]: 'not_found',
        'not-a-uuid': 'not_found',
      });
      expect(await jobRow(other.id)).toMatchObject({ status: 'pending' });
      expect((await attempts(due.id))[0]).toMatchObject({ trigger: 'nudge' });
    });

    it('reports a terminal job as terminal and claims a duplicate id once', async () => {
      const job = await enqueue();
      const claimed = await claimOne();
      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: claimed!.leaseToken,
        transition: succeeded,
        now: T0,
      });

      expect(
        await claimByIds(driver.db, {
          ids: [job.id, job.id],
          kinds: [EMAIL],
          leaseToken: uuidv7(),
          now: T0,
        }),
      ).toEqual([{ id: job.id, result: 'terminal' }]);
    });
  });

  describe('settleJob', () => {
    // port: submission-job.repository.claim.db:745
    it('settles success, clears the lease and closes the attempt', async () => {
      const job = await enqueue();
      const claimed = await claimOne(T0);

      expect(
        await settleJob(driver.db, {
          jobId: job.id,
          leaseToken: claimed!.leaseToken,
          transition: succeeded,
          now: at(1_500),
        }),
      ).toBe('applied');
      expect(await jobRow(job.id)).toMatchObject({
        status: 'succeeded',
        lease_token: null,
        leased_until: null,
        finished_attempts: 1,
        finished_at: at(1_500),
      });
      expect(await attempts(job.id)).toEqual([
        expect.objectContaining({
          outcome: 'succeeded',
          finished_at: at(1_500),
          duration_ms: 1_500,
        }),
      ]);
    });

    // port: submission-job-transition.service:689
    // port: submission-job-transition.service:754
    it('schedules a retry on the database clock and records the error', async () => {
      const job = await enqueue();
      const claimed = await claimOne(T0);

      expect(
        await settleJob(driver.db, {
          jobId: job.id,
          leaseToken: claimed!.leaseToken,
          transition: retrying(30_000),
          now: at(100),
        }),
      ).toBe('applied');

      expect(await jobRow(job.id)).toMatchObject({
        status: 'retrying',
        next_attempt_at: at(30_100),
        last_error_code: 'http_503',
        last_error_message: 'unavailable',
        code_streak: 1,
        finished_at: null,
      });
      expect((await attempts(job.id))[0]).toMatchObject({ outcome: 'retry', code: 'http_503' });
    });

    // port: submission-job-transition.service:1069
    it('records dead and cancelled reasons', async () => {
      const deadJob = await enqueue();
      const deadClaim = await claimOne();
      await settleJob(driver.db, {
        jobId: deadJob.id,
        leaseToken: deadClaim!.leaseToken,
        transition: {
          to: 'dead',
          reason: 'exhausted',
          code: 'http_503',
          message: null,
          codeStreak: 3,
          attemptOutcome: 'retry',
        },
        now: T0,
      });

      const cancelled = await enqueue();
      const cancelClaim = await claimOne();
      await settleJob(driver.db, {
        jobId: cancelled.id,
        leaseToken: cancelClaim!.leaseToken,
        transition: { to: 'cancelled', reason: 'user_deleted', attemptOutcome: 'cancelled' },
        now: T0,
      });

      expect(await jobRow(deadJob.id)).toMatchObject({
        status: 'dead',
        terminal_reason: 'exhausted',
        code_streak: 3,
      });
      expect(await jobRow(cancelled.id)).toMatchObject({
        status: 'cancelled',
        terminal_reason: 'user_deleted',
        finished_at: T0,
        lease_token: null,
      });
    });

    it('fences out a stale worker whose lease was recovered and re-claimed', async () => {
      const job = await enqueue();
      const stale = await claimOne(T0);
      const lease = stale!.leaseToken;

      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: lease,
        transition: { ...retrying(0), code: 'lease_expired', attemptOutcome: 'lease_expired' },
        now: at(EMAIL.leaseMs + 1),
        requireExpiredLease: true,
      });
      const fresh = await claimOne(at(EMAIL.leaseMs + 2));

      expect(
        await settleJob(driver.db, {
          jobId: job.id,
          leaseToken: lease,
          transition: succeeded,
          now: at(EMAIL.leaseMs + 3),
        }),
      ).toBe('lease_lost');
      expect(await jobRow(job.id)).toMatchObject({
        status: 'processing',
        lease_token: fresh!.leaseToken,
      });
      expect(await attempts(job.id)).toEqual([
        expect.objectContaining({ attempt_no: 1, outcome: 'lease_expired' }),
        expect.objectContaining({ attempt_no: 2, outcome: null }),
      ]);
    });

    // port: stuck-job-recovery.db:240
    it('refuses a recovery settle while the lease is still live', async () => {
      const job = await enqueue();
      const claimed = await claimOne(T0);

      expect(
        await settleJob(driver.db, {
          jobId: job.id,
          leaseToken: claimed!.leaseToken,
          transition: retrying(0),
          now: at(EMAIL.leaseMs - 1),
          requireExpiredLease: true,
        }),
      ).toBe('lease_lost');
      expect(await listExpiredLeases(driver.db, { limit: 10, now: at(EMAIL.leaseMs) })).toEqual([]);
      expect(await jobRow(job.id)).toMatchObject({
        status: 'processing',
        lease_token: claimed!.leaseToken,
        started_attempts: 1,
        finished_attempts: 0,
        spent_attempts: 0,
      });
    });
  });

  describe('listExpiredLeases', () => {
    it('lists only processing jobs past their lease, oldest first', async () => {
      // distinct enqueue times: equal due times have no defined order
      const a = await enqueue({}, at(0));
      const b = await enqueue({}, at(1));
      await claimOne(at(1));
      await claimOne(at(10));
      await enqueue();

      expect(await listExpiredLeases(driver.db, { limit: 10, now: at(EMAIL.leaseMs + 1) })).toEqual(
        [],
      );
      expect(
        (await listExpiredLeases(driver.db, { limit: 10, now: at(EMAIL.leaseMs + 11) })).map(
          (l) => l.id,
        ),
      ).toEqual([a.id, b.id]);
      expect(
        await listExpiredLeases(driver.db, { limit: 1, now: at(EMAIL.leaseMs + 11) }),
      ).toHaveLength(1);
    });
  });

  describe('cancelJob / replayJob', () => {
    it('cancels a waiting job by id or dedupe key, and refuses in-flight or terminal ones', async () => {
      const byId = await enqueue();
      await enqueue({ dedupeKey: 'cancel-me' });
      const inFlight = await enqueue();
      await claimByIds(driver.db, {
        ids: [inFlight.id],
        kinds: [EMAIL],
        leaseToken: uuidv7(),
        now: T0,
      });

      expect(await cancelJob(driver.db, { ref: { id: byId.id }, reason: 'expired', now: T0 })).toBe(
        'cancelled',
      );
      expect(
        await cancelJob(driver.db, {
          ref: { dedupeKey: 'cancel-me', tenantKey: null },
          reason: 'expired',
          now: T0,
        }),
      ).toBe('cancelled');
      expect(
        await cancelJob(driver.db, { ref: { id: inFlight.id }, reason: 'expired', now: T0 }),
      ).toBe('in_flight');
      expect(await cancelJob(driver.db, { ref: { id: byId.id }, reason: 'again', now: T0 })).toBe(
        'already_terminal',
      );
      expect(await cancelJob(driver.db, { ref: { id: uuidv7() }, reason: 'x', now: T0 })).toBe(
        'not_found',
      );
      expect(await jobRow(byId.id)).toMatchObject({
        status: 'cancelled',
        terminal_reason: 'expired',
        finished_at: T0,
      });
    });

    it('replays a dead job with a fresh budget and keeps its attempt history', async () => {
      const job = await enqueue();
      const claimed = await claimOne(T0);
      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: claimed!.leaseToken,
        transition: {
          to: 'dead',
          reason: 'exhausted',
          code: 'x',
          message: null,
          codeStreak: 4,
          attemptOutcome: 'retry',
        },
        now: T0,
      });

      expect(await replayJob(driver.db, { id: job.id, now: at(5) })).toBe('replayed');
      expect(await jobRow(job.id)).toMatchObject({
        status: 'pending',
        spent_attempts: 0,
        finished_attempts: 1,
        started_attempts: 1,
        code_streak: 0,
        terminal_reason: null,
        finished_at: null,
        available_at: at(5),
      });
      expect((await claimOne(at(5)))?.attemptNo).toBe(2);
      expect(await replayJob(driver.db, { id: job.id, now: at(5) })).toBe('not_replayable');
      expect(await replayJob(driver.db, { id: uuidv7(), now: at(5) })).toBe('not_found');
    });
  });

  describe('deleteFinishedJobs', () => {
    // port: cleanup.repository.db:895
    it('deletes finished jobs past their retention window, in bounded batches', async () => {
      const ok1 = await settleAs(succeeded);
      const ok2 = await settleAs(succeeded);
      const deadJob = await settleAs(dead('x'));
      const pending = await enqueue();

      const policy = { succeededRetentionMs: 1_000, terminalRetentionMs: 10_000, now: at(5_000) };

      expect(await deleteFinishedJobs(driver.db, { ...policy, batch: 1 })).toBe(1);
      expect(await deleteFinishedJobs(driver.db, { ...policy, batch: 10 })).toBe(1);
      expect(await deleteFinishedJobs(driver.db, { ...policy, batch: 10 })).toBe(0);
      expect(await jobRow(ok1)).toBeUndefined();
      expect(await jobRow(ok2)).toBeUndefined();
      expect(await jobRow(deadJob)).toBeDefined();
      expect(await jobRow(pending.id)).toBeDefined();

      expect(await deleteFinishedJobs(driver.db, { ...policy, now: at(10_001), batch: 10 })).toBe(
        1,
      );
      expect(await attempts(deadJob)).toEqual([]);
    });
  });

  describe('hasDueJobs', () => {
    it('is true only when a due job of a fitting kind exists', async () => {
      await enqueue({ delayMs: 1_000 });

      expect(await hasDueJobs(driver.db, { kinds: [EMAIL], now: T0 })).toBe(false);
      expect(await hasDueJobs(driver.db, { kinds: [EMAIL], now: at(1_000) })).toBe(true);
      expect(
        await hasDueJobs(driver.db, { kinds: [{ ...EMAIL, fits: false }], now: at(1_000) }),
      ).toBe(false);
    });
  });

  describe('claim filters (baseline)', () => {
    const OTHER: KindFilter = { kind: 'other.kind', leaseMs: 60_000, fits: true };

    // port: process-submission-outbox.service:872
    // port: dispatch-sweep:200
    // port: submission-outbox-queue.server:515
    it('never claims a kind without a handler; the row stays pending and untouched', async () => {
      const orphan = await enqueue({ kind: 'unknown.kind' }, at(0));
      await enqueue({}, at(1));

      expect((await claimOne(at(2), [EMAIL]))?.kind).toBe(EMAIL.kind);
      expect(await claimOne(at(2), [EMAIL])).toBeUndefined();
      expect(
        await claimByIds(driver.db, {
          ids: [orphan.id],
          kinds: [EMAIL],
          leaseToken: uuidv7(),
          now: at(2),
        }),
      ).toEqual([{ id: orphan.id, result: 'unregistered_kind' }]);
      expect(await jobRow(orphan.id)).toMatchObject({
        status: 'pending',
        started_attempts: 0,
        lease_token: null,
      });
      expect(await attempts(orphan.id)).toEqual([]);
    });

    // port: submission-job.repository.claim.db:186
    it('lets consumers with disjoint kinds share the table without stealing each other’s jobs', async () => {
      const foreign = await enqueue({ kind: OTHER.kind }, at(0));
      const mine = await enqueue({}, at(1));

      expect((await claimOne(at(2), [EMAIL]))?.id).toBe(mine.id);
      expect(await claimOne(at(2), [EMAIL])).toBeUndefined();
      expect(await jobRow(foreign.id)).toMatchObject({ status: 'pending' });

      expect((await claimOne(at(2), [EMAIL, OTHER]))?.id).toBe(foreign.id);
    });

    // port: submission-job.repository.claim.db:234
    it('claims nothing when no due job matches the kinds', async () => {
      await enqueue({ kind: OTHER.kind });

      expect(await claimOne(T0, [EMAIL, CRM])).toBeUndefined();
    });

    // port: submission-job.repository.claim.db:248
    it('claims nothing for an empty kind list, issues no SQL and leaves rows pending', async () => {
      const job = await enqueue();
      const { runner, statements } = recordingRunner(driver.db);

      expect(
        await claimDue(runner, {
          kinds: [],
          limit: 5,
          trigger: 'sweep',
          leaseToken: uuidv7(),
          now: T0,
        }),
      ).toEqual([]);
      expect(statements()).toEqual([]);
      expect(await jobRow(job.id)).toMatchObject({ status: 'pending', started_attempts: 0 });
    });

    // port: submission-job.repository.claim.db:275
    it('claims nothing for an empty-string kind', async () => {
      await enqueue();

      expect(await claimOne(T0, [{ kind: '', leaseMs: 60_000, fits: true }])).toBeUndefined();
    });

    // port: submission-job.repository.claim.db:296
    it('claims only the one kind in a single-kind filter', async () => {
      await enqueue({ kind: OTHER.kind }, at(0));
      const crm = await enqueue({ kind: CRM.kind }, at(1));

      expect((await claimOne(at(2), [CRM]))?.id).toBe(crm.id);
      expect(await claimOne(at(2), [CRM])).toBeUndefined();
    });

    // port: submission-job.repository.claim.db:460
    it('takes lease timestamps from the database clock when no time is injected', async () => {
      const { id } = await enqueue({}, new Date('2000-01-01T00:00:00Z'));
      const [claimed] = await claimDue(driver.db, {
        kinds: [EMAIL],
        limit: 1,
        trigger: 'sweep',
        leaseToken: uuidv7(),
        now: null,
      });
      const [clock] = await queryRows<{ db_now: number }>(
        driver.db,
        sql`SELECT (EXTRACT(EPOCH FROM now()) * 1000)::float8 AS db_now`,
      );
      const row = (await jobRow(id)) as { leased_until: Date; updated_at: Date };

      expect(claimed?.id).toBe(id);
      expect(Math.abs(row.leased_until.getTime() - EMAIL.leaseMs - clock!.db_now)).toBeLessThan(
        5_000,
      );
      expect(Math.abs(row.updated_at.getTime() - clock!.db_now)).toBeLessThan(5_000);
    });

    // port: submission-job.repository.claim.db:510
    it('claims a due retrying job and opens its attempt row in the same statement', async () => {
      const job = await enqueue();
      const first = await claimOne(T0);
      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: first!.leaseToken,
        transition: retrying(1_000),
        now: T0,
      });

      const second = await claimOne(at(1_000));

      expect(second).toMatchObject({ id: job.id, attemptNo: 2, spentAttempts: 1 });
      expect(await jobRow(job.id)).toMatchObject({ status: 'processing', next_attempt_at: null });
      expect(await attempts(job.id)).toEqual([
        expect.objectContaining({ attempt_no: 1, outcome: 'retry' }),
        expect.objectContaining({ attempt_no: 2, outcome: null, lease_token: second!.leaseToken }),
      ]);
    });

    // port: submission-job.repository.claim.db:904
    it('lets an earlier-due retry win over later fresh work', async () => {
      const old = await enqueue({}, at(0));
      const claimed = await claimOne(at(0));
      await settleJob(driver.db, {
        jobId: old.id,
        leaseToken: claimed!.leaseToken,
        transition: retrying(500),
        now: at(0),
      });
      await enqueue({}, at(1_000));

      expect((await claimOne(at(2_000)))?.id).toBe(old.id);
    });

    // port: submission-job.repository.claim.db:934
    it('follows due order across pending and retrying over successive single claims', async () => {
      const a = await enqueue({}, at(0));
      const claimedA = await claimOne(at(0));
      await settleJob(driver.db, {
        jobId: a.id,
        leaseToken: claimedA!.leaseToken,
        transition: retrying(2_500),
        now: at(0),
      });
      const b = await enqueue({}, at(1_000));
      const c = await enqueue({ delayMs: 2_000 }, at(1_000));

      const order = [];

      for (let i = 0; i < 3; i++) {
        order.push((await claimOne(at(5_000)))?.id);
      }

      expect(order).toEqual([b.id, a.id, c.id]);
    });

    // port: submission-job.repository.claim.db:984
    it('rolls the whole claim back when its attempt row cannot be written', async () => {
      const job = await enqueue();
      await driver.db.execute(
        sql`INSERT INTO "outbox"."attempt" (job_id, attempt_no, kind, lease_token, trigger, started_at)
            VALUES (${job.id}, 1, ${job.kind}, ${uuidv7()}, 'sweep', now())`,
      );

      await expect(claimOne(T0)).rejects.toMatchObject({
        cause: { message: expect.stringContaining('attempt_pkey') },
      });
      expect(await jobRow(job.id)).toMatchObject({
        status: 'pending',
        started_attempts: 0,
        lease_token: null,
        leased_until: null,
      });
    });
  });

  describe('claimByIds (baseline)', () => {
    // port: submission-job.repository.claim.db:564
    // port: submission-job.repository.claim.db:415
    it('never substitutes another job, even an older due retry; the others stay untouched', async () => {
      const older = await enqueue({}, at(0));
      const olderClaim = await claimOne(at(0));
      await settleJob(driver.db, {
        jobId: older.id,
        leaseToken: olderClaim!.leaseToken,
        transition: retrying(0),
        now: at(0),
      });
      const bystander = await enqueue({}, at(1));
      const current = await enqueue({}, at(2));

      const results = await claimByIds(driver.db, {
        ids: [current.id],
        kinds: [EMAIL],
        leaseToken: uuidv7(),
        now: at(3),
      });

      expect(results.map((r) => [r.id, r.result])).toEqual([[current.id, 'claimed']]);
      expect(await jobRow(older.id)).toMatchObject({ status: 'retrying', started_attempts: 1 });
      expect(await jobRow(bystander.id)).toMatchObject({ status: 'pending', started_attempts: 0 });
      expect(await attempts(bystander.id)).toEqual([]);
    });

    // port: submission-job.repository.claim.db:600
    it('reports not_due for a job that is not due yet and claims nothing else', async () => {
      const future = await enqueue({ delayMs: 60_000 });
      const other = await enqueue();

      expect(
        await claimByIds(driver.db, {
          ids: [future.id],
          kinds: [EMAIL],
          leaseToken: uuidv7(),
          now: T0,
        }),
      ).toEqual([{ id: future.id, result: 'not_due' }]);
      expect(await jobRow(future.id)).toMatchObject({ status: 'pending', started_attempts: 0 });
      expect(await jobRow(other.id)).toMatchObject({ status: 'pending', started_attempts: 0 });
    });

    // port: submission-job.repository.claim.db:623
    it('reports leased for a job another worker holds a live lease on', async () => {
      const job = await enqueue();
      const holder = await claimOne(T0);

      expect(
        await claimByIds(driver.db, {
          ids: [job.id],
          kinds: [EMAIL],
          leaseToken: uuidv7(),
          now: at(1),
        }),
      ).toEqual([{ id: job.id, result: 'leased' }]);
      expect(await jobRow(job.id)).toMatchObject({
        lease_token: holder!.leaseToken,
        started_attempts: 1,
      });
      expect(await attempts(job.id)).toHaveLength(1);
    });

    // port: process-submission-outbox.service:2862
    // port: submission-outbox-queue.server:394
    it('claims nothing at all when the requested id is unclaimable', async () => {
      const finished = await settleAs(succeeded);
      const waiting = await enqueue();

      for (const id of [finished, uuidv7()]) {
        const [result] = await claimByIds(driver.db, {
          ids: [id],
          kinds: [EMAIL],
          leaseToken: uuidv7(),
          now: T0,
        });

        expect(result?.result).not.toBe('claimed');
      }

      expect(await jobRow(waiting.id)).toMatchObject({ status: 'pending', started_attempts: 0 });
    });
  });

  describe('the lease fence (baseline)', () => {
    const cancelled: Transition = {
      to: 'cancelled',
      reason: 'user_deleted',
      attemptOutcome: 'cancelled',
    };

    const forceTerminal = (id: string, status: 'cancelled' | 'succeeded' | 'dead') =>
      driver.db.execute(sql`UPDATE "outbox"."job" SET status = ${status}, lease_token = NULL,
        leased_until = NULL, finished_at = now(),
        terminal_reason = ${status === 'succeeded' ? null : 'operator'} WHERE id = ${id}`);

    // port: submission-job-transition.service:496
    // port: submission-job-transition.service:732
    // port: submission-job-transition.service:928
    // port: submission-job.repository.db:372
    // port: submission-job.repository.db:434
    // port: submission-job.repository.db:470
    // port: submission-job.repository.claim.db:716
    // port: process-submission-outbox.service:110
    it.each([
      ['succeeded', succeeded, 'cancelled'],
      ['retrying', retrying(0), 'cancelled'],
      ['dead', dead(), 'cancelled'],
      ['succeeded', succeeded, 'dead'],
      ['retrying', retrying(0), 'succeeded'],
      ['dead', dead(), 'succeeded'],
    ] as const)(
      'is lease_lost for a %s settle after the job went %s under it, and writes nothing',
      async (_to, transition, external) => {
        const job = await enqueue();
        const claimed = await claimOne(T0);
        await forceTerminal(job.id, external);
        const before = await jobRow(job.id);

        expect(
          await settleJob(driver.db, {
            jobId: job.id,
            leaseToken: claimed!.leaseToken,
            transition,
            now: at(1),
          }),
        ).toBe('lease_lost');
        expect(await jobRow(job.id)).toEqual(before);
        expect(await attempts(job.id)).toEqual([expect.objectContaining({ outcome: null })]);
      },
    );

    // port: submission-job-transition.service:629
    // port: submission-job-transition.service:850
    // port: submission-job-transition.service:977
    // port: submission-job-transition.service:1102
    // port: process-submission-outbox.service:2637
    it.each([
      ['succeeded', succeeded, dead()],
      ['dead', dead(), succeeded],
      ['retrying', retrying(60_000), succeeded],
      ['succeeded', succeeded, cancelled],
      ['dead', dead(), retrying(0)],
    ] as const)(
      "lets a peer's %s stand against a stale worker's late settle",
      async (_peer, peerTransition, staleTransition) => {
        const job = await enqueue();
        const stale = await claimOne(T0);
        await settleJob(driver.db, {
          jobId: job.id,
          leaseToken: stale!.leaseToken,
          transition: { ...retrying(0), code: 'lease_expired', attemptOutcome: 'lease_expired' },
          now: at(EMAIL.leaseMs + 1),
          requireExpiredLease: true,
        });
        const peer = await claimOne(at(EMAIL.leaseMs + 2));
        await settleJob(driver.db, {
          jobId: job.id,
          leaseToken: peer!.leaseToken,
          transition: peerTransition,
          now: at(EMAIL.leaseMs + 3),
        });
        const settled = await jobRow(job.id);
        const ledger = await attempts(job.id);

        expect(
          await settleJob(driver.db, {
            jobId: job.id,
            leaseToken: stale!.leaseToken,
            transition: staleTransition,
            now: at(EMAIL.leaseMs + 4),
          }),
        ).toBe('lease_lost');
        expect(await jobRow(job.id)).toEqual(settled);
        expect(await attempts(job.id)).toEqual(ledger);
        expect(ledger[0]).toMatchObject({ attempt_no: 1, outcome: 'lease_expired' });
      },
    );

    // port: submission-job-transition.service:996
    it('never lets a cancel overwrite a terminal job', async () => {
      const done = await settleAs(succeeded);
      const failed = await settleAs(dead());
      const doneBefore = await jobRow(done);

      expect(await cancelJob(driver.db, { ref: { id: done }, reason: 'skip', now: T0 })).toBe(
        'already_terminal',
      );
      expect(await cancelJob(driver.db, { ref: { id: failed }, reason: 'skip', now: T0 })).toBe(
        'already_terminal',
      );
      expect(await jobRow(done)).toEqual(doneBefore);
      expect(await jobRow(failed)).toMatchObject({ status: 'dead', terminal_reason: 'failed' });
    });

    // port: submission-job-transition.service:952
    // port: process-submission-outbox.service:1942
    it('settles dead with the code verbatim, clearing the due time and the lease', async () => {
      const job = await enqueue();
      const first = await claimOne(T0);
      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: first!.leaseToken,
        transition: retrying(0),
        now: T0,
      });
      const second = await claimOne(at(1));

      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: second!.leaseToken,
        transition: dead('HTTP_404:Not-Found'),
        now: at(2),
      });

      expect(await jobRow(job.id)).toMatchObject({
        status: 'dead',
        last_error_code: 'HTTP_404:Not-Found',
        next_attempt_at: null,
        lease_token: null,
        leased_until: null,
        finished_at: at(2),
      });
      expect((await attempts(job.id))[1]).toMatchObject({
        outcome: 'failed',
        code: 'HTTP_404:Not-Found',
      });
    });

    // port: process-submission-outbox.service:3306
    it('truncates a long error message on the job and its attempt', async () => {
      const job = await enqueue();
      const claimed = await claimOne(T0);

      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: claimed!.leaseToken,
        transition: { ...retrying(0), message: 'x'.repeat(5_000) },
        now: T0,
      });

      const message = (await jobRow(job.id))?.['last_error_message'] as string;
      expect(message).toHaveLength(2_000);
      expect(message.endsWith('…')).toBe(true);
      expect((await attempts(job.id))[0]?.['message']).toBe(message);
    });

    // port: process-submission-outbox.service:3332
    it('persists a short error message verbatim', async () => {
      const job = await enqueue();
      const claimed = await claimOne(T0);

      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: claimed!.leaseToken,
        transition: { ...retrying(0), message: 'upstream said: 503 "busy"\nretry later' },
        now: T0,
      });

      expect(await jobRow(job.id)).toMatchObject({
        last_error_message: 'upstream said: 503 "busy"\nretry later',
      });
    });
  });

  describe('the retry budget through the store', () => {
    const policy = resolveRetryPolicy({ maxAttempts: 10, perCode: { flaky: 3 } });

    const attempt = async (code: string, now: Date) => {
      const [claimed] = await claimDue(driver.db, {
        kinds: [EMAIL],
        limit: 1,
        trigger: 'sweep',
        leaseToken: uuidv7(),
        now,
      });
      const transition = decideTransition({
        outcome: retry({ code, after: 0 }),
        job: claimed!,
        policy,
        timing: { graceMs: 0 },
      });

      await settleJob(driver.db, {
        jobId: claimed!.id,
        leaseToken: claimed!.leaseToken,
        transition,
        now,
      });

      return transition;
    };

    // port: process-submission-outbox.service:1777
    it('persists the streak with each settle, so the next claim decides on the true streak', async () => {
      const job = await enqueue();

      expect(await attempt('flaky', at(1))).toMatchObject({ to: 'retrying', codeStreak: 1 });
      expect(await attempt('flaky', at(2))).toMatchObject({ to: 'retrying', codeStreak: 2 });
      expect(await jobRow(job.id)).toMatchObject({ code_streak: 2, last_error_code: 'flaky' });
      expect(await attempt('flaky', at(3))).toMatchObject({
        to: 'dead',
        reason: 'exhausted',
        codeStreak: 3,
      });
    });

    // port: submission-job.repository.db:552
    // port: submission-job.repository.db:618
    it('gives a replayed job a fresh budget: pre-replay attempts and streaks no longer count', async () => {
      const job = await enqueue();
      await attempt('flaky', at(1));
      await attempt('flaky', at(2));
      await attempt('flaky', at(3));
      await replayJob(driver.db, { id: job.id, now: at(4) });

      expect(await attempt('flaky', at(5))).toMatchObject({ to: 'retrying', codeStreak: 1 });
      expect(await jobRow(job.id)).toMatchObject({
        status: 'retrying',
        started_attempts: 4,
        spent_attempts: 1,
        code_streak: 1,
      });
    });

    it('records the episode, keeps it through a crash settle, and replay ends it', async () => {
      const sticky = resolveRetryPolicy({
        maxAttempts: 2,
        perCode: { awaiting: { maxAttempts: 50, sticky: true } },
      });
      const job = await enqueue();
      const first = await claimOne(T0);
      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: first!.leaseToken,
        transition: decideTransition({
          outcome: retry({ code: 'awaiting', after: 0 }),
          job: first!,
          policy: sticky,
          timing: { graceMs: 0 },
        }),
        now: T0,
      });
      const second = await claimOne(at(1));
      expect(second?.stickyCode).toBe('awaiting');

      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: second!.leaseToken,
        transition: { ...retrying(0), code: 'lease_expired', attemptOutcome: 'lease_expired' },
        now: at(1 + EMAIL.leaseMs + 1),
        requireExpiredLease: true,
      });
      expect(await jobRow(job.id)).toMatchObject({ sticky_code: 'awaiting' });

      const third = await claimOne(at(1 + EMAIL.leaseMs + 2));
      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: third!.leaseToken,
        transition: dead(),
        now: at(1 + EMAIL.leaseMs + 3),
      });
      await replayJob(driver.db, { id: job.id, now: at(1 + EMAIL.leaseMs + 4) });

      expect(await jobRow(job.id)).toMatchObject({ sticky_code: null });
    });

    it('keeps the code and message of a job cancelled by exhaustion', async () => {
      const job = await enqueue();
      const claimed = await claimOne(T0);

      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: claimed!.leaseToken,
        transition: {
          to: 'cancelled',
          reason: 'exhausted',
          code: 'deferred',
          message: 'window closed',
          codeStreak: 4,
          attemptOutcome: 'retry',
        },
        now: T0,
      });

      expect(await jobRow(job.id)).toMatchObject({
        status: 'cancelled',
        terminal_reason: 'exhausted',
        last_error_code: 'deferred',
        last_error_message: 'window closed',
        code_streak: 4,
        spent_attempts: 1,
      });
      expect((await attempts(job.id))[0]).toMatchObject({ outcome: 'retry', code: 'deferred' });
    });
  });

  describe('recovery (baseline)', () => {
    // port: stuck-job-recovery:48
    it('lists nothing when no lease has expired', async () => {
      await enqueue();
      await claimOne(T0);

      expect(await listExpiredLeases(driver.db, { limit: 10, now: at(EMAIL.leaseMs) })).toEqual([]);
    });

    // port: stuck-job-recovery:299
    it('honors the limit; jobs it did not list stay processing', async () => {
      const a = await enqueue({}, at(0));
      const b = await enqueue({}, at(1));
      await claimOne(at(1));
      await claimOne(at(2));

      const listed = await listExpiredLeases(driver.db, { limit: 1, now: at(EMAIL.leaseMs + 10) });

      expect(listed.map((l) => l.id)).toEqual([a.id]);
      expect(await jobRow(b.id)).toMatchObject({ status: 'processing' });
    });

    // port: stuck-job-recovery:493
    it('refuses to recover a job its worker settled after it was listed', async () => {
      const job = await enqueue();
      const claimed = await claimOne(T0);
      const [expired] = await listExpiredLeases(driver.db, {
        limit: 1,
        now: at(EMAIL.leaseMs + 1),
      });

      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: claimed!.leaseToken,
        transition: succeeded,
        now: at(EMAIL.leaseMs + 2),
      });
      const settled = await jobRow(job.id);

      expect(
        await settleJob(driver.db, {
          jobId: expired!.id,
          leaseToken: expired!.leaseToken,
          transition: { ...retrying(0), code: 'lease_expired', attemptOutcome: 'lease_expired' },
          now: at(EMAIL.leaseMs + 3),
          requireExpiredLease: true,
        }),
      ).toBe('lease_lost');
      expect(await jobRow(job.id)).toEqual(settled);
    });

    // port: job-outbox-nudge.repository.db:145
    // port: job-outbox-nudge.repository.db:163
    it('keys expiry on leased_until alone: an unrelated write to a processing row cannot delay it', async () => {
      const job = await enqueue();
      await claimOne(T0);

      await driver.db.execute(
        sql`UPDATE "outbox"."job" SET updated_at = ${at(EMAIL.leaseMs).toISOString()}::timestamptz,
            metadata = '{"touched":true}' WHERE id = ${job.id}`,
      );
      expect(
        await recordNudgeResult(driver.db, { ids: [job.id], error: null, now: at(EMAIL.leaseMs) }),
      ).toBe(0);

      expect(
        (await listExpiredLeases(driver.db, { limit: 10, now: at(EMAIL.leaseMs + 1) })).map(
          (l) => l.id,
        ),
      ).toEqual([job.id]);
    });

    it('lists only the given kinds, and nothing (with no SQL) for an empty list', async () => {
      await enqueue({}, at(0));
      const crm = await enqueue({ kind: CRM.kind }, at(1));
      await claimOne(at(1));
      await claimOne(at(2));
      const now = at(CRM.leaseMs + 10);
      const { runner, statements } = recordingRunner(driver.db);

      expect(
        (await listExpiredLeases(driver.db, { limit: 10, now, kinds: [CRM.kind] })).map(
          (l) => l.id,
        ),
      ).toEqual([crm.id]);
      expect(await listExpiredLeases(runner, { limit: 10, now, kinds: [] })).toEqual([]);
      expect(statements()).toEqual([]);
    });
  });

  describe('retention (baseline)', () => {
    const DAY = 86_400_000;
    const retention = {
      succeededRetentionMs: DEFAULT_SUCCEEDED_RETENTION_MS,
      terminalRetentionMs: DEFAULT_TERMINAL_RETENTION_MS,
      batch: 100,
    };
    const cancelledAt = async (now: Date) => {
      const { id } = await enqueue({}, now);
      await cancelJob(driver.db, { ref: { id }, reason: 'expired', now });

      return id;
    };

    // port: cleanup.repository.db:918
    it('deletes succeeded jobs finished more than 7 days ago and keeps newer ones', async () => {
      const old = await settleAs(succeeded, at(0));
      const edge = await settleAs(succeeded, at(1));

      await deleteFinishedJobs(driver.db, { ...retention, now: at(7 * DAY + 1) });

      expect(await jobRow(old)).toBeUndefined();
      expect(await jobRow(edge)).toBeDefined();
    });

    // port: cleanup.repository.db:946
    it('deletes dead jobs finished more than 90 days ago and keeps newer ones', async () => {
      const old = await settleAs(dead(), at(0));
      const recent = await settleAs(dead(), at(DAY));

      await deleteFinishedJobs(driver.db, { ...retention, now: at(90 * DAY + 1) });

      expect(await jobRow(old)).toBeUndefined();
      expect(await jobRow(recent)).toBeDefined();
    });

    // port: cleanup.repository.db:862
    it('deletes old cancelled jobs and keeps recent cancelled, dead and waiting ones', async () => {
      const old = await cancelledAt(at(0));
      const recent = await cancelledAt(at(DAY));
      const deadRecent = await settleAs(dead(), at(DAY));
      const waiting = await enqueue({}, at(0));

      await deleteFinishedJobs(driver.db, { ...retention, now: at(90 * DAY + 1) });

      expect(await jobRow(old)).toBeUndefined();
      expect(await jobRow(recent)).toBeDefined();
      expect(await jobRow(deadRecent)).toBeDefined();
      expect(await jobRow(waiting.id)).toBeDefined();
    });
  });

  it('embeds now() in every statement so Hyperdrive never caches one', async () => {
    const { runner, statements } = recordingRunner(driver.db);
    const job = newJob();

    await insertJob(runner, job, null);
    const [claimed] = await claimDue(runner, {
      kinds: [EMAIL],
      limit: 1,
      trigger: 'sweep',
      leaseToken: uuidv7(),
      now: null,
    });
    await claimByIds(runner, { ids: [job.id], kinds: [EMAIL], leaseToken: uuidv7(), now: null });
    await settleJob(runner, {
      jobId: job.id,
      leaseToken: claimed!.leaseToken,
      transition: succeeded,
      now: null,
    });
    await listExpiredLeases(runner, { limit: 1, now: null });
    await cancelJob(runner, { ref: { id: job.id }, reason: 'x', now: null });
    await replayJob(runner, { id: job.id, now: null });
    await deleteFinishedJobs(runner, {
      succeededRetentionMs: 1,
      terminalRetentionMs: 1,
      batch: 1,
      now: null,
    });
    await hasDueJobs(runner, { kinds: [EMAIL], now: null });
    // A cached reply would silently skip the native settlement timeouts.
    await setSettlementTimeouts(runner, {
      transactionMs: 1_000,
      statementMs: 1_000,
      lockMs: 1_000,
    });

    expect(statements()).toHaveLength(10);
    for (const statement of statements()) {
      expect(statement).toContain('now()');
    }
  });
});
