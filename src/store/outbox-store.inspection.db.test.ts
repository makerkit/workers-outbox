import { v7 as uuidv7 } from 'uuid';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { testDrivers, type TestDriver } from '../test-support/drivers';
import { recordingRunner } from '../test-support/recording-runner';
import {
  at,
  CRM,
  dead,
  EMAIL,
  retrying,
  storeHelpers,
  succeeded,
  T0,
} from '../test-support/store-fixtures';
import {
  aggregateAttemptOutcomes,
  cancelJob,
  claimByIds,
  claimDue,
  countBacklog,
  countJobs,
  isLeaseHeld,
  listDueJobIds,
  listExpiredLeases,
  listJobs,
  recordNudgeResult,
  replayJob,
  settleJob,
} from './outbox-store';

describe.each(testDrivers())('outbox store inspection on $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  const { jobRow, enqueue, claimOne, settleAs } = storeHelpers(() => driver.db);

  describe('listJobs filters for bulk operator work', () => {
    it('filters by created and finished windows, terminal reason and dedupe key', async () => {
      const early = await settleAs(dead('x'), at(0), { dedupeKey: 'k-early' });
      const mid = await settleAs(dead('x'), at(60_000), { dedupeKey: 'k-mid' });
      const late = await settleAs(dead('x'), at(120_000), { dedupeKey: 'k-late' });
      const ids = async (filter: Parameters<typeof listJobs>[1]) =>
        (await listJobs(driver.db, filter)).jobs.map((j) => j.id);

      expect(await ids({ status: 'dead', limit: 10, finishedAfter: at(30_000) })).toEqual([
        mid,
        late,
      ]);
      expect(await ids({ status: 'dead', limit: 10, createdBefore: at(90_000) })).toEqual([
        early,
        mid,
      ]);
      expect(await ids({ status: 'dead', limit: 10, terminalReason: 'failed' })).toEqual([
        early,
        mid,
        late,
      ]);
      expect(await ids({ status: 'dead', limit: 10, terminalReason: 'exhausted' })).toEqual([]);
      expect(await ids({ status: 'dead', limit: 10, dedupeKey: 'k-mid' })).toEqual([mid]);
      expect(await countJobs(driver.db, { status: 'dead', finishedAfter: at(30_000) })).toBe(2);
    });
  });

  describe('listJobs / countJobs', () => {
    // port: generic-outbox-job.repository:67
    // port: generic-outbox-job.repository:99
    it('lists nothing and counts 0 when no job has the status', async () => {
      await enqueue();

      expect(await listJobs(driver.db, { status: 'dead', limit: 10 })).toEqual({
        jobs: [],
        nextCursor: null,
      });
      expect(await countJobs(driver.db, { status: 'dead' })).toBe(0);
    });

    // port: generic-outbox-job.repository:75
    // port: generic-outbox-job.repository:107
    it('lists and counts the jobs in a status, filtered by kind and tenant', async () => {
      const a = await settleAs(dead('http_404'), at(0), { tenantKey: 'org-1' });
      const b = await settleAs(dead(), at(1), { kind: CRM.kind, tenantKey: 'org-1' });
      await settleAs(dead(), at(2), { tenantKey: 'org-2' });
      await settleAs(succeeded, at(3), { tenantKey: 'org-1' });

      const { jobs } = await listJobs(driver.db, { status: 'dead', tenantKey: 'org-1', limit: 10 });

      expect(jobs.map((j) => j.id)).toEqual([a, b]);
      expect(jobs[0]).toMatchObject({
        id: a,
        kind: EMAIL.kind,
        status: 'dead',
        tenantKey: 'org-1',
        terminalReason: 'failed',
        lastErrorCode: 'http_404',
        startedAttempts: 1,
        finishedAttempts: 1,
        createdAt: at(0),
        finishedAt: at(0),
        nextAttemptAt: null,
        nudgeAttempts: 0,
      });
      expect(jobs[0]).not.toHaveProperty('payload');
      expect(await countJobs(driver.db, { status: 'dead' })).toBe(3);
      expect(await countJobs(driver.db, { status: 'dead', kind: CRM.kind })).toBe(1);
      expect(await countJobs(driver.db, { status: 'dead', tenantKey: 'org-1' })).toBe(2);
      expect(await countJobs(driver.db, { status: 'dead', tenantKey: null })).toBe(0);
    });

    it('pages with a keyset cursor, oldest first, and rejects a malformed cursor', async () => {
      const ids = [];

      for (let i = 0; i < 5; i++) {
        ids.push((await enqueue({}, at(i))).id);
      }

      const first = await listJobs(driver.db, { status: 'pending', limit: 2 });
      const second = await listJobs(driver.db, {
        status: 'pending',
        limit: 2,
        cursor: first.nextCursor,
      });
      const third = await listJobs(driver.db, {
        status: 'pending',
        limit: 2,
        cursor: second.nextCursor,
      });

      expect([...first.jobs, ...second.jobs, ...third.jobs].map((j) => j.id)).toEqual(ids);
      expect(third.nextCursor).toBeNull();
      await expect(
        listJobs(driver.db, { status: 'pending', limit: 2, cursor: 'nope' }),
      ).rejects.toThrow(/invalid cursor/);
    });
  });

  describe('aggregateAttemptOutcomes', () => {
    const HOUR = 3_600_000;

    // port: precondition-violated-sweep:29
    it('returns no groups when no attempt finished in the window', async () => {
      await settleAs(dead(), at(0));

      expect(
        await aggregateAttemptOutcomes(driver.db, {
          windowMs: HOUR,
          maxRows: 10,
          now: at(2 * HOUR),
        }),
      ).toEqual({ cutoff: at(HOUR), groups: [], truncated: false });
    });

    // port: precondition-violated-sweep:42
    // port: precondition-violated-sweep:93
    it('groups attempts per kind, tenant, outcome and code, keeping the null-tenant bucket', async () => {
      await settleAs(dead('precondition'), at(0), { tenantKey: 'org-1' });
      await settleAs(dead('precondition'), at(1), { tenantKey: 'org-1' });
      await settleAs(dead('precondition'), at(2), { tenantKey: null });
      await settleAs(dead('precondition'), at(3), { kind: CRM.kind, tenantKey: 'org-1' });
      await settleAs(succeeded, at(4), { tenantKey: 'org-1' });

      const result = await aggregateAttemptOutcomes(driver.db, {
        windowMs: HOUR,
        maxRows: 10,
        now: at(10),
        code: 'precondition',
      });

      expect(result.groups).toEqual([
        {
          kind: EMAIL.kind,
          tenantKey: 'org-1',
          outcome: 'failed',
          code: 'precondition',
          count: 2,
        },
        { kind: CRM.kind, tenantKey: 'org-1', outcome: 'failed', code: 'precondition', count: 1 },
        { kind: EMAIL.kind, tenantKey: null, outcome: 'failed', code: 'precondition', count: 1 },
      ]);
      expect(result.truncated).toBe(false);
    });

    // port: precondition-violated-sweep:127
    // port: precondition-violated-sweep:147
    it('flags truncation exactly when there are more groups than maxRows', async () => {
      await settleAs(dead('a'), at(0));
      await settleAs(dead('b'), at(1));
      await settleAs(dead('c'), at(2));
      const aggregate = (maxRows: number) =>
        aggregateAttemptOutcomes(driver.db, { windowMs: HOUR, maxRows, now: at(10) });

      expect(await aggregate(2)).toMatchObject({ truncated: true, groups: { length: 2 } });
      expect(await aggregate(3)).toMatchObject({ truncated: false, groups: { length: 3 } });
    });

    // port: precondition-violated-sweep:161
    // port: precondition-violated-sweep:176
    it('cuts off at now − window on the finish time: older attempts are excluded, the boundary kept', async () => {
      await settleAs(dead('old'), at(0));
      await settleAs(dead('edge'), at(1));
      await settleAs(dead('new'), at(2));

      const result = await aggregateAttemptOutcomes(driver.db, {
        windowMs: HOUR - 1,
        maxRows: 10,
        now: at(HOUR),
      });

      expect(result.cutoff).toEqual(at(1));
      expect(result.groups.map((g) => g.code).sort()).toEqual(['edge', 'new']);
    });

    it('filters by kinds, tenant and outcomes', async () => {
      await settleAs(dead(), at(0), { tenantKey: 'org-1' });
      await settleAs(retrying(60_000), at(1), { tenantKey: 'org-1' });
      await settleAs(dead(), at(2), { kind: CRM.kind, tenantKey: 'org-2' });

      const result = await aggregateAttemptOutcomes(driver.db, {
        windowMs: HOUR,
        maxRows: 10,
        now: at(10),
        kinds: [EMAIL.kind],
        tenantKey: 'org-1',
        outcomes: ['retry'],
      });

      expect(result.groups).toEqual([
        { kind: EMAIL.kind, tenantKey: 'org-1', outcome: 'retry', code: 'http_503', count: 1 },
      ]);
    });
  });

  describe('countBacklog', () => {
    const AGED = 600_000;

    it('counts per kind: due, aged, scheduled, processing and stuck', async () => {
      await enqueue({}, at(0));
      await enqueue({ delayMs: 10 * AGED }, at(0));
      await enqueue({ kind: CRM.kind }, at(0));
      await claimOne(at(0), [CRM]);

      const now = at(AGED + 1);

      expect(await countBacklog(driver.db, { agedMs: AGED, now })).toEqual([
        {
          kind: CRM.kind,
          due: 0,
          aged: 0,
          scheduled: 0,
          processing: 1,
          stuck: 1,
          oldestDueAgeMs: null,
        },
        {
          kind: EMAIL.kind,
          due: 1,
          aged: 1,
          scheduled: 1,
          processing: 0,
          stuck: 0,
          oldestDueAgeMs: AGED + 1,
        },
      ]);
    });

    // port: outbox-depth.monitor.repository.db:58
    it('counts a due job as aged once it has waited past its due time beyond the threshold', async () => {
      await enqueue({}, at(0));

      const aged = async (now: Date) =>
        (await countBacklog(driver.db, { agedMs: AGED, now }))[0]?.aged;

      expect(await aged(at(AGED))).toBe(0);
      expect(await aged(at(AGED + 1))).toBe(1);
    });

    // port: outbox-depth.monitor.repository.db:74
    // port: outbox-depth.monitor:121
    it('never counts a job scheduled in the future as aged, however old the row is', async () => {
      await enqueue({ delayMs: 100 * AGED }, at(0));

      expect(await countBacklog(driver.db, { agedMs: AGED, now: at(50 * AGED) })).toEqual([
        expect.objectContaining({ due: 0, aged: 0, scheduled: 1 }),
      ]);
    });

    // port: outbox-depth.monitor:244
    it('calls a job stuck at exactly the instant recovery would list it', async () => {
      await enqueue();
      await claimOne(T0);
      const probe = async (now: Date) => ({
        stuck: (await countBacklog(driver.db, { agedMs: AGED, now }))[0]?.stuck,
        listed: (await listExpiredLeases(driver.db, { limit: 10, now })).length,
      });

      expect(await probe(at(EMAIL.leaseMs))).toEqual({ stuck: 0, listed: 0 });
      expect(await probe(at(EMAIL.leaseMs + 1))).toEqual({ stuck: 1, listed: 1 });
    });

    it('counts only the given kinds and tenant', async () => {
      await enqueue({ tenantKey: 'org-1' });
      await enqueue({ tenantKey: 'org-2' });
      await enqueue({ kind: CRM.kind, tenantKey: 'org-1' });

      expect(
        await countBacklog(driver.db, {
          agedMs: AGED,
          now: T0,
          kinds: [EMAIL.kind],
          tenantKey: 'org-1',
        }),
      ).toEqual([expect.objectContaining({ kind: EMAIL.kind, due: 1 })]);
      expect(await countBacklog(driver.db, { agedMs: AGED, now: T0, kinds: [] })).toEqual([]);
    });
  });

  describe('recordNudgeResult', () => {
    // port: job-outbox-nudge.repository.db:67
    it('stamps a delivered nudge, counts it and clears the last error', async () => {
      const { id } = await enqueue();
      await recordNudgeResult(driver.db, { ids: [id], error: 'queue unavailable', now: at(1) });

      expect(await recordNudgeResult(driver.db, { ids: [id], error: null, now: at(2) })).toBe(1);
      expect(await jobRow(id)).toMatchObject({
        nudge_sent_at: at(2),
        nudge_attempts: 2,
        last_nudge_error: null,
        updated_at: T0,
      });
    });

    // port: job-outbox-nudge.repository.db:110
    it('counts every failed send without stamping delivery', async () => {
      const { id } = await enqueue();

      for (let i = 1; i <= 3; i++) {
        await recordNudgeResult(driver.db, { ids: [id], error: `send failed ${i}`, now: at(i) });
      }

      expect(await jobRow(id)).toMatchObject({
        nudge_sent_at: null,
        nudge_attempts: 3,
        last_nudge_error: 'send failed 3',
      });
    });

    // port: job-outbox-nudge.repository.db:99
    it('bounds the stored error text', async () => {
      const { id } = await enqueue();
      await recordNudgeResult(driver.db, { ids: [id], error: 'e'.repeat(10_000), now: T0 });

      expect(await jobRow(id)).toMatchObject({ last_nudge_error: `${'e'.repeat(499)}…` });
    });

    // port: job-outbox-nudge.repository.db:123
    it('writes only the given rows, and skips malformed ids instead of failing the batch', async () => {
      const target = await enqueue();
      const other = await enqueue();

      expect(
        await recordNudgeResult(driver.db, {
          ids: [target.id, 'not-a-uuid'],
          error: null,
          now: T0,
        }),
      ).toBe(1);
      expect(await jobRow(other.id)).toMatchObject({ nudge_sent_at: null, nudge_attempts: 0 });
    });

    // port: job-outbox-nudge.repository.db:179
    it('stamps a retrying job awaiting re-pickup, but never a claimed or finished one', async () => {
      const waiting = await settleAs(retrying(60_000));
      const finished = await settleAs(succeeded);

      expect(
        await recordNudgeResult(driver.db, { ids: [waiting, finished], error: null, now: at(1) }),
      ).toBe(1);
      expect(await jobRow(waiting)).toMatchObject({ nudge_sent_at: at(1), nudge_attempts: 1 });
      expect(await jobRow(finished)).toMatchObject({ nudge_sent_at: null, nudge_attempts: 0 });
    });
  });

  describe('tenant scope', () => {
    // port: submission-job.repository.claim.db:313
    it('claims only the scoped tenant, skipping an older job of another tenant', async () => {
      const foreign = await enqueue({ tenantKey: 'org-2' }, at(0));
      const mine = await enqueue({ tenantKey: 'org-1' }, at(1));
      const claim = () =>
        claimDue(driver.db, {
          kinds: [EMAIL],
          limit: 5,
          trigger: 'sweep',
          leaseToken: uuidv7(),
          now: at(2),
          tenantKey: 'org-1',
        });

      expect((await claim()).map((j) => j.id)).toEqual([mine.id]);
      expect(await jobRow(foreign.id)).toMatchObject({ status: 'pending', started_attempts: 0 });
    });

    it('scopes null to jobs without a tenant', async () => {
      const system = await enqueue({ tenantKey: null }, at(1));
      await enqueue({ tenantKey: 'org-1' }, at(0));

      const claimed = await claimDue(driver.db, {
        kinds: [EMAIL],
        limit: 5,
        trigger: 'sweep',
        leaseToken: uuidv7(),
        now: at(2),
        tenantKey: null,
      });

      expect(claimed.map((j) => j.id)).toEqual([system.id]);
    });

    it("treats another tenant's job as not found for claimByIds, cancel and replay, and leaves it untouched", async () => {
      const deadId = await settleAs(dead(), T0, { tenantKey: 'org-2' });
      const waiting = await enqueue({ tenantKey: 'org-2' });
      const scope = { tenantKey: 'org-1', now: T0 };

      expect(
        await claimByIds(driver.db, {
          ids: [waiting.id],
          kinds: [EMAIL],
          leaseToken: uuidv7(),
          ...scope,
        }),
      ).toEqual([{ id: waiting.id, result: 'not_found' }]);
      expect(await cancelJob(driver.db, { ref: { id: waiting.id }, reason: 'x', ...scope })).toBe(
        'not_found',
      );
      expect(await replayJob(driver.db, { id: deadId, ...scope })).toBe('not_found');

      expect(await jobRow(waiting.id)).toMatchObject({ status: 'pending', started_attempts: 0 });
      expect(await jobRow(deadId)).toMatchObject({ status: 'dead' });

      expect(
        await cancelJob(driver.db, {
          ref: { id: waiting.id },
          reason: 'x',
          tenantKey: 'org-2',
          now: T0,
        }),
      ).toBe('cancelled');
      expect(await replayJob(driver.db, { id: deadId, tenantKey: 'org-2', now: T0 })).toBe(
        'replayed',
      );
    });
  });

  describe('listDueJobIds', () => {
    it('lists due ids in claim order without claiming them', async () => {
      const a = await enqueue({}, at(0));
      const b = await enqueue({ kind: CRM.kind }, at(1));
      await enqueue({ delayMs: 60_000 }, at(0));
      await enqueue({ kind: 'other.kind' }, at(0));

      expect(
        await listDueJobIds(driver.db, {
          kinds: [EMAIL.kind, CRM.kind],
          limit: 10,
          nudgeCooldownMs: 0,
          now: at(2),
        }),
      ).toEqual([a.id, b.id]);
      expect(
        await listDueJobIds(driver.db, {
          kinds: [CRM.kind],
          limit: 10,
          nudgeCooldownMs: 0,
          now: at(2),
        }),
      ).toEqual([b.id]);
      expect(await jobRow(a.id)).toMatchObject({ status: 'pending', started_attempts: 0 });
    });

    it('skips jobs nudged within the cooldown', async () => {
      const fresh = await enqueue({}, at(0));
      const stale = await enqueue({}, at(0));
      await recordNudgeResult(driver.db, { ids: [stale.id], error: null, now: at(1) });
      await recordNudgeResult(driver.db, { ids: [fresh.id], error: null, now: at(9) });

      expect(
        await listDueJobIds(driver.db, {
          kinds: [EMAIL.kind],
          limit: 10,
          now: at(10),
          nudgeCooldownMs: 5,
        }),
      ).toEqual([stale.id]);
    });

    it('honors the limit and tenant, and runs no SQL for an empty kind list', async () => {
      await enqueue({ tenantKey: 'org-1' }, at(0));
      const second = await enqueue({ tenantKey: 'org-2' }, at(1));
      await enqueue({ tenantKey: 'org-2' }, at(2));
      const { runner, statements } = recordingRunner(driver.db);

      expect(
        await listDueJobIds(driver.db, {
          kinds: [EMAIL.kind],
          limit: 1,
          nudgeCooldownMs: 0,
          now: at(3),
          tenantKey: 'org-2',
        }),
      ).toEqual([second.id]);
      expect(
        await listDueJobIds(runner, { kinds: [], limit: 10, nudgeCooldownMs: 0, now: at(3) }),
      ).toEqual([]);
      expect(statements()).toEqual([]);
    });
  });

  describe('isLeaseHeld', () => {
    it('is true only for the live lease token of a processing job, and takes no lock', async () => {
      const job = await enqueue();
      const claimed = await claimOne(T0);
      const held = (leaseToken: string, now: Date, requireExpiredLease?: boolean) =>
        isLeaseHeld(driver.db, {
          jobId: job.id,
          leaseToken,
          now,
          ...(requireExpiredLease === undefined ? {} : { requireExpiredLease }),
        });

      expect(await held(claimed!.leaseToken, at(1))).toBe(true);
      expect(await held(uuidv7(), at(1))).toBe(false);
      expect(await held('not-a-uuid', at(1))).toBe(false);
      expect(await held(claimed!.leaseToken, at(EMAIL.leaseMs), true)).toBe(false);
      expect(await held(claimed!.leaseToken, at(EMAIL.leaseMs + 1), true)).toBe(true);

      const { runner, statements } = recordingRunner(driver.db);
      await isLeaseHeld(runner, { jobId: job.id, leaseToken: claimed!.leaseToken, now: null });
      expect(statements()[0]).not.toMatch(/FOR (UPDATE|SHARE)/);

      await settleJob(driver.db, {
        jobId: job.id,
        leaseToken: claimed!.leaseToken,
        transition: succeeded,
        now: at(2),
      });
      expect(await held(claimed!.leaseToken, at(3))).toBe(false);
    });
  });

  it('embeds now() in every new statement so Hyperdrive never caches one', async () => {
    const { id } = await enqueue();
    const { runner, statements } = recordingRunner(driver.db);

    await listDueJobIds(runner, { kinds: [EMAIL.kind], limit: 1, nudgeCooldownMs: 0, now: null });
    await countBacklog(runner, { agedMs: 1, now: null });
    await listJobs(runner, { status: 'pending', limit: 1 });
    await countJobs(runner, { status: 'pending' });
    await aggregateAttemptOutcomes(runner, { windowMs: 1, maxRows: 1, now: null });
    await recordNudgeResult(runner, { ids: [id], error: null, now: null });
    await isLeaseHeld(runner, { jobId: id, leaseToken: uuidv7(), now: null });
    await listExpiredLeases(runner, { limit: 1, now: null, kinds: [EMAIL.kind] });

    expect(statements()).toHaveLength(8);
    for (const statement of statements()) {
      expect(statement).toContain('now()');
    }
  });
});
