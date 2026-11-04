import { sql } from 'drizzle-orm';
import { v7 as uuidv7 } from 'uuid';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createOutboxTestDatabase,
  type OutboxTestDatabase,
} from '../testing/create-outbox-test-database';

describe('outbox DDL constraints', () => {
  let testDb: OutboxTestDatabase;

  beforeAll(async () => {
    testDb = await createOutboxTestDatabase();
  });

  beforeEach(() => testDb.reset());

  afterAll(() => testDb.close());

  const insertJob = (overrides: Record<string, unknown> = {}) => {
    const row: Record<string, unknown> = {
      id: uuidv7(),
      kind: 'email.welcome',
      dedupe_key: `welcome:${uuidv7()}`,
      payload: '{}',
      ...overrides,
    };

    const columns = sql.join(
      Object.keys(row).map((key) => sql.identifier(key)),
      sql`, `,
    );
    const values = sql.join(
      Object.values(row).map((value) => sql`${value}`),
      sql`, `,
    );

    return testDb.db.execute(sql`INSERT INTO "outbox"."job" (${columns}) VALUES (${values})`);
  };

  const rejects = (promise: Promise<unknown>, constraint: string) =>
    expect(promise).rejects.toMatchObject({
      cause: { message: expect.stringContaining(constraint) },
    });

  it('defaults the baseline-addition columns so old writers keep working', async () => {
    const id = uuidv7();

    await insertJob({ id });

    const rows = await testDb.db.execute(
      sql`SELECT sticky_code, nudge_sent_at, nudge_attempts, last_nudge_error FROM "outbox"."job" WHERE id = ${id}`,
    );

    expect(rows.rows).toEqual([
      { sticky_code: null, nudge_sent_at: null, nudge_attempts: 0, last_nudge_error: null },
    ]);
  });

  it('accepts a minimal pending job with defaults', async () => {
    const id = uuidv7();

    await insertJob({ id });

    const rows = await testDb.db.execute(
      sql`SELECT status, metadata, started_attempts, spent_attempts FROM "outbox"."job" WHERE id = ${id}`,
    );

    expect(rows.rows).toEqual([
      { status: 'pending', metadata: {}, started_attempts: 0, spent_attempts: 0 },
    ]);
  });

  it('rejects an unknown status', async () => {
    await rejects(insertJob({ status: 'paused' }), 'job_status_check');
  });

  it('rejects a duplicate dedupe key', async () => {
    await insertJob({ dedupe_key: 'same' });

    await rejects(insertJob({ dedupe_key: 'same' }), 'job_tenant_dedupe_key_unique');
    await insertJob({ dedupe_key: 'same', tenant_key: 'org-a' });
    await insertJob({ dedupe_key: 'same', tenant_key: 'org-b' });
    await rejects(
      insertJob({ dedupe_key: 'same', tenant_key: 'org-a' }),
      'job_tenant_dedupe_key_unique',
    );
  });

  // port: submission-job.repository.claim.db:499
  it('rejects a retrying job with no due time, which could never be claimed', async () => {
    await rejects(insertJob({ status: 'retrying' }), 'job_retrying_requires_next_attempt_at');
  });

  it('requires a lease exactly while processing', async () => {
    await rejects(
      insertJob({ status: 'processing', started_attempts: 1 }),
      'job_processing_iff_leased',
    );
    await rejects(
      insertJob({ lease_token: uuidv7(), leased_until: new Date().toISOString() }),
      'job_processing_iff_leased',
    );

    await insertJob({
      status: 'processing',
      started_attempts: 1,
      lease_token: uuidv7(),
      leased_until: new Date().toISOString(),
    });
  });

  it('keeps attempt counters consistent', async () => {
    await rejects(insertJob({ finished_attempts: 1 }), 'job_attempt_counters');
    await rejects(insertJob({ spent_attempts: 1 }), 'job_attempt_counters');
    await rejects(insertJob({ code_streak: -1 }), 'job_attempt_counters');
  });

  it('stamps finished_at exactly on terminal jobs', async () => {
    await rejects(insertJob({ status: 'succeeded' }), 'job_terminal_iff_finished_at');
    await rejects(
      insertJob({ finished_at: new Date().toISOString() }),
      'job_terminal_iff_finished_at',
    );
  });

  it('requires a reason exactly on dead and cancelled jobs', async () => {
    const finished_at = new Date().toISOString();

    await rejects(insertJob({ status: 'dead', finished_at }), 'job_terminal_reason');
    await rejects(
      insertJob({ status: 'succeeded', finished_at, terminal_reason: 'exhausted' }),
      'job_terminal_reason',
    );

    await insertJob({ status: 'cancelled', finished_at, terminal_reason: 'expired' });
  });

  describe('attempt ledger', () => {
    const insertAttempt = (jobId: string, overrides: Record<string, unknown> = {}) => {
      const row = {
        attemptNo: 1,
        trigger: 'nudge',
        outcome: null,
        finishedAt: null,
        ...overrides,
      };

      return testDb.db.execute(sql`
        INSERT INTO "outbox"."attempt" (job_id, attempt_no, kind, lease_token, trigger, started_at, outcome, finished_at)
        VALUES (${jobId}, ${row.attemptNo}, 'test.kind', ${uuidv7()}, ${row.trigger}, now(), ${row.outcome}, ${row.finishedAt})`);
    };

    it('numbers attempts uniquely per job and cascades on job delete', async () => {
      const jobId = uuidv7();
      await insertJob({ id: jobId });
      await insertAttempt(jobId);

      await rejects(insertAttempt(jobId), 'attempt_pkey');

      await testDb.db.execute(sql`DELETE FROM "outbox"."job" WHERE id = ${jobId}`);
      const left = await testDb.db.execute(sql`SELECT count(*)::int AS n FROM "outbox"."attempt"`);

      expect(left.rows).toEqual([{ n: 0 }]);
    });

    it('rejects an attempt for a job that does not exist', async () => {
      await rejects(insertAttempt(uuidv7()), 'attempt_job_id_fkey');
    });

    it('rejects unknown triggers and outcomes, and outcomes without a finish time', async () => {
      const jobId = uuidv7();
      await insertJob({ id: jobId });

      await rejects(insertAttempt(jobId, { trigger: 'manual' }), 'attempt_trigger_check');
      await rejects(
        insertAttempt(jobId, { outcome: 'exploded', finishedAt: new Date().toISOString() }),
        'attempt_outcome_check',
      );
      await rejects(insertAttempt(jobId, { outcome: 'succeeded' }), 'attempt_finished_iff_outcome');
    });

    // port: async-job-completion-stamp.schema.db:71
    // port: async-job-completion-stamp.schema.db:77
    it('rejects a finished outcome without a finish time', async () => {
      const jobId = uuidv7();
      await insertJob({ id: jobId });

      await rejects(insertAttempt(jobId, { outcome: 'succeeded' }), 'attempt_finished_iff_outcome');
      await rejects(
        insertAttempt(jobId, { attemptNo: 2, outcome: 'failed' }),
        'attempt_finished_iff_outcome',
      );
      await rejects(
        insertAttempt(jobId, { attemptNo: 3, finishedAt: new Date().toISOString() }),
        'attempt_finished_iff_outcome',
      );
    });

    // port: async-job-completion-stamp.schema.db:81
    // port: async-job-completion-stamp.schema.db:87
    it('accepts a finished attempt with its stamp, and an open attempt without one', async () => {
      const jobId = uuidv7();
      await insertJob({ id: jobId });

      await insertAttempt(jobId, { outcome: 'succeeded', finishedAt: new Date().toISOString() });
      await insertAttempt(jobId, { attemptNo: 2 });

      const rows = await testDb.db.execute(
        sql`SELECT attempt_no, outcome, finished_at IS NOT NULL AS finished FROM "outbox"."attempt" ORDER BY attempt_no`,
      );

      expect(rows.rows).toEqual([
        { attempt_no: 1, outcome: 'succeeded', finished: true },
        { attempt_no: 2, outcome: null, finished: false },
      ]);
    });
  });
});
