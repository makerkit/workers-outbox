import { sql } from 'drizzle-orm';
import { v7 as uuidv7 } from 'uuid';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { queryRows } from '../drizzle/catalog';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import {
  claimDue,
  deleteTenantJobs,
  findJobsByCorrelationId,
  getJob,
  insertJob,
  rescheduleNow,
  settleJob,
  type KindFilter,
  type NewJob,
} from './outbox-store';

const T0 = new Date('2026-01-01T00:00:00.000Z');
const EMAIL: KindFilter = { kind: 'email.welcome', leaseMs: 60_000, fits: true };

const newJob = (overrides: Partial<NewJob> = {}): NewJob => ({
  id: uuidv7(),
  kind: EMAIL.kind,
  dedupeKey: `key:${uuidv7()}`,
  payload: { n: 1 },
  tenantKey: null,
  correlationId: null,
  metadata: {},
  delayMs: 0,
  ...overrides,
});

describe.each(testDrivers())('operator store functions on $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  const failOnce = async (id: string) => {
    const [claimed] = await claimDue(driver.db, {
      kinds: [EMAIL],
      limit: 1,
      trigger: 'sweep',
      leaseToken: uuidv7(),
      now: T0,
    });
    await settleJob(driver.db, {
      jobId: id,
      leaseToken: claimed!.leaseToken,
      transition: {
        to: 'retrying',
        delayMs: 3_600_000,
        code: 'busy',
        message: null,
        codeStreak: 1,
        attemptOutcome: 'retry',
      },
      now: T0,
    });
  };

  it('getJob returns the job with its attempts, scoped to a tenant', async () => {
    const job = newJob({ tenantKey: 'org-a' });
    await insertJob(driver.db, job, T0);
    await failOnce(job.id);

    expect(await getJob(driver.db, { id: job.id, tenantKey: 'org-a' })).toMatchObject({
      id: job.id,
      kind: EMAIL.kind,
      status: 'retrying',
      tenantKey: 'org-a',
      lastErrorCode: 'busy',
      attempts: [expect.objectContaining({ attemptNo: 1, outcome: 'retry', code: 'busy' })],
    });
    expect(await getJob(driver.db, { id: job.id, tenantKey: 'org-b' })).toBeNull();
    expect(await getJob(driver.db, { id: 'not-a-uuid' })).toBeNull();
  });

  it('findJobsByCorrelationId lists every job of one request, newest first', async () => {
    const a = newJob({ correlationId: 'req-1' });
    const b = newJob({ correlationId: 'req-1' });
    await insertJob(driver.db, a, T0);
    await insertJob(driver.db, b, new Date(T0.getTime() + 1_000));
    await insertJob(driver.db, newJob({ correlationId: 'req-2' }), T0);

    expect(
      (await findJobsByCorrelationId(driver.db, { correlationId: 'req-1', limit: 10 })).map(
        (j) => j.id,
      ),
    ).toEqual([b.id, a.id]);
  });

  it('rescheduleNow makes a waiting job due immediately and refuses others', async () => {
    const job = newJob();
    await insertJob(driver.db, job, T0);
    await failOnce(job.id);

    expect(await rescheduleNow(driver.db, { id: job.id, now: new Date(T0.getTime() + 5) })).toEqual(
      {
        result: 'rescheduled',
        kind: job.kind,
      },
    );
    const [row] = await queryRows<{ next_attempt_at: unknown }>(
      driver.db,
      sql`SELECT next_attempt_at FROM "outbox"."job" WHERE id = ${job.id}`,
    );
    expect(
      new Date(
        String(row?.next_attempt_at)
          .replace(' ', 'T')
          .replace(/([+-]\d{2})$/, '$1:00'),
      ).getTime(),
    ).toBe(T0.getTime() + 5);
    expect(await rescheduleNow(driver.db, { id: uuidv7(), now: T0 })).toEqual({
      result: 'not_found',
      kind: null,
    });
  });

  it('deleteTenantJobs erases every job of one tenant, attempts included, in batches', async () => {
    for (let i = 0; i < 3; i++) {
      await insertJob(driver.db, newJob({ tenantKey: 'org-a' }), T0);
    }
    const keep = newJob({ tenantKey: 'org-b' });
    await insertJob(driver.db, keep, T0);

    expect(await deleteTenantJobs(driver.db, { tenantKey: 'org-a', batch: 2 })).toBe(2);
    expect(await deleteTenantJobs(driver.db, { tenantKey: 'org-a', batch: 2 })).toBe(1);
    expect(await deleteTenantJobs(driver.db, { tenantKey: 'org-a', batch: 2 })).toBe(0);
    expect(await getJob(driver.db, { id: keep.id })).not.toBeNull();
  });
});
