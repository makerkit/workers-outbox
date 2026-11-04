import { sql } from 'drizzle-orm';
import { v7 as uuidv7 } from 'uuid';

import type { Transition } from '../core/transition';
import type { SqlRunner } from '../drizzle/catalog';
import type { KindFilter, NewJob } from '../store/outbox-store';

import { queryRows } from '../drizzle/catalog';
import { claimDue, insertJob, settleJob } from '../store/outbox-store';

export const T0 = new Date('2026-01-01T00:00:00.000Z');

export const at = (offsetMs: number) => new Date(T0.getTime() + offsetMs);

export const EMAIL: KindFilter = { kind: 'email.welcome', leaseMs: 60_000, fits: true };
export const CRM: KindFilter = { kind: 'crm.sync', leaseMs: 120_000, fits: true };

export const newJob = (overrides: Partial<NewJob> = {}): NewJob => ({
  id: uuidv7(),
  kind: EMAIL.kind,
  dedupeKey: `key:${uuidv7()}`,
  payload: { userId: 'u1' },
  tenantKey: null,
  correlationId: null,
  metadata: {},
  delayMs: 0,
  ...overrides,
});

// Raw rows carry timestamps as strings (pglite) or Dates (node-pg).
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}/;

export const withDates = (row: Record<string, unknown>) =>
  Object.fromEntries(
    Object.entries(row).map(([key, value]) => [
      key,
      typeof value === 'string' && TIMESTAMP.test(value)
        ? new Date(value.replace(' ', 'T').replace(/([+-]\d{2})$/, '$1:00'))
        : value,
    ]),
  );

export const succeeded: Transition = { to: 'succeeded', attemptOutcome: 'succeeded' };

export const retrying = (delayMs: number): Extract<Transition, { to: 'retrying' }> => ({
  to: 'retrying',
  delayMs,
  code: 'http_503',
  message: 'unavailable',
  codeStreak: 1,
  attemptOutcome: 'retry',
});

export const dead = (code = 'http_503'): Transition => ({
  to: 'dead',
  reason: 'failed',
  code,
  message: null,
  codeStreak: 1,
  attemptOutcome: 'failed',
});

export function storeHelpers(db: () => SqlRunner) {
  const jobRow = async (id: string) => {
    const rows = await queryRows<Record<string, unknown>>(
      db(),
      sql`SELECT * FROM "outbox"."job" WHERE id = ${id}`,
    );

    return rows[0] && withDates(rows[0]);
  };

  const attempts = async (jobId: string) => {
    const rows = await queryRows<Record<string, unknown>>(
      db(),
      sql`SELECT * FROM "outbox"."attempt" WHERE job_id = ${jobId} ORDER BY attempt_no`,
    );

    return rows.map(withDates);
  };

  const enqueue = (overrides: Partial<NewJob> = {}, now: Date = T0) =>
    insertJob(db(), newJob(overrides), now);

  const claimOne = async (now: Date = T0, kinds = [EMAIL, CRM]) => {
    const [job] = await claimDue(db(), {
      kinds,
      limit: 1,
      trigger: 'sweep',
      leaseToken: uuidv7(),
      now,
    });

    return job;
  };

  const settleAs = async (
    transition: Transition,
    now: Date = T0,
    overrides: Partial<NewJob> = {},
  ) => {
    const job = await enqueue(overrides, now);
    const [claimed] = await claimDue(db(), {
      kinds: [EMAIL, CRM],
      limit: 1,
      trigger: 'sweep',
      leaseToken: uuidv7(),
      now,
    });

    if (claimed?.id !== job.id) {
      throw new Error('settleAs: expected to claim the job it just enqueued');
    }

    await settleJob(db(), { jobId: job.id, leaseToken: claimed.leaseToken, transition, now });

    return job.id;
  };

  return { jobRow, attempts, enqueue, claimOne, settleAs };
}
