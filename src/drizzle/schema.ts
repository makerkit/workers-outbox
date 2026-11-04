import { sql } from 'drizzle-orm';
import {
  check,
  foreignKey,
  index,
  bigint,
  integer,
  jsonb,
  pgSchema,
  primaryKey,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from 'drizzle-orm/pg-core';

import {
  ATTEMPT_OUTCOMES,
  ATTEMPT_TRIGGERS,
  CLAIMABLE_STATUSES,
  JOB_STATUSES,
  REASONED_TERMINAL_STATUSES,
  TERMINAL_STATUSES,
  type AttemptOutcome,
  type AttemptTrigger,
  type JobStatus,
} from '../core/status';
import { sqlLiteralList } from '../sql/sql-list';

/** Source of truth for the tables; the `/sql` DDL is pinned to it by a parity test. */
export const outboxSchema = pgSchema('outbox');

const timestamptz = (name: string) => timestamp(name, { precision: 3, withTimezone: true });

const list = (values: readonly string[]) => sql.raw(sqlLiteralList(values));

export const outboxJob = outboxSchema.table(
  'job',
  {
    // uuidv7 minted in TypeScript: Postgres < 18 and pglite have no uuidv7().
    id: uuid('id').primaryKey(),
    kind: text('kind').notNull(),
    // A repeat enqueue is a no-op until retention cleanup deletes the row.
    dedupeKey: text('dedupe_key').notNull(),
    payload: jsonb('payload').notNull(),
    status: text('status').$type<JobStatus>().notNull().default('pending'),
    tenantKey: text('tenant_key'),
    correlationId: text('correlation_id'),
    metadata: jsonb('metadata').$type<Record<string, unknown>>().notNull().default({}),
    // Never refreshed on retry; claims order by COALESCE(next_attempt_at, available_at).
    availableAt: timestamptz('available_at').notNull().defaultNow(),
    nextAttemptAt: timestamptz('next_attempt_at'),
    // The fence: every outcome write must present the token its claim minted.
    leaseToken: uuid('lease_token'),
    leasedUntil: timestamptz('leased_until'),
    // `attempt.attempt_no` = `started_attempts`; a gap to `finished_attempts` is an attempt in flight.
    startedAttempts: integer('started_attempts').notNull().default(0),
    finishedAttempts: integer('finished_attempts').notNull().default(0),
    spentAttempts: integer('spent_attempts').notNull().default(0),
    codeStreak: integer('code_streak').notNull().default(0),
    stickyCode: text('sticky_code'),
    // The enqueuing deploy's version (epoch ms): older deploys skip the job until the gate expires.
    minVersionMs: bigint('min_version_ms', { mode: 'number' }),
    lastErrorCode: text('last_error_code'),
    lastErrorMessage: text('last_error_message'),
    terminalReason: text('terminal_reason'),
    createdAt: timestamptz('created_at').notNull().defaultNow(),
    updatedAt: timestamptz('updated_at').notNull().defaultNow(),
    finishedAt: timestamptz('finished_at'),
    nudgeSentAt: timestamptz('nudge_sent_at'),
    nudgeAttempts: integer('nudge_attempts').notNull().default(0),
    lastNudgeError: text('last_nudge_error'),
    // Reserved for per-key ordered delivery (not enforced yet): shipped in 0001_init because adding
    // an identity column to a populated table later rewrites it under ACCESS EXCLUSIVE.
    orderingKey: text('ordering_key'),
    // Insert order, assigned by the database; ties within one transaction's now() are common.
    seq: bigint('seq', { mode: 'number' }).notNull().generatedAlwaysAsIdentity(),
  },
  (t) => [
    // Per tenant: one tenant can't claim (or probe) another tenant's keys. '' stands for no tenant.
    uniqueIndex('job_tenant_dedupe_key_unique').on(sql`COALESCE(${t.tenantKey}, '')`, t.dedupeKey),

    check('job_status_check', sql`${t.status} IN (${list(JOB_STATUSES)})`),
    check(
      'job_retrying_requires_next_attempt_at',
      sql`${t.status} <> 'retrying' OR ${t.nextAttemptAt} IS NOT NULL`,
    ),
    check(
      'job_processing_iff_leased',
      sql`(${t.status} = 'processing') = (${t.leaseToken} IS NOT NULL AND ${t.leasedUntil} IS NOT NULL)`,
    ),
    check(
      'job_attempt_counters',
      sql`${t.finishedAttempts} >= 0 AND ${t.finishedAttempts} <= ${t.startedAttempts} AND ${t.spentAttempts} >= 0 AND ${t.spentAttempts} <= ${t.finishedAttempts} AND ${t.codeStreak} >= 0`,
    ),
    check(
      'job_terminal_iff_finished_at',
      sql`(${t.status} IN (${list(TERMINAL_STATUSES)})) = (${t.finishedAt} IS NOT NULL)`,
    ),
    check(
      'job_terminal_reason',
      sql`(${t.status} IN (${list(REASONED_TERMINAL_STATUSES)})) = (${t.terminalReason} IS NOT NULL)`,
    ),

    // Keyed on the due instant, not led by status: the planner never picks a status-led index here.
    index('job_claim_due_idx')
      .on(sql`COALESCE(${t.nextAttemptAt}, ${t.availableAt})`, t.createdAt, t.id)
      .where(sql`${t.status} IN (${list(CLAIMABLE_STATUSES)})`),
    index('job_pending_kind_idx')
      .on(t.kind, t.availableAt, t.createdAt)
      .where(sql`${t.status} = 'pending'`),
    index('job_retrying_kind_idx')
      .on(t.kind, t.nextAttemptAt, t.createdAt)
      .where(sql`${t.status} = 'retrying'`),
    index('job_lease_expiry_idx')
      .on(t.leasedUntil)
      .where(sql`${t.status} = 'processing'`),
    index('job_retention_idx')
      .on(t.status, t.finishedAt)
      .where(sql`${t.status} IN (${list(TERMINAL_STATUSES)})`),
    index('job_tenant_key_idx')
      .on(t.tenantKey)
      .where(sql`${t.tenantKey} IS NOT NULL`),
    index('job_correlation_id_idx')
      .on(t.correlationId)
      .where(sql`${t.correlationId} IS NOT NULL`),
    // Full, not partial: inspection lists every status, terminal ones included.
    index('job_status_created_idx').on(t.status, t.createdAt, t.id),
  ],
);

export const outboxAttempt = outboxSchema.table(
  'attempt',
  {
    jobId: uuid('job_id').notNull(),
    attemptNo: integer('attempt_no').notNull(),
    // Denormalized from the job at claim: per-kind rate windows count attempts without a join.
    kind: text('kind').notNull(),
    leaseToken: uuid('lease_token').notNull(),
    trigger: text('trigger').$type<AttemptTrigger>().notNull(),
    startedAt: timestamptz('started_at').notNull(),
    finishedAt: timestamptz('finished_at'),
    outcome: text('outcome').$type<AttemptOutcome>(),
    code: text('code'),
    message: text('message'),
    durationMs: integer('duration_ms'),
  },
  (t) => [
    primaryKey({ name: 'attempt_pkey', columns: [t.jobId, t.attemptNo] }),
    foreignKey({
      name: 'attempt_job_id_fkey',
      columns: [t.jobId],
      foreignColumns: [outboxJob.id],
    }).onDelete('cascade'),
    check('attempt_trigger_check', sql`${t.trigger} IN (${list(ATTEMPT_TRIGGERS)})`),
    check(
      'attempt_outcome_check',
      sql`${t.outcome} IS NULL OR ${t.outcome} IN (${list(ATTEMPT_OUTCOMES)})`,
    ),
    check('attempt_finished_iff_outcome', sql`(${t.outcome} IS NULL) = (${t.finishedAt} IS NULL)`),
    index('attempt_finished_at_idx')
      .on(t.finishedAt)
      .where(sql`${t.finishedAt} IS NOT NULL`),
  ],
);

export type OutboxJobRow = typeof outboxJob.$inferSelect;
export type OutboxAttemptRow = typeof outboxAttempt.$inferSelect;
