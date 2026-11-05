import { sql, type SQL } from 'drizzle-orm';

import type { KindBacklog } from '../core/backlog';
import type { ClaimableKind } from '../core/registry';
import type { AttemptOutcome, AttemptTrigger, JobStatus } from '../core/status';
import type { Transition } from '../core/transition';

import { TERMINAL_STATUSES } from '../core/status';
import { isUuid } from '../core/uuid';
import { queryRows, type SqlRunner } from '../drizzle/catalog';

// Every statement is one autocommit-safe statement (Hyperdrive shares a session per connection),
// runs on the DB clock (`now` null -> now()), and embeds now() so Hyperdrive never caches it.
// Lists bind as one typed array literal: Drizzle's tuple expansion breaks `= ANY(...)`.

export interface ClaimedJob {
  id: string;
  kind: string;
  payload: unknown;
  tenantKey: string | null;
  correlationId: string | null;
  metadata: Record<string, unknown>;
  leaseToken: string;
  attemptNo: number;
  spentAttempts: number;
  codeStreak: number;
  lastErrorCode: string | null;
  /** Always set by the store; optional only for hand-built values. */
  stickyCode?: string | null;
}

export type ClaimByIdResult =
  | { id: string; result: 'claimed'; job: ClaimedJob }
  | {
      id: string;
      result:
        | 'not_found'
        | 'unregistered_kind'
        | 'over_budget'
        | 'newer_version'
        | 'terminal'
        | 'leased'
        | 'not_due'
        | 'locked';
    };

export interface NewJob {
  id: string;
  kind: string;
  dedupeKey: string;
  payload: unknown;
  tenantKey: string | null;
  correlationId: string | null;
  metadata: Record<string, unknown>;
  delayMs: number;
  /** The enqueuing deploy's version (epoch ms); `null`/absent = not gated. */
  minVersionMs?: number | null;
}

/**
 * A worker's own deploy version: it skips jobs a newer deploy enqueued (their payload may
 * be a shape it can't read) until `gateMs` after enqueue, so a rollback never strands them.
 */
export interface VersionGate {
  mineMs: number;
  gateMs: number;
}

const versionClause = (alias: SQL, gate: VersionGate | undefined) =>
  gate === undefined
    ? sql``
    : sql` AND (${alias}.min_version_ms IS NULL OR ${alias}.min_version_ms <= ${gate.mineMs} OR ${alias}.created_at <= (SELECT t FROM clock) - ${ms(gate.gateMs)})`;

export interface KindFilter extends ClaimableKind {
  /** The kind's lease fits the remaining time budget. */
  readonly fits: boolean;
}

/** A claim whose lease ran out: recovery settles it without reading the payload. */
export type ExpiredLease = Omit<ClaimedJob, 'payload'>;

/** `undefined` = every tenant, `null` = untenanted only; out-of-scope jobs read as `not_found`. */
export type TenantScope = string | null | undefined;

export const DEFAULT_SUCCEEDED_RETENTION_MS = 7 * 86_400_000;
export const DEFAULT_TERMINAL_RETENTION_MS = 90 * 86_400_000;

const MAX_MESSAGE_LENGTH = 2_000;

const MAX_NUDGE_ERROR_LENGTH = 500;

const isTerminal = (status: JobStatus) =>
  (TERMINAL_STATUSES as readonly JobStatus[]).includes(status);

const dbNow = (now: Date | null) =>
  sql`COALESCE(${now?.toISOString() ?? null}::timestamptz, now())`;

const ms = (value: number) => sql`(${Math.round(value)}::bigint * interval '1 millisecond')`;

const jsonb = (value: unknown) => sql`${JSON.stringify(value)}::jsonb`;

const arrayLiteral = (values: readonly string[]) =>
  `{${values.map((value) => `"${value.replaceAll('\\', '\\\\').replaceAll('"', '\\"')}"`).join(',')}}`;

const textArray = (values: readonly string[]) => sql`${arrayLiteral(values)}::text[]`;

const uuidArray = (values: readonly string[]) => sql`${arrayLiteral(values)}::uuid[]`;

const tenantClause = (column: SQL, tenantKey: TenantScope) => {
  if (tenantKey === undefined) {
    return sql``;
  }

  return tenantKey === null ? sql` AND ${column} IS NULL` : sql` AND ${column} = ${tenantKey}`;
};

/** float8 epoch ms: every driver returns it as a number. */
const epochMs = (column: SQL) => sql`(EXTRACT(EPOCH FROM ${column}) * 1000)::float8`;

const toDate = (value: number | null) => (value === null ? null : new Date(value));

const kindsTable = (kinds: readonly KindFilter[]) =>
  sql`jsonb_to_recordset(${jsonb(kinds.map((k) => ({ kind: k.kind, lease_ms: k.leaseMs, fits: k.fits })))}) AS k(kind text, lease_ms bigint, fits boolean)`;

const isDue = (alias: SQL) =>
  sql`((${alias}.status = 'pending' AND ${alias}.available_at <= (SELECT t FROM clock)) OR (${alias}.status = 'retrying' AND ${alias}.next_attempt_at <= (SELECT t FROM clock)))`;

// A dedupe-race loser inserts nothing; the follow-up SELECT (fresh READ COMMITTED snapshot) sees
// the winner. Under REPEATABLE READ+ Postgres raises 40001 for the caller's retry instead.
export async function insertJob(tx: SqlRunner, job: NewJob, now: Date | null) {
  const inserted = await queryRows<{ id: string; kind: string; tenant_key: string | null }>(
    tx,
    sql`INSERT INTO "outbox"."job" (id, kind, dedupe_key, payload, tenant_key, correlation_id, metadata, min_version_ms, available_at, created_at, updated_at)
        VALUES (${job.id}, ${job.kind}, ${job.dedupeKey}, ${jsonb(job.payload)}, ${job.tenantKey}, ${job.correlationId},
                ${jsonb(job.metadata)}, ${job.minVersionMs ?? null}::bigint, ${dbNow(now)} + ${ms(job.delayMs)}, ${dbNow(now)}, ${dbNow(now)})
        ON CONFLICT ((COALESCE(tenant_key, '')), dedupe_key) DO NOTHING
        RETURNING id, kind, tenant_key`,
  );

  const created = inserted[0];

  if (created) {
    return { id: created.id, kind: created.kind, tenantKey: created.tenant_key, created: true };
  }

  const existing = await queryRows<{
    id: string;
    kind: string;
    tenant_key: string | null;
    read_at: unknown;
  }>(
    tx,
    sql`SELECT id, kind, tenant_key, now() AS read_at FROM "outbox"."job" WHERE dedupe_key = ${job.dedupeKey} AND tenant_key IS NOT DISTINCT FROM ${job.tenantKey}`,
  );

  const row = existing[0];

  if (!row) {
    throw new Error(
      `insertJob: dedupe key ${JSON.stringify(job.dedupeKey)} conflicted but no row holds it`,
    );
  }

  return { id: row.id, kind: row.kind, tenantKey: row.tenant_key, created: false };
}

export async function claimDue(
  db: SqlRunner,
  input: {
    kinds: readonly KindFilter[];
    limit: number;
    trigger: Exclude<AttemptTrigger, 'nudge'>;
    leaseToken: string;
    now: Date | null;
    tenantKey?: TenantScope;
    version?: VersionGate;
  },
) {
  const claimable = input.kinds.filter((k) => k.fits);

  // An empty filter never widens to a global drain.
  if (claimable.length === 0 || input.limit < 1) {
    return [];
  }

  const rows = await queryRows<ClaimedRow>(
    db,
    sql`WITH clock AS (SELECT ${dbNow(input.now)} AS t),
        k AS (SELECT * FROM ${kindsTable(claimable)}),
        picked AS (
          SELECT j.id FROM "outbox"."job" j
          WHERE j.kind = ANY(${textArray(claimable.map((k) => k.kind))}) AND ${isDue(sql`j`)}${tenantClause(sql`j.tenant_key`, input.tenantKey)}${versionClause(sql`j`, input.version)}
          ORDER BY COALESCE(j.next_attempt_at, j.available_at), j.created_at, j.id
          LIMIT ${input.limit}
          FOR UPDATE SKIP LOCKED
        ),
        ${claimAndOpenAttempt(input.leaseToken, input.trigger)}
        SELECT * FROM claimed`,
  );

  return rows.map(toClaimedJob);
}

/** Never claims a substitute; each id's diagnosis reads the pre-claim snapshot. */
export async function claimByIds(
  db: SqlRunner,
  input: {
    ids: readonly string[];
    kinds: readonly KindFilter[];
    leaseToken: string;
    now: Date | null;
    tenantKey?: TenantScope;
    version?: VersionGate;
  },
): Promise<ClaimByIdResult[]> {
  const ids = [...new Set(input.ids)];
  const valid = ids.filter((id) => isUuid(id));
  const invalid = ids
    .filter((id) => !isUuid(id))
    .map((id) => ({ id, result: 'not_found' as const }));

  if (valid.length === 0 || input.kinds.length === 0) {
    return [...invalid, ...valid.map((id) => ({ id, result: 'not_found' as const }))];
  }

  const rows = await queryRows<
    ClaimedRow & {
      req_id: string;
      result: Exclude<ClaimByIdResult['result'], 'claimed'> | 'claimed';
    }
  >(
    db,
    sql`WITH clock AS (SELECT ${dbNow(input.now)} AS t),
        k AS (SELECT * FROM ${kindsTable(input.kinds)}),
        req AS (SELECT DISTINCT unnest(${uuidArray(valid)}) AS id),
        picked AS (
          SELECT j.id FROM "outbox"."job" j
          JOIN req ON req.id = j.id
          WHERE j.kind = ANY(${textArray(input.kinds.filter((k) => k.fits).map((k) => k.kind))}) AND ${isDue(sql`j`)}${tenantClause(sql`j.tenant_key`, input.tenantKey)}${versionClause(sql`j`, input.version)}
          FOR UPDATE OF j SKIP LOCKED
        ),
        ${claimAndOpenAttempt(input.leaseToken, 'nudge')}
        SELECT req.id::text AS req_id,
          CASE
            WHEN c.id IS NOT NULL THEN 'claimed'
            WHEN j.id IS NULL THEN 'not_found'
            WHEN NOT EXISTS (SELECT 1 FROM k WHERE k.kind = j.kind) THEN 'unregistered_kind'
            WHEN j.status IN ('succeeded', 'dead', 'cancelled') THEN 'terminal'
            WHEN j.status = 'processing' THEN 'leased'
            WHEN NOT ${isDue(sql`j`)} THEN 'not_due'
            WHEN ${input.version === undefined ? sql`false` : sql`NOT (TRUE${versionClause(sql`j`, input.version)})`} THEN 'newer_version'
            WHEN NOT (SELECT fits FROM k WHERE k.kind = j.kind) THEN 'over_budget'
            ELSE 'locked'
          END AS result,
          c.*
        FROM req
        LEFT JOIN claimed c ON c.id = req.id
        -- LATERAL forces a primary-key probe per id; a plain join may seq-scan the table.
        LEFT JOIN LATERAL (SELECT * FROM "outbox"."job" WHERE id = req.id${tenantClause(sql`tenant_key`, input.tenantKey)}) j ON true`,
  );

  return [
    ...invalid,
    ...rows.map((row): ClaimByIdResult =>
      row.result === 'claimed'
        ? { id: row.req_id, result: 'claimed', job: toClaimedJob(row) }
        : { id: row.req_id, result: row.result },
    ),
  ];
}

/** The lease token fences stale workers: they get `lease_lost` and write nothing. */
export async function settleJob(
  db: SqlRunner,
  input: {
    jobId: string;
    leaseToken: string;
    transition: Transition;
    now: Date | null;
    requireExpiredLease?: boolean;
  },
) {
  const t = input.transition;
  const failure = 'code' in t ? t : null;
  const code = failure?.code ?? null;
  const message = failure ? truncate(failure.message) : null;
  const codeStreak = failure?.codeStreak ?? 0;
  // Absent = keep the row's episode: only replay ends one.
  const stickyCode = failure?.stickyCode ?? null;
  const reason = t.to === 'dead' || t.to === 'cancelled' ? t.reason : null;
  const delayMs = t.to === 'retrying' ? t.delayMs : 0;
  const finishes = t.to !== 'retrying';
  const expiredClause = input.requireExpiredLease
    ? sql` AND leased_until < (SELECT t FROM clock)`
    : sql``;

  const rows = await queryRows<{ applied: number }>(
    db,
    sql`WITH clock AS (SELECT ${dbNow(input.now)} AS t),
        settled AS (
          UPDATE "outbox"."job" SET
            status = ${t.to},
            lease_token = NULL,
            leased_until = NULL,
            finished_attempts = finished_attempts + 1,
            spent_attempts = spent_attempts + ${t.attemptOutcome === 'lease_expired' ? 0 : 1},
            next_attempt_at = ${t.to === 'retrying' ? sql`(SELECT t FROM clock) + ${ms(delayMs)}` : sql`NULL`},
            code_streak = ${codeStreak},
            sticky_code = COALESCE(${stickyCode}, sticky_code),
            last_error_code = COALESCE(${code}, last_error_code),
            last_error_message = ${code === null ? sql`last_error_message` : sql`${message}`},
            terminal_reason = ${reason},
            finished_at = ${finishes ? sql`(SELECT t FROM clock)` : sql`NULL`},
            updated_at = (SELECT t FROM clock)
          WHERE id = ${input.jobId} AND lease_token = ${input.leaseToken} AND status = 'processing'${expiredClause}
          RETURNING id, started_attempts
        ),
        closed AS (
          UPDATE "outbox"."attempt" a SET
            finished_at = (SELECT t FROM clock),
            outcome = ${t.attemptOutcome},
            code = ${code},
            message = ${message},
            duration_ms = LEAST(2147483647, GREATEST(0, EXTRACT(EPOCH FROM ((SELECT t FROM clock) - a.started_at)) * 1000))::int
          FROM settled
          WHERE a.job_id = settled.id AND a.attempt_no = settled.started_attempts AND a.lease_token = ${input.leaseToken}
          RETURNING 1
        )
        SELECT (SELECT count(*) FROM settled)::int AS applied`,
  );

  return firstRow(rows, 'settleJob').applied === 1 ? ('applied' as const) : ('lease_lost' as const);
}

/** Lock-free pre-check before a settle hook; `settleJob` stays the authoritative fence. */
export async function isLeaseHeld(
  db: SqlRunner,
  input: { jobId: string; leaseToken: string; now: Date | null; requireExpiredLease?: boolean },
) {
  if (!isUuid(input.jobId) || !isUuid(input.leaseToken)) {
    return false;
  }

  const expiredClause = input.requireExpiredLease
    ? sql` AND leased_until < ${dbNow(input.now)}`
    : sql``;

  const rows = await queryRows<{ held: boolean }>(
    db,
    sql`SELECT EXISTS (
          SELECT 1 FROM "outbox"."job"
          WHERE id = ${input.jobId} AND lease_token = ${input.leaseToken} AND status = 'processing'${expiredClause}
        ) AS held, now() AS read_at`,
  );

  return firstRow(rows, 'isLeaseHeld').held;
}

/**
 * Native bounds for the rest of the current transaction (PostgreSQL 17+), in one round trip:
 * the whole transaction (FATAL 25P04: the session ends), each statement (57014), each lock wait (55P03).
 */
export async function setSettlementTimeouts(
  tx: SqlRunner,
  input: { transactionMs: number; statementMs: number; lockMs: number },
) {
  const toMs = (value: number) => `'${Math.max(1, Math.ceil(value))}ms'`;

  await tx.execute(
    sql.raw(
      `SELECT set_config('transaction_timeout', ${toMs(input.transactionMs)}, true), set_config('statement_timeout', ${toMs(input.statementMs)}, true), set_config('lock_timeout', ${toMs(input.lockMs)}, true), now() AS read_at`,
    ),
  );
}

/**
 * How the attempt opened under this lease ended, if it did: resolves a settle whose
 * connection dropped after the write may have committed.
 */
export async function readAttemptOutcome(
  db: SqlRunner,
  input: { jobId: string; leaseToken: string },
) {
  const rows = await queryRows<{ outcome: AttemptOutcome | null; read_at: unknown }>(
    db,
    sql`SELECT outcome, now() AS read_at FROM "outbox"."attempt"
        WHERE job_id = ${input.jobId} AND lease_token = ${input.leaseToken}`,
  );

  return rows[0]?.outcome ?? null;
}

export async function listExpiredLeases(
  db: SqlRunner,
  input: {
    limit: number;
    now: Date | null;
    kinds?: readonly string[];
  },
) {
  if (input.kinds?.length === 0 || input.limit < 1) {
    return [];
  }

  const kindClause = input.kinds ? sql` AND kind = ANY(${textArray(input.kinds)})` : sql``;

  const rows = await queryRows<{
    id: string;
    kind: string;
    lease_token: string;
    spent_attempts: number;
    code_streak: number;
    last_error_code: string | null;
    sticky_code: string | null;
    tenant_key: string | null;
    correlation_id: string | null;
    metadata: Record<string, unknown>;
    started_attempts: number;
  }>(
    db,
    sql`SELECT id, kind, lease_token, spent_attempts, code_streak, last_error_code, sticky_code,
               tenant_key, correlation_id, metadata, started_attempts
        FROM "outbox"."job"
        WHERE status = 'processing' AND leased_until < ${dbNow(input.now)}${kindClause}
        ORDER BY leased_until
        LIMIT ${input.limit}`,
  );

  return rows.map((row): ExpiredLease => ({
    id: row.id,
    kind: row.kind,
    leaseToken: row.lease_token,
    spentAttempts: row.spent_attempts,
    codeStreak: row.code_streak,
    lastErrorCode: row.last_error_code,
    stickyCode: row.sticky_code,
    tenantKey: row.tenant_key,
    correlationId: row.correlation_id,
    metadata: row.metadata,
    attemptNo: row.started_attempts,
  }));
}

/** A job mid-attempt is `in_flight`: its worker owns it until it settles. */
export async function cancelJob(
  db: SqlRunner,
  input: {
    /** A dedupe key is unique per tenant, so it needs its tenant (`null` = none). */
    ref: { id: string } | { dedupeKey: string; tenantKey: string | null };
    reason: string;
    now: Date | null;
    tenantKey?: TenantScope;
  },
) {
  if ('id' in input.ref && !isUuid(input.ref.id)) {
    return 'not_found' as const;
  }

  const match =
    'id' in input.ref
      ? sql`id = ${input.ref.id}`
      : sql`dedupe_key = ${input.ref.dedupeKey} AND tenant_key IS NOT DISTINCT FROM ${input.ref.tenantKey}`;

  const [row] = await updateMatching(db, {
    match,
    tenantKey: input.tenantKey,
    now: input.now,
    ...cancellation(input.reason),
  });

  return row ? cancelResult(row) : ('not_found' as const);
}

/** Dead or cancelled → pending, with a fresh retry budget. */
export async function replayJob(
  db: SqlRunner,
  input: { id: string; now: Date | null; tenantKey?: TenantScope },
) {
  if (!isUuid(input.id)) {
    return 'not_found' as const;
  }

  const [row] = await updateMatching(db, {
    match: sql`id = ${input.id}`,
    tenantKey: input.tenantKey,
    now: input.now,
    ...REPLAY,
  });

  return row ? replayResult(row) : ('not_found' as const);
}

export async function deleteFinishedJobs(
  db: SqlRunner,
  input: {
    succeededRetentionMs: number;
    terminalRetentionMs: number;
    batch: number;
    now: Date | null;
  },
) {
  const expired = sql`((status = 'succeeded' AND finished_at < (SELECT t FROM clock) - ${ms(input.succeededRetentionMs)})
             OR (status IN ('dead', 'cancelled') AND finished_at < (SELECT t FROM clock) - ${ms(input.terminalRetentionMs)}))`;
  const rows = await queryRows<{ deleted: number }>(
    db,
    // The predicate is repeated on the DELETE: under READ COMMITTED a row replayed while the
    // DELETE waited on its lock is re-checked against these columns, not just its id.
    sql`WITH clock AS (SELECT ${dbNow(input.now)} AS t),
        doomed AS (
          SELECT id FROM "outbox"."job"
          WHERE ${expired}
          LIMIT ${input.batch}
        ),
        deleted AS (DELETE FROM "outbox"."job" WHERE id IN (SELECT id FROM doomed) AND ${expired} RETURNING 1)
        SELECT count(*)::int AS deleted FROM deleted`,
  );

  return firstRow(rows, 'deleteFinishedJobs').deleted;
}

export async function hasDueJobs(
  db: SqlRunner,
  input: { kinds: readonly KindFilter[]; now: Date | null; version?: VersionGate },
) {
  const claimable = input.kinds.filter((k) => k.fits);

  if (claimable.length === 0) {
    return false;
  }

  const rows = await queryRows<{ due: boolean }>(
    db,
    sql`WITH clock AS (SELECT ${dbNow(input.now)} AS t)
        SELECT EXISTS (
          SELECT 1 FROM "outbox"."job" j WHERE j.kind = ANY(${textArray(claimable.map((k) => k.kind))}) AND ${isDue(sql`j`)}${versionClause(sql`j`, input.version)}
        ) AS due`,
  );

  return firstRow(rows, 'hasDueJobs').due;
}

export async function listDueJobIds(
  db: SqlRunner,
  input: {
    kinds: readonly string[];
    limit: number;
    now: Date | null;
    // Jobs nudged more recently than this are skipped: their nudge may still be queued.
    nudgeCooldownMs: number;
    tenantKey?: TenantScope;
    version?: VersionGate;
  },
) {
  if (input.kinds.length === 0 || input.limit < 1) {
    return [];
  }

  const rows = await queryRows<{ id: string }>(
    db,
    sql`WITH clock AS (SELECT ${dbNow(input.now)} AS t)
        SELECT j.id FROM "outbox"."job" j
        WHERE j.kind = ANY(${textArray(input.kinds)}) AND ${isDue(sql`j`)}
          AND (j.nudge_sent_at IS NULL OR j.nudge_sent_at <= (SELECT t FROM clock) - ${ms(input.nudgeCooldownMs)})${tenantClause(sql`j.tenant_key`, input.tenantKey)}${versionClause(sql`j`, input.version)}
        ORDER BY COALESCE(j.next_attempt_at, j.available_at), j.created_at, j.id
        LIMIT ${input.limit}`,
  );

  return rows.map((row) => row.id);
}

// "Stuck" reuses recovery's predicate so the monitor and `listExpiredLeases` always agree.
export async function countBacklog(
  db: SqlRunner,
  input: { agedMs: number; now: Date | null; kinds?: readonly string[]; tenantKey?: TenantScope },
): Promise<KindBacklog[]> {
  if (input.kinds?.length === 0) {
    return [];
  }

  const kindClause = input.kinds ? sql` AND j.kind = ANY(${textArray(input.kinds)})` : sql``;

  const rows = await queryRows<{
    kind: string;
    due: number;
    aged: number;
    scheduled: number;
    processing: number;
    stuck: number;
    oldest_due_age_ms: number | null;
  }>(
    db,
    sql`WITH clock AS (SELECT ${dbNow(input.now)} AS t),
        active AS (
          SELECT j.kind, j.status, j.leased_until,
                 CASE j.status WHEN 'pending' THEN j.available_at WHEN 'retrying' THEN j.next_attempt_at END AS due_at
          FROM "outbox"."job" j
          WHERE j.status IN ('pending', 'retrying', 'processing')${kindClause}${tenantClause(sql`j.tenant_key`, input.tenantKey)}
        )
        SELECT kind,
          count(*) FILTER (WHERE due_at <= (SELECT t FROM clock))::int AS due,
          count(*) FILTER (WHERE due_at < (SELECT t FROM clock) - ${ms(input.agedMs)})::int AS aged,
          count(*) FILTER (WHERE due_at > (SELECT t FROM clock))::int AS scheduled,
          count(*) FILTER (WHERE status = 'processing')::int AS processing,
          count(*) FILTER (WHERE status = 'processing' AND leased_until < (SELECT t FROM clock))::int AS stuck,
          (max(EXTRACT(EPOCH FROM ((SELECT t FROM clock) - due_at))) FILTER (WHERE due_at <= (SELECT t FROM clock)) * 1000)::float8 AS oldest_due_age_ms
        FROM active
        GROUP BY kind
        ORDER BY kind`,
  );

  return rows.map((row) => ({
    kind: row.kind,
    due: row.due,
    aged: row.aged,
    scheduled: row.scheduled,
    processing: row.processing,
    stuck: row.stuck,
    oldestDueAgeMs: row.oldest_due_age_ms === null ? null : Math.round(row.oldest_due_age_ms),
  }));
}

export interface JobSummary {
  id: string;
  kind: string;
  status: JobStatus;
  dedupeKey: string;
  tenantKey: string | null;
  correlationId: string | null;
  metadata: Record<string, unknown>;
  startedAttempts: number;
  finishedAttempts: number;
  spentAttempts: number;
  codeStreak: number;
  stickyCode: string | null;
  lastErrorCode: string | null;
  lastErrorMessage: string | null;
  terminalReason: string | null;
  availableAt: Date;
  nextAttemptAt: Date | null;
  leasedUntil: Date | null;
  createdAt: Date;
  updatedAt: Date;
  finishedAt: Date | null;
  nudgeSentAt: Date | null;
  nudgeAttempts: number;
  lastNudgeError: string | null;
}

export interface JobFilter {
  status: JobStatus;
  kind?: string;
  tenantKey?: TenantScope;
  dedupeKey?: string;
  terminalReason?: string;
  createdAfter?: Date;
  createdBefore?: Date;
  finishedAfter?: Date;
  finishedBefore?: Date;
}

const optional = <T>(value: T | undefined, clause: (value: T) => SQL) =>
  value === undefined ? sql`` : clause(value);

const jobFilter = (filter: JobFilter) =>
  sql`j.status = ${filter.status}${optional(filter.kind, (v) => sql` AND j.kind = ${v}`)}${tenantClause(sql`j.tenant_key`, filter.tenantKey)}${optional(filter.dedupeKey, (v) => sql` AND j.dedupe_key = ${v}`)}${optional(filter.terminalReason, (v) => sql` AND j.terminal_reason = ${v}`)}${optional(filter.createdAfter, (v) => sql` AND j.created_at > ${v.toISOString()}::timestamptz`)}${optional(filter.createdBefore, (v) => sql` AND j.created_at < ${v.toISOString()}::timestamptz`)}${optional(filter.finishedAfter, (v) => sql` AND j.finished_at > ${v.toISOString()}::timestamptz`)}${optional(filter.finishedBefore, (v) => sql` AND j.finished_at < ${v.toISOString()}::timestamptz`)}`;

const jobSummaryColumns = sql`j.id, j.kind, j.status, j.dedupe_key, j.tenant_key, j.correlation_id, j.metadata,
  j.started_attempts, j.finished_attempts, j.spent_attempts, j.code_streak, j.sticky_code,
  j.last_error_code, j.last_error_message, j.terminal_reason, j.nudge_attempts, j.last_nudge_error,
  ${epochMs(sql`j.available_at`)} AS available_at, ${epochMs(sql`j.next_attempt_at`)} AS next_attempt_at,
  ${epochMs(sql`j.leased_until`)} AS leased_until, ${epochMs(sql`j.created_at`)} AS created_at,
  ${epochMs(sql`j.updated_at`)} AS updated_at, ${epochMs(sql`j.finished_at`)} AS finished_at,
  ${epochMs(sql`j.nudge_sent_at`)} AS nudge_sent_at, now() AS read_at`;

/** Keyset-paginated: pass `nextCursor` back as `cursor`. */
export async function listJobs(
  db: SqlRunner,
  input: JobFilter & { limit: number; cursor?: string | null },
) {
  const after = input.cursor ? parseCursor(input.cursor) : null;
  const afterClause = after
    ? sql` AND (j.created_at, j.id) > (${after.createdAt}::timestamptz, ${after.id}::uuid)`
    : sql``;

  const rows = await queryRows<JobSummaryRow>(
    db,
    sql`SELECT ${jobSummaryColumns}
        FROM "outbox"."job" j
        WHERE ${jobFilter(input)}${afterClause}
        ORDER BY j.created_at, j.id
        LIMIT ${Math.max(0, input.limit) + 1}`,
  );

  const page = rows.slice(0, Math.max(0, input.limit)).map(toJobSummary);
  const last = page.at(-1);

  return {
    jobs: page,
    nextCursor:
      rows.length > page.length && last ? `${last.createdAt.toISOString()}|${last.id}` : null,
  };
}

export async function countJobs(db: SqlRunner, input: JobFilter) {
  const rows = await queryRows<{ count: number }>(
    db,
    sql`SELECT count(*)::int AS count, now() AS read_at FROM "outbox"."job" j WHERE ${jobFilter(input)}`,
  );

  return firstRow(rows, 'countJobs').count;
}

export interface AttemptOutcomeGroup {
  kind: string;
  tenantKey: string | null;
  outcome: AttemptOutcome;
  code: string | null;
  count: number;
}

export async function aggregateAttemptOutcomes(
  db: SqlRunner,
  input: {
    windowMs: number;
    maxRows: number;
    now: Date | null;
    kinds?: readonly string[];
    tenantKey?: TenantScope;
    outcomes?: readonly AttemptOutcome[];
    code?: string;
  },
) {
  const filters = sql.join(
    [
      input.kinds ? sql` AND j.kind = ANY(${textArray(input.kinds)})` : sql``,
      tenantClause(sql`j.tenant_key`, input.tenantKey),
      input.outcomes ? sql` AND a.outcome = ANY(${textArray(input.outcomes)})` : sql``,
      input.code === undefined ? sql`` : sql` AND a.code = ${input.code}`,
    ],
    sql``,
  );
  const maxRows = Math.max(0, input.maxRows);

  const rows = await queryRows<{ cutoff_ms: number; groups: AttemptOutcomeGroupRow[] }>(
    db,
    sql`WITH clock AS (SELECT ${dbNow(input.now)} AS t),
        w AS (SELECT (SELECT t FROM clock) - ${ms(input.windowMs)} AS cutoff),
        g AS (
          SELECT j.kind, j.tenant_key, a.outcome, a.code, count(*)::int AS count
          FROM "outbox"."attempt" a
          JOIN "outbox"."job" j ON j.id = a.job_id
          WHERE a.finished_at >= (SELECT cutoff FROM w)${filters}
          GROUP BY j.kind, j.tenant_key, a.outcome, a.code
          ORDER BY count DESC, j.kind, j.tenant_key NULLS FIRST, a.outcome, a.code NULLS FIRST
          LIMIT ${maxRows + 1}
        )
        SELECT ${epochMs(sql`(SELECT cutoff FROM w)`)} AS cutoff_ms,
          COALESCE((SELECT json_agg(g ORDER BY g.count DESC, g.kind, g.tenant_key NULLS FIRST, g.outcome, g.code NULLS FIRST) FROM g), '[]'::json) AS groups`,
  );

  const row = firstRow(rows, 'aggregateAttemptOutcomes');

  return {
    cutoff: new Date(row.cutoff_ms),
    groups: row.groups.slice(0, maxRows).map((g): AttemptOutcomeGroup => ({
      kind: g.kind,
      tenantKey: g.tenant_key,
      outcome: g.outcome,
      code: g.code,
      count: g.count,
    })),
    truncated: row.groups.length > maxRows,
  };
}

/** Only waiting jobs are stamped: bookkeeping must never wait on a claimed row's settle lock. */
export async function recordNudgeResult(
  db: SqlRunner,
  input: { ids: readonly string[]; error: string | null; now: Date | null },
) {
  const ids = [...new Set(input.ids)].filter((id) => isUuid(id));

  if (ids.length === 0) {
    return 0;
  }

  const stamp =
    input.error === null
      ? sql`nudge_sent_at = (SELECT t FROM clock), last_nudge_error = NULL`
      : sql`last_nudge_error = ${truncate(input.error, MAX_NUDGE_ERROR_LENGTH)}`;

  const rows = await queryRows<{ stamped: number }>(
    db,
    sql`WITH clock AS (SELECT ${dbNow(input.now)} AS t),
        stamped AS (
          UPDATE "outbox"."job" SET nudge_attempts = nudge_attempts + 1, ${stamp}
          WHERE id = ANY(${uuidArray(ids)}) AND status IN ('pending', 'retrying')
          RETURNING 1
        )
        SELECT count(*)::int AS stamped FROM stamped`,
  );

  return firstRow(rows, 'recordNudgeResult').stamped;
}

interface AttemptOutcomeGroupRow {
  kind: string;
  tenant_key: string | null;
  outcome: AttemptOutcome;
  code: string | null;
  count: number;
}

interface JobSummaryRow {
  id: string;
  kind: string;
  status: JobStatus;
  dedupe_key: string;
  tenant_key: string | null;
  correlation_id: string | null;
  metadata: Record<string, unknown>;
  started_attempts: number;
  finished_attempts: number;
  spent_attempts: number;
  code_streak: number;
  sticky_code: string | null;
  last_error_code: string | null;
  last_error_message: string | null;
  terminal_reason: string | null;
  nudge_attempts: number;
  last_nudge_error: string | null;
  available_at: number;
  next_attempt_at: number | null;
  leased_until: number | null;
  created_at: number;
  updated_at: number;
  finished_at: number | null;
  nudge_sent_at: number | null;
}

function toJobSummary(row: JobSummaryRow): JobSummary {
  return {
    id: row.id,
    kind: row.kind,
    status: row.status,
    dedupeKey: row.dedupe_key,
    tenantKey: row.tenant_key,
    correlationId: row.correlation_id,
    metadata: row.metadata,
    startedAttempts: row.started_attempts,
    finishedAttempts: row.finished_attempts,
    spentAttempts: row.spent_attempts,
    codeStreak: row.code_streak,
    stickyCode: row.sticky_code,
    lastErrorCode: row.last_error_code,
    lastErrorMessage: row.last_error_message,
    terminalReason: row.terminal_reason,
    availableAt: new Date(row.available_at),
    nextAttemptAt: toDate(row.next_attempt_at),
    leasedUntil: toDate(row.leased_until),
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
    finishedAt: toDate(row.finished_at),
    nudgeSentAt: toDate(row.nudge_sent_at),
    nudgeAttempts: row.nudge_attempts,
    lastNudgeError: row.last_nudge_error,
  };
}

function parseCursor(cursor: string) {
  const [createdAt = '', id = ''] = cursor.split('|');

  if (!isUuid(id) || Number.isNaN(Date.parse(createdAt))) {
    throw new Error(`listJobs: invalid cursor ${JSON.stringify(cursor)}`);
  }

  return { createdAt, id };
}

interface ClaimedRow {
  id: string;
  kind: string;
  payload: unknown;
  tenant_key: string | null;
  correlation_id: string | null;
  metadata: Record<string, unknown>;
  lease_token: string;
  started_attempts: number;
  spent_attempts: number;
  code_streak: number;
  last_error_code: string | null;
  sticky_code: string | null;
}

/** One statement claims and opens the attempt: a claim never exists without its attempt. */
function claimAndOpenAttempt(leaseToken: string, trigger: AttemptTrigger) {
  return sql`claimed AS (
          UPDATE "outbox"."job" j SET
            status = 'processing',
            started_attempts = j.started_attempts + 1,
            next_attempt_at = NULL,
            lease_token = ${leaseToken},
            leased_until = (SELECT t FROM clock) + k.lease_ms * interval '1 millisecond',
            updated_at = (SELECT t FROM clock)
          FROM picked, k
          WHERE j.id = picked.id AND k.kind = j.kind
          RETURNING j.id, j.kind, j.payload, j.tenant_key, j.correlation_id, j.metadata,
                    j.lease_token, j.started_attempts, j.spent_attempts, j.code_streak, j.last_error_code,
                    j.sticky_code
        ),
        opened AS (
          INSERT INTO "outbox"."attempt" (job_id, attempt_no, kind, lease_token, trigger, started_at)
          SELECT id, started_attempts, kind, lease_token, ${trigger}, (SELECT t FROM clock) FROM claimed
          RETURNING 1
        )`;
}

function toClaimedJob(row: ClaimedRow): ClaimedJob {
  return {
    id: row.id,
    kind: row.kind,
    payload: row.payload,
    tenantKey: row.tenant_key,
    correlationId: row.correlation_id,
    metadata: row.metadata,
    leaseToken: row.lease_token,
    attemptNo: row.started_attempts,
    spentAttempts: row.spent_attempts,
    codeStreak: row.code_streak,
    lastErrorCode: row.last_error_code,
    stickyCode: row.sticky_code,
  };
}

function truncate(message: string, max?: number): string;
function truncate(message: string | null, max?: number): string | null;
function truncate(message: string | null, max = MAX_MESSAGE_LENGTH) {
  if (message === null || message.length <= max) {
    return message;
  }

  return `${message.slice(0, max - 1)}…`;
}

function firstRow<Row>(rows: Row[], statement: string) {
  const row = rows[0];

  if (!row) {
    throw new Error(`${statement}: statement returned no row`);
  }

  return row;
}

/** One job with its attempt ledger, for an operator view. Payload excluded. */
export async function getJob(db: SqlRunner, input: { id: string; tenantKey?: TenantScope }) {
  if (!isUuid(input.id)) {
    return null;
  }

  const [row] = await queryRows<JobSummaryRow>(
    db,
    sql`SELECT ${jobSummaryColumns}
        FROM "outbox"."job" j
        WHERE j.id = ${input.id}${tenantClause(sql`j.tenant_key`, input.tenantKey)}`,
  );

  if (!row) {
    return null;
  }

  const attempts = await queryRows<{
    attempt_no: number;
    trigger: AttemptTrigger;
    outcome: AttemptOutcome | null;
    code: string | null;
    message: string | null;
    duration_ms: number | null;
    started_at: number;
    finished_at: number | null;
    read_at: unknown;
  }>(
    db,
    sql`SELECT attempt_no, trigger, outcome, code, message, duration_ms,
               ${epochMs(sql`started_at`)} AS started_at, ${epochMs(sql`finished_at`)} AS finished_at, now() AS read_at
        FROM "outbox"."attempt" WHERE job_id = ${input.id}
        ORDER BY attempt_no`,
  );

  return {
    ...toJobSummary(row),
    attempts: attempts.map((a) => ({
      attemptNo: a.attempt_no,
      trigger: a.trigger,
      outcome: a.outcome,
      code: a.code,
      message: a.message,
      durationMs: a.duration_ms,
      startedAt: new Date(a.started_at),
      finishedAt: toDate(a.finished_at),
    })),
  };
}

/** Every job one request produced, newest first. */
export async function findJobsByCorrelationId(
  db: SqlRunner,
  input: { correlationId: string; limit: number; tenantKey?: TenantScope },
) {
  const rows = await queryRows<JobSummaryRow>(
    db,
    sql`SELECT ${jobSummaryColumns}
        FROM "outbox"."job" j
        WHERE j.correlation_id = ${input.correlationId}${tenantClause(sql`j.tenant_key`, input.tenantKey)}
        ORDER BY j.created_at DESC, j.id DESC
        LIMIT ${Math.max(0, input.limit)}`,
  );

  return rows.map(toJobSummary);
}

/** Makes a pending or retrying job due now ("retry now"). */
export async function rescheduleNow(
  db: SqlRunner,
  input: { id: string; now: Date | null; tenantKey?: TenantScope },
) {
  if (!isUuid(input.id)) {
    return { result: 'not_found' as const, kind: null };
  }

  const [row] = await updateMatching(db, {
    match: sql`id = ${input.id}`,
    tenantKey: input.tenantKey,
    now: input.now,
    set: sql`available_at = CASE WHEN status = 'pending' THEN (SELECT t FROM clock) ELSE available_at END,
        next_attempt_at = CASE WHEN status = 'retrying' THEN (SELECT t FROM clock) ELSE next_attempt_at END,
        updated_at = (SELECT t FROM clock)`,
    allowed: WAITING,
  });

  if (!row) {
    return { result: 'not_found' as const, kind: null };
  }

  return {
    result: row.done ? ('rescheduled' as const) : ('not_waiting' as const),
    kind: row.kind,
  };
}

/**
 * Erasure: deletes one tenant's jobs (attempts cascade), in flight or not. A worker
 * mid-attempt then settles `lease_lost`. One bounded batch per call.
 */
export async function deleteTenantJobs(db: SqlRunner, input: { tenantKey: string; batch: number }) {
  const rows = await queryRows<{ deleted: number }>(
    db,
    sql`WITH doomed AS (
          SELECT id FROM "outbox"."job" WHERE tenant_key = ${input.tenantKey} LIMIT ${input.batch}
        ),
        deleted AS (DELETE FROM "outbox"."job" WHERE id IN (SELECT id FROM doomed) RETURNING 1)
        SELECT count(*)::int AS deleted, now() AS read_at FROM deleted`,
  );

  return firstRow(rows, 'deleteTenantJobs').deleted;
}

/** Cancels many waiting jobs in one statement; every requested id gets a result. */
export async function cancelJobs(
  db: SqlRunner,
  input: { ids: readonly string[]; reason: string; now: Date | null; tenantKey?: TenantScope },
) {
  const rows = await updateByIds(db, input, cancellation(input.reason));

  return mapBulk(input.ids, rows, cancelResult);
}

/** Replays many dead or cancelled jobs in one statement, each with a fresh budget. */
export async function replayJobs(
  db: SqlRunner,
  input: { ids: readonly string[]; now: Date | null; tenantKey?: TenantScope },
) {
  const rows = await updateByIds(db, input, REPLAY);

  return mapBulk(input.ids, rows, replayResult);
}

const WAITING = sql`status IN ('pending', 'retrying')`;

/** An operator's state change: what it writes, and the statuses it may write over. */
interface StatusChange {
  set: SQL;
  allowed: SQL;
}

const cancellation = (reason: string): StatusChange => ({
  set: sql`status = 'cancelled', terminal_reason = ${reason}, next_attempt_at = NULL,
        finished_at = (SELECT t FROM clock), updated_at = (SELECT t FROM clock)`,
  allowed: WAITING,
});

// Fresh retry budget; the attempt ledger keeps its numbering.
const REPLAY: StatusChange = {
  set: sql`status = 'pending', available_at = (SELECT t FROM clock), next_attempt_at = NULL,
        terminal_reason = NULL, finished_at = NULL, spent_attempts = 0, code_streak = 0,
        sticky_code = NULL, updated_at = (SELECT t FROM clock)`,
  allowed: sql`status IN ('dead', 'cancelled')`,
};

interface MatchedRow {
  id: string;
  kind: string;
  status: JobStatus;
  /** The change applied: the row's status allowed it. */
  done: boolean;
}

const cancelResult = (row: MatchedRow) =>
  row.done
    ? ('cancelled' as const)
    : isTerminal(row.status)
      ? ('already_terminal' as const)
      : ('in_flight' as const);

const replayResult = (row: MatchedRow) =>
  row.done ? ('replayed' as const) : ('not_replayable' as const);

/** One statement: every matching job (in the tenant scope) reports its status, and whether it changed. */
function updateMatching(
  db: SqlRunner,
  input: StatusChange & { match: SQL; tenantKey: TenantScope; now: Date | null },
) {
  return queryRows<MatchedRow>(
    db,
    sql`WITH clock AS (SELECT ${dbNow(input.now)} AS t),
        target AS (
          SELECT id, kind, status FROM "outbox"."job"
          WHERE ${input.match}${tenantClause(sql`tenant_key`, input.tenantKey)}
        ),
        updated AS (
          UPDATE "outbox"."job" SET ${input.set}
          WHERE id IN (SELECT id FROM target) AND ${input.allowed}
          RETURNING id
        )
        SELECT target.id::text AS id, target.kind, target.status, (updated.id IS NOT NULL) AS done
        FROM target LEFT JOIN updated USING (id)`,
  );
}

async function updateByIds(
  db: SqlRunner,
  input: { ids: readonly string[]; now: Date | null; tenantKey?: TenantScope },
  change: StatusChange,
) {
  const valid = [...new Set(input.ids)].filter((id) => isUuid(id));

  if (valid.length === 0) {
    return [];
  }

  return updateMatching(db, {
    ...change,
    match: sql`id = ANY(${uuidArray(valid)})`,
    tenantKey: input.tenantKey,
    now: input.now,
  });
}

function mapBulk<R extends string>(
  ids: readonly string[],
  rows: MatchedRow[],
  decide: (row: MatchedRow) => R,
) {
  const byId = new Map(rows.map((row) => [row.id, row]));
  const results: Record<string, R | 'not_found'> = {};

  for (const id of new Set(ids)) {
    // PostgreSQL accepts any-case hex and returns it lowercase; keys stay as the caller wrote them.
    const row = byId.get(id.toLowerCase());
    results[id] = row ? decide(row) : 'not_found';
  }

  return results;
}
