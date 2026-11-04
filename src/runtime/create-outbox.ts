import type { StandardSchemaV1 } from '@standard-schema/spec';

import { v7 as uuidv7 } from 'uuid';

import type { SqlRunner } from '../drizzle/catalog';
import type { MessageRedactor } from './sanitize-message';

import { LEASE_MARGIN_MS, type AnyJob, type JobDeps } from '../core/define-job';
import { createJobRegistry } from '../core/registry';
import {
  cancelJob,
  deleteTenantJobs,
  insertJob,
  replayJob,
  rescheduleNow,
  cancelJobs,
  replayJobs,
} from '../store/outbox-store';
import { settledWithin, unwrap, untilDeadline } from './deadline';
import { createEmitter, type EventSink } from './events';
import {
  createRuntime,
  DEFAULT_PHASE_DEADLINE_MS,
  type Runtime,
  type TransitionHook,
} from './runtime';
import {
  MAX_QUEUE_DELAY_SECONDS,
  systemClock,
  type Clock,
  type LaneConnection,
  type QueueSender,
} from './types';

type UnionToIntersection<U> = (U extends unknown ? (x: U) => void : never) extends (
  x: infer I,
) => void
  ? I
  : never;

/** What `deps` must satisfy: every registered job's needs at once. */
export type DepsOf<J extends readonly AnyJob[]> = UnionToIntersection<JobDeps<J[number]>>;

// Required as soon as one job declares deps; optional when none does.
type DepsOption<D> = [D] extends [never]
  ? { deps?: unknown }
  : unknown extends D
    ? { deps?: unknown }
    : { deps: D };

export type OutboxOptions<J extends readonly AnyJob[]> = OutboxBaseOptions<J> &
  DepsOption<NoInfer<DepsOf<J>>>;

export interface OutboxBaseOptions<J extends readonly AnyJob[]> {
  jobs: J;
  /** One call per lane per invocation, plus short-lived ones; never at construction. */
  connect: () => Promise<LaneConnection>;
  /** Default 1; one connection per lane. */
  lanes?: number;
  /** One queue or one per lane; a lane without a queue is driven by cron only. */
  queue?: QueueSender | Readonly<Record<string, QueueSender>>;
  /** Costs one connection per nudge. */
  recordNudges?: boolean;
  /**
   * Bound for one operator action (cancel, replay, retryNow, eraseTenant, …): acquisition, SQL
   * and close. Past it the connection is destroyed and the call rejects. Default 60 s.
   */
  operationTimeoutMs?: number;
  /**
   * Largest payload `enqueue` accepts, in UTF-8 bytes of its JSON form; a bigger one returns
   * `invalid_payload`. Every claim reads the payload back from the hot table, so put large data in
   * object storage (e.g. R2) and enqueue its key. Default 256 KiB.
   */
  maxPayloadBytes?: number;
  /** Runs in the settle transaction before the job row write; commits only if the transition applies. */
  onTransition?: TransitionHook;
  events?: EventSink;
  redactError?: MessageRedactor;
  warnAtStreak?: number;
  clock?: Clock;
  /**
   * Mints job ids. Default: `uuidv7()`. Must return a UUID string (the `id` column is `uuid`);
   * time-ordered ids keep the primary-key index and `(created_at, id)` pagination well-behaved.
   */
  generateId?: () => string;
  /**
   * This deploy's version, e.g. `env.CF_VERSION_METADATA`. Jobs are stamped with it, and an
   * older deploy skips newer jobs for `gateMs` (default 6 h) during a gradual rollout.
   */
  version?: { timestamp: string | number; gateMs?: number };
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const DEFAULT_VERSION_GATE_MS = 6 * 3_600_000;

function parseVersion(version: OutboxOptions<readonly AnyJob[]>['version']) {
  if (version === undefined) {
    return undefined;
  }

  const mineMs =
    typeof version.timestamp === 'number' ? version.timestamp : Date.parse(version.timestamp);

  if (!Number.isFinite(mineMs)) {
    throw new Error(
      `createOutbox: version.timestamp ${JSON.stringify(version.timestamp)} is not a date`,
    );
  }

  return { mineMs, gateMs: version.gateMs ?? DEFAULT_VERSION_GATE_MS };
}

export type EnqueueRequest<T extends AnyJob> = {
  payload: StandardSchemaV1.InferInput<T['payload']>;
  dedupeKey: string;
  tenantKey?: string | null;
  correlationId?: string | null;
  metadata?: Record<string, unknown>;
  /** Milliseconds. */
  delayMs?: number;
};

export type EnqueueResult =
  | { status: 'created' | 'exists'; id: string; kind: string }
  | { status: 'dedupe_conflict'; reason: 'kind'; id: string; existingKind: string }
  | { status: 'invalid_payload'; issues: readonly StandardSchemaV1.Issue[] };

/** Not part of the public API. */
export const RUNTIME = Symbol('workers-outbox.runtime');

const MAX_DEDUPE_KEY_LENGTH = 512;

export const DEFAULT_MAX_PAYLOAD_BYTES = 256 * 1024;

const MAX_ERASE_BATCH = 10_000;

export function createOutbox<const J extends readonly AnyJob[]>(
  options: OutboxOptions<J>,
): Outbox<J> {
  const registry = createJobRegistry(options.jobs);
  const emit = createEmitter(options.events);
  const clock = options.clock ?? systemClock;
  const lanes = options.lanes ?? 1;

  if (!Number.isInteger(lanes) || lanes < 1) {
    throw new Error(`createOutbox: lanes must be a positive integer, got ${lanes}`);
  }

  const version = parseVersion(options.version);
  const generateId = options.generateId ?? uuidv7;

  const runtime = createRuntime({
    version,
    registry,
    deps: options.deps,
    connect: options.connect,
    lanes,
    queues: normalizeQueues(options.queue),
    recordNudges: options.recordNudges ?? false,
    onTransition: options.onTransition,
    emit,
    clock,
    redact: options.redactError,
    warnAtStreak: options.warnAtStreak,
    // Retries outlast a Hyperdrive failover; every wait, sleep and lock shares one 25 s budget
    // inside the 30 s lease margin, so the sum can never overrun it.
    settleRetry: {
      attempts: 6,
      delaysMs: [250, 750, 1_500, 3_000, 5_000, 8_000],
      lockTimeoutMs: 5_000,
      budgetMs: LEASE_MARGIN_MS - 5_000,
    },
  });

  const operationTimeoutMs = options.operationTimeoutMs ?? DEFAULT_PHASE_DEADLINE_MS;

  if (!Number.isInteger(operationTimeoutMs) || operationTimeoutMs < 1) {
    throw new Error(
      `createOutbox: operationTimeoutMs must be a positive integer, got ${operationTimeoutMs}`,
    );
  }

  const maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;

  if (!Number.isInteger(maxPayloadBytes) || maxPayloadBytes < 1) {
    throw new Error(
      `createOutbox: maxPayloadBytes must be a positive integer, got ${maxPayloadBytes}`,
    );
  }

  // Operator actions run outside any invocation deadline: they get their own, and a session
  // abandoned mid-query is destroyed, never returned to a pool.
  const withConnection = async <T>(
    work: (connection: LaneConnection) => Promise<T>,
    /** An invocation's deadline, when the work runs inside one (a scheduled tick). */
    invocationDeadlineAt = Infinity,
  ): Promise<T> => {
    const deadlineAt = Math.min(clock.nowMs() + operationTimeoutMs, invocationDeadlineAt);
    const connecting = options.connect();
    const got = await untilDeadline(clock, connecting, deadlineAt);

    if (!got.done) {
      void connecting.then(
        (late) => late.destroy().catch(() => undefined),
        () => undefined,
      );
    }

    const connection = unwrap(got, 'connection acquisition');
    const running = Promise.resolve().then(() => work(connection));
    const result = await untilDeadline(clock, running, deadlineAt);

    if (!result.done) {
      running.catch(() => undefined);
      await settledWithin(connection.destroy(), 5_000);

      return unwrap(result, 'operator action');
    }

    const closed = await settledWithin(connection.close(), 2_000);

    if (!closed.done) {
      void connection.destroy().catch(() => undefined);
    }

    return unwrap(result, 'operator action');
  };

  const outbox: Outbox<J> = {
    /** Runs iff the caller's transaction commits; a duplicate `dedupeKey` returns the existing job. */
    async enqueue<T extends J[number]>(
      tx: SqlRunner,
      job: T,
      request: EnqueueRequest<T>,
    ): Promise<EnqueueResult> {
      if (registry.get(job.kind) !== job) {
        throw new Error(
          `outbox.enqueue: job ${JSON.stringify(job.kind)} is not registered with this outbox`,
        );
      }

      if (request.tenantKey === '') {
        throw new Error('outbox.enqueue: tenantKey must be non-empty, or null for no tenant');
      }

      if (request.dedupeKey.length === 0 || request.dedupeKey.length > MAX_DEDUPE_KEY_LENGTH) {
        throw new Error(`outbox.enqueue: dedupeKey must be 1–${MAX_DEDUPE_KEY_LENGTH} characters`);
      }

      // Persist the JSON form of the *input*; execution parses it once (transforms run once).
      const wire = toWire(request.payload, maxPayloadBytes);

      if (!wire.ok) {
        return { status: 'invalid_payload', issues: [{ message: wire.message }] };
      }

      const validated = await job.payload['~standard'].validate(wire.value);

      if (validated.issues) {
        return { status: 'invalid_payload', issues: validated.issues };
      }

      const id = generateId();

      if (!UUID_RE.test(id)) {
        throw new Error(`outbox.enqueue: generateId returned ${JSON.stringify(id)}, not a UUID`);
      }

      const row = await insertJob(
        tx,
        {
          id,
          kind: job.kind,
          dedupeKey: request.dedupeKey,
          payload: wire.value,
          tenantKey: request.tenantKey ?? null,
          correlationId: request.correlationId ?? null,
          metadata: request.metadata ?? {},
          delayMs: Math.max(0, request.delayMs ?? 0),
          minVersionMs: version?.mineMs ?? null,
        },
        clock.dbNow(),
      );

      if (row.kind !== job.kind) {
        return { status: 'dedupe_conflict', reason: 'kind', id: row.id, existingKind: row.kind };
      }

      emit({ type: 'enqueued', jobId: row.id, kind: row.kind, created: row.created });

      return { status: row.created ? 'created' : 'exists', id: row.id, kind: row.kind };
    },

    // Nudges only after the commit resolves: a rollback must never announce a job.
    async transaction<Tx extends SqlRunner, R>(
      db: { transaction<T>(fn: (tx: Tx) => Promise<T>): Promise<T> },
      work: (tx: Tx, enqueue: ScopedEnqueue<J>) => Promise<R>,
    ): Promise<R> {
      // Grouped by delay: a delayed job is nudged for when it falls due, not now, when its claim
      // would only miss. Past the queue's 24 h ceiling no nudge is sent; the cron sweep runs it.
      const created = new Map<number, { id: string; kind: string }[]>();

      const result = await db.transaction((tx) =>
        work(tx, async (job, request) => {
          const enqueued = await outbox.enqueue(tx, job, request);

          if (enqueued.status === 'created') {
            const delayMs = Number.isFinite(request.delayMs) ? Math.max(0, request.delayMs!) : 0;

            // Rounded up: an early nudge misses, a late one by under a second is harmless.
            const delaySeconds = Math.ceil(delayMs / 1_000);

            if (delaySeconds <= MAX_QUEUE_DELAY_SECONDS) {
              const group = created.get(delaySeconds) ?? [];
              group.push({ id: enqueued.id, kind: enqueued.kind });
              created.set(delaySeconds, group);
            }
          }

          return enqueued;
        }),
      );

      for (const [delaySeconds, jobs] of created) {
        await runtime.nudge(jobs, delaySeconds);
      }

      return result;
    },

    /** Call after commit; bare ids go to the default lane. Never throws. */
    nudge: (jobs: readonly (string | { id: string; kind: string })[]) => runtime.nudge(jobs),

    cancel: (
      ref: { id: string } | { dedupeKey: string; tenantKey: string | null },
      reason: string,
      scope: TenantScope = {},
    ) =>
      withConnection((c) =>
        cancelJob(c.db, { ref, reason, now: clock.dbNow(), ...tenantScope(scope) }),
      ),

    replay: (id: string, scope: TenantScope = {}) =>
      withConnection((c) => replayJob(c.db, { id, now: clock.dbNow(), ...tenantScope(scope) })),

    /** Makes a pending or retrying job due now. */
    retryNow: (id: string, scope: TenantScope = {}) =>
      withConnection(async (c) => {
        // The statement returns the kind, so routing the nudge never reads the attempt ledger.
        const { result, kind } = await rescheduleNow(c.db, {
          id,
          now: clock.dbNow(),
          ...tenantScope(scope),
        });

        if (result === 'rescheduled' && kind !== null) {
          // This connection is idle again: it records the nudge, no second checkout.
          await runtime.nudge([{ id, kind }], 0, { db: c.db });
        }

        return result;
      }),

    /** One statement; every id gets a result. */
    cancelMany: (ids: readonly string[], reason: string, scope: TenantScope = {}) =>
      withConnection((c) =>
        cancelJobs(c.db, { ids, reason, now: clock.dbNow(), ...tenantScope(scope) }),
      ),

    /** One statement; every id gets a result. */
    replayMany: (ids: readonly string[], scope: TenantScope = {}) =>
      withConnection((c) => replayJobs(c.db, { ids, now: clock.dbNow(), ...tenantScope(scope) })),

    /** Erasure: deletes every job of one tenant, including in-flight ones. Returns the count. */
    eraseTenant: (tenantKey: string, opts: { batch?: number } = {}) =>
      withConnection(async (c) => {
        const batch = opts.batch ?? 1_000;

        // `removed < batch` ends the loop; a batch of 0 never could.
        if (!Number.isInteger(batch) || batch < 1 || batch > MAX_ERASE_BATCH) {
          throw new Error(
            `outbox.eraseTenant: batch must be an integer in [1, ${MAX_ERASE_BATCH}], got ${batch}`,
          );
        }

        let deleted = 0;

        for (;;) {
          const removed = await deleteTenantJobs(c.db, { tenantKey, batch });
          deleted += removed;

          if (removed < batch) {
            return deleted;
          }
        }
      }),

    [RUNTIME]: { ...runtime, withConnection },
  };

  return outbox;
}

/**
 * Restricts an operator action to one tenant. Out-of-scope jobs report `not_found`.
 * - omitted (or `undefined`): any tenant — the unscoped, admin form;
 * - `null`: only jobs enqueued without a tenant;
 * - a string: only that tenant's jobs.
 */
export interface TenantScope {
  tenantKey?: string | null | undefined;
}

/** `enqueue` bound to one transaction. */
export type ScopedEnqueue<J extends readonly AnyJob[]> = <T extends J[number]>(
  job: T,
  request: EnqueueRequest<T>,
) => Promise<EnqueueResult>;

type Result<F extends (...args: never[]) => Promise<unknown>> = Awaited<ReturnType<F>>;

/**
 * What `createOutbox` returns. Named, so an exported `outboxFor` compiles under `declaration`
 * without reaching the runtime's internal key.
 */
export interface Outbox<J extends readonly AnyJob[] = readonly AnyJob[]> {
  /** Runs iff the caller's transaction commits; a duplicate `dedupeKey` returns the existing job. */
  enqueue<T extends J[number]>(
    tx: SqlRunner,
    job: T,
    request: EnqueueRequest<T>,
  ): Promise<EnqueueResult>;
  /**
   * Runs `work` in `db.transaction` and nudges the jobs it created once that commits. A throw
   * rolls back and nudges nothing. Pass the top-level db, not a transaction (drizzle would open a
   * savepoint, and the nudge would precede the outer commit).
   */
  transaction<Tx extends SqlRunner, R>(
    db: { transaction<T>(fn: (tx: Tx) => Promise<T>): Promise<T> },
    work: (tx: Tx, enqueue: ScopedEnqueue<J>) => Promise<R>,
  ): Promise<R>;
  /** Call after commit; bare ids go to the default lane. Never throws. */
  nudge(jobs: readonly (string | { id: string; kind: string })[]): Promise<void>;
  cancel(
    ref: { id: string } | { dedupeKey: string; tenantKey: string | null },
    reason: string,
    scope?: TenantScope,
  ): Promise<Result<typeof cancelJob>>;
  /** dead/cancelled → a fresh retry budget. */
  replay(id: string, scope?: TenantScope): Promise<Result<typeof replayJob>>;
  /** Makes a pending or retrying job due now, and nudges it. */
  retryNow(id: string, scope?: TenantScope): Promise<Result<typeof rescheduleNow>['result']>;
  /** One statement; every id gets a result. */
  cancelMany(
    ids: readonly string[],
    reason: string,
    scope?: TenantScope,
  ): Promise<Result<typeof cancelJobs>>;
  /** One statement; every id gets a result. */
  replayMany(ids: readonly string[], scope?: TenantScope): Promise<Result<typeof replayJobs>>;
  /** Erasure: deletes every job of one tenant, including in-flight ones. Returns the count. */
  eraseTenant(tenantKey: string, opts?: { batch?: number }): Promise<number>;
  /** Not part of the public API. */
  readonly [RUNTIME]: OutboxRuntime;
}

/** Not part of the public API: the runtime plus a bounded connection for one-off work. */
export type OutboxRuntime = Runtime & {
  withConnection<T>(
    work: (connection: LaneConnection) => Promise<T>,
    invocationDeadlineAt?: number,
  ): Promise<T>;
};

const isQueueSender = (value: unknown): value is QueueSender =>
  typeof value === 'object' && value !== null && typeof (value as QueueSender).send === 'function';

function normalizeQueues(
  queue: OutboxOptions<readonly AnyJob[]>['queue'],
): Record<string, QueueSender> {
  if (queue === undefined) {
    return {};
  }

  return isQueueSender(queue) ? { default: queue } : { ...queue };
}

const tenantScope = (scope: TenantScope) =>
  scope.tenantKey === undefined ? {} : { tenantKey: scope.tenantKey };

const utf8 = new TextEncoder();

function toWire(
  payload: unknown,
  maxBytes: number,
): { ok: true; value: unknown } | { ok: false; message: string } {
  try {
    const json = JSON.stringify(payload);

    if (json === undefined) {
      return { ok: false, message: 'payload is not JSON-serializable' };
    }

    // Cheap bound first: a UTF-8 string is at most 3 bytes per UTF-16 code unit.
    const bytes = json.length * 3 <= maxBytes ? json.length : utf8.encode(json).byteLength;

    if (bytes > maxBytes) {
      return {
        ok: false,
        message: `payload is ${bytes} bytes as JSON, over maxPayloadBytes (${maxBytes}); store it elsewhere and enqueue a reference`,
      };
    }

    return { ok: true, value: JSON.parse(json) as unknown };
  } catch (error) {
    return {
      ok: false,
      message: `payload is not JSON-serializable: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
}
