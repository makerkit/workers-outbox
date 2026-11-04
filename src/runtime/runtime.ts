import { v7 as uuidv7 } from 'uuid';

import type { AnyJob } from '../core/define-job';
import type { JobRegistry } from '../core/registry';
import type { SqlRunner } from '../drizzle/catalog';
import type { Emit, OutboxEvent } from './events';

import { DEFAULT_BACKLOG_AGED_MS, evaluateBacklog, type BacklogThresholds } from '../core/backlog';
import { HOOK_FAILED_CODE, retry, type SettleOutcome } from '../core/outcome';
import { decideTransition, type JobBudgetState, type Transition } from '../core/transition';
import {
  claimByIds,
  claimDue,
  countBacklog,
  deleteFinishedJobs,
  hasDueJobs,
  isLeaseHeld,
  listDueJobIds,
  listExpiredLeases,
  readAttemptOutcome,
  setSettlementTimeouts,
  recordNudgeResult,
  settleJob,
  type ClaimedJob,
  type KindFilter,
  type VersionGate,
} from '../store/outbox-store';
import { isConnectionError, isTimeoutError, isTransientDbError, sqlstateOf } from './db-errors';
import { DeadlineExceededError, settledWithin, unwrap, untilDeadline } from './deadline';
import { classifySafely, executeHandler } from './execute-handler';
import { sanitizeErrorMessage, type MessageRedactor } from './sanitize-message';
import { serializeError } from './serialize-error';
import {
  MAX_IDS_PER_NUDGE,
  MAX_QUEUE_DELAY_SECONDS,
  type Clock,
  type LaneConnection,
  type OutboxDb,
  type OutboxMessage,
  type QueueSender,
} from './types';

export type TransitionHook = (tx: OutboxDb, event: TransitionHookEvent) => Promise<void>;

export interface TransitionHookEvent {
  job: ClaimedJob;
  transition: Transition;
}

export interface RuntimeConfig {
  registry: JobRegistry;
  deps: unknown;
  connect: () => Promise<LaneConnection>;
  lanes: number;
  queues: Readonly<Record<string, QueueSender>>;
  recordNudges: boolean;
  onTransition: TransitionHook | undefined;
  emit: Emit;
  clock: Clock;
  redact: MessageRedactor | undefined;
  warnAtStreak: number | undefined;
  settleRetry: {
    attempts: number;
    delaysMs: readonly number[];
    lockTimeoutMs: number;
    /** One settlement's whole budget, retries included; inside the lease margin. */
    budgetMs: number;
  };
  /** This deploy's version gate; claims skip jobs a newer deploy enqueued. */
  version: VersionGate | undefined;
}

export interface Budget {
  maxJobs: number;
  deadlineMs: number;
}

export interface OutcomeTally {
  claimed: number;
  succeeded: number;
  retrying: number;
  dead: number;
  cancelled: number;
  leaseLost: number;
  settleFailed: number;
}

export interface RunReport extends OutcomeTally {
  byKind: Record<string, OutcomeTally>;
  /** Runtime faults, not handler failures. */
  errored: number;
  erroredSamples: string[];
  reachedLimit: boolean;
  reachedDeadline: boolean;
}

type TallyKey = keyof OutcomeTally;

const emptyTally = (): OutcomeTally => ({
  claimed: 0,
  succeeded: 0,
  retrying: 0,
  dead: 0,
  cancelled: 0,
  leaseLost: 0,
  settleFailed: 0,
});

const emptyReport = (): RunReport => ({
  ...emptyTally(),
  byKind: {},
  errored: 0,
  erroredSamples: [],
  reachedLimit: false,
  reachedDeadline: false,
});

const MAX_ERROR_SAMPLES = 5;

function tally(report: RunReport, kind: string, key: TallyKey) {
  report[key]++;
  const forKind = (report.byKind[kind] ??= emptyTally());
  forKind[key]++;
}

function recordError(report: RunReport, error: unknown) {
  report.errored++;

  if (report.erroredSamples.length < MAX_ERROR_SAMPLES) {
    report.erroredSamples.push(error instanceof Error ? error.message : String(error));
  }
}

export type Trigger = 'nudge' | 'sweep' | 'drain' | 'recovery';

interface SettleContext {
  thrown: boolean;
  recovered: boolean;
  trigger: Trigger;
}

class HookError extends Error {
  constructor(readonly original: unknown) {
    super('onTransition hook failed', { cause: original });
  }
}

/** The handler's `ok({ commit })` callback threw inside the settle transaction. */
class CommitError extends Error {
  constructor(readonly original: unknown) {
    super('ok({ commit }) failed', { cause: original });
  }
}

class LeaseLostRollback extends Error {}

interface Settlement {
  /** `clock.nowMs()` value after which nothing more is attempted. */
  readonly deadlineAt: number;
  /** Set by the watchdog: the work may still unwind, but must not act or report. */
  abandoned: boolean;
}

/** One connection at a time, owned by the runtime for an invocation. */
interface Lane {
  readonly db: OutboxDb;
  /** Holds a live connection and was not retired. */
  readonly usable: boolean;
  /** Its connection reports a dead session (`LaneConnection.isBroken`). */
  readonly broken: boolean;
  /** Before each claim: reconnects a lane whose reconnect failed or whose session died; false once retired. */
  ready(deadlineAt: number): Promise<boolean>;
  /** Destroys the session, then acquires a replacement. */
  replace(): Promise<void>;
  /** Destroys the session; the lane claims nothing more this invocation. */
  retire(): Promise<void>;
  /** `work` on this lane's session, bounded: past the deadline the lane retires and this throws. */
  bounded<T>(work: () => PromiseLike<T>, deadlineAt: number, what: string): Promise<T>;
  close(): Promise<void>;
}

type PersistResult = 'applied' | 'lease_lost' | 'hook_failed' | 'settle_failed' | 'commit_failed';

interface NudgeVia {
  /** An idle connection the caller holds and bounds itself (an operator action). */
  db?: SqlRunner;
  /** The idle lane that just settled: bookkeeping runs on it, bounded, and retires it on expiry. */
  lane?: Lane;
  /** The deadline this nudge (its send and bookkeeping) must not outlive. */
  deadlineAt?: number;
  /** Stamp even with `recordNudges` off: the handoff cooldown reads `nudge_sent_at`. */
  record?: boolean;
}

// Never settle past the lease: recovery may take the job over from then on.
const SETTLE_SAFETY_MS = 2_000;

// The watchdog fires just after the native timeouts should have.
const WATCHDOG_SLACK_MS = 1_000;

const DESTROY_WAIT_MS = 5_000;

// A healthy close() that never returns is abandoned, and the session destroyed instead.
const CLOSE_WAIT_MS = 2_000;

const QUEUE_SEND_TIMEOUT_MS = 10_000;

// Settling a claim_late attempt: brief, fenced, and independent of the spent lease.
const CLAIM_LATE_SETTLE_MS = 5_000;

/** A claimed job starts only if this much lease and deadline remain after its timeout and grace. */
const MIN_SETTLE_WINDOW_MS = 10_000;

/** For a phase called without an invocation deadline (direct runtime use, operator paths). */
export const DEFAULT_PHASE_DEADLINE_MS = 60_000;

/** Recovery's share of an invocation when none is given: count-limited *and* time-limited. */
export const DEFAULT_RECOVERY_BUDGET_MS = 120_000;

export interface Scope {
  lane?: string;
  kinds?: readonly string[];
}

const RENUDGE_DELAY_SECONDS = 5;

// One platform invocation window: a handed-off nudge has run, or been re-sent, by then. Within
// it the cron does not hand the same job off again, so a backlog cannot pile up duplicates.
export const HANDOFF_COOLDOWN_MS = 15 * 60_000;

export function createRuntime(config: RuntimeConfig) {
  const { registry, emit, clock } = config;
  const gate = config.version ? { version: config.version } : {};

  const kindFilter = (remainingMs: number, scope: Scope = {}): KindFilter[] => {
    const lane = scope.lane === undefined ? {} : { lane: scope.lane };
    const inScope = (kind: string) => scope.kinds === undefined || scope.kinds.includes(kind);
    const fitting = new Set(registry.claimable(remainingMs, lane).map((k) => k.kind));

    return registry
      .claimable(Number.POSITIVE_INFINITY, lane)
      .filter((k) => inScope(k.kind))
      .map((k) => ({ ...k, fits: fitting.has(k.kind) }));
  };

  async function withLanes<T>(
    count: number,
    work: (lane: Lane, index: number) => Promise<T>,
    deadlineAt: number = clock.nowMs() + DEFAULT_PHASE_DEADLINE_MS,
  ) {
    const opening = await Promise.allSettled(
      Array.from({ length: Math.max(1, count) }, () => openLane(deadlineAt)),
    );
    const lanes = opening.flatMap((r) => (r.status === 'fulfilled' ? [r.value] : []));
    const failed = opening.find((r): r is PromiseRejectedResult => r.status === 'rejected');

    if (failed) {
      await Promise.all(lanes.map((lane) => lane.close().catch(() => undefined)));
      throw failed.reason;
    }

    try {
      // Every lane settles before any connection closes: a sibling may still be mid-job.
      const settled = await Promise.allSettled(lanes.map((lane, index) => work(lane, index)));
      const rejected = settled.find((r): r is PromiseRejectedResult => r.status === 'rejected');

      if (rejected) {
        throw rejected.reason;
      }

      return settled.map((r) => (r as PromiseFulfilledResult<Awaited<T>>).value);
    } finally {
      await Promise.all(lanes.map((lane) => lane.close()));
    }
  }

  /**
   * A lane owns one connection at a time. A runaway or timed-out session is destroyed — never
   * returned to a pool — *before* its replacement is acquired, so a one-client pool still works.
   */
  /** Acquisition bounded by `deadlineAt`; a connection that arrives after it is destroyed. */
  async function acquire(deadlineAt: number, isWanted: () => boolean = () => true) {
    const connecting = config.connect();
    const got = await untilDeadline(clock, connecting, deadlineAt);

    if (!got.done) {
      void connecting.then(
        (late) => late.destroy().catch(() => undefined),
        () => undefined,
      );

      throw new DeadlineExceededError('connection acquisition');
    }

    const connection = unwrap(got, 'connection acquisition');

    if (!isWanted()) {
      void connection.destroy().catch(() => undefined);

      throw new Error('runtime: lane was retired while reconnecting');
    }

    return connection;
  }

  /**
   * A lane owns one connection at a time. A runaway or timed-out session is destroyed — never
   * returned to a pool — *before* its replacement is acquired, so a one-client pool still works.
   */
  async function openLane(deadlineAt: number): Promise<Lane> {
    let connection: LaneConnection | null = await acquire(deadlineAt);
    let generation = 0;
    let retired = false;

    const discard = async () => {
      const stale = connection;
      connection = null;
      generation++;

      if (stale) {
        // Destroying may wait on a runaway query; bound it, the session is unusable either way.
        await settledWithin(stale.destroy(), DESTROY_WAIT_MS);
      }
    };

    const lane: Lane = {
      get db(): OutboxDb {
        if (!connection) {
          throw new Error('runtime: lane has no connection');
        }

        return connection.db;
      },
      get usable() {
        return connection !== null && !retired;
      },
      get broken() {
        return connection?.isBroken?.() === true;
      },
      ready: async (readyBy) => {
        if (retired) {
          return false;
        }

        // A session that died while idle would fail the claim: replace it first.
        if (connection?.isBroken?.() === true) {
          await discard();
        }

        if (connection) {
          return true;
        }

        const mine = generation;

        try {
          connection = await acquire(readyBy, () => !retired && generation === mine);

          return true;
        } catch (error) {
          emit({
            type: 'infra_error',
            operation: 'run_job',
            dbCode: sqlstateOf(error),
            transient: isTransientDbError(error),
            error: serializeError(error),
          });

          return false;
        }
      },
      replace: async () => {
        await discard();
        const mine = generation;
        // Inside a settlement, whose watchdog bounds this acquisition.
        const fresh = await config.connect();

        // Retired (or replaced) while this connect was in flight: the late session is not ours.
        if (retired || generation !== mine) {
          void fresh.destroy().catch(() => undefined);

          throw new Error('runtime: lane was retired while reconnecting');
        }

        connection = fresh;
      },
      retire: async () => {
        retired = true;
        await discard();
      },
      bounded: async (work, until, what) => {
        const running = Promise.resolve().then(work);
        const result = await untilDeadline(clock, running, until);

        if (!result.done) {
          // Abandoning a query and returning its session to a pool is unsafe: destroy it.
          await lane.retire();
        }

        return unwrap(result, what);
      },
      close: async () => {
        const open = connection;
        connection = null;

        if (!open) {
          return;
        }

        const closed = await settledWithin(open.close(), CLOSE_WAIT_MS);

        if (!closed.done) {
          void open.destroy().catch(() => undefined);
        }
      },
    };

    return lane;
  }

  /**
   * One absolute deadline for a settlement: shared by its retries, reconnects, fallbacks and
   * read-back, never reset. It never runs past the lease (less a safety margin).
   */
  function startSettlement(leaseEndsAtMs?: number, invocationDeadlineAt?: number): Settlement {
    const deadlineAt = Math.min(
      clock.nowMs() + config.settleRetry.budgetMs,
      leaseEndsAtMs === undefined ? Infinity : leaseEndsAtMs - SETTLE_SAFETY_MS,
      invocationDeadlineAt ?? Infinity,
    );

    return { deadlineAt, abandoned: false };
  }

  const remainingOf = (settlement: Settlement) =>
    settlement.abandoned ? 0 : settlement.deadlineAt - clock.nowMs();

  async function runClaimed(
    lane: Lane,
    claimed: ClaimedJob,
    report: RunReport,
    trigger: Trigger,
    claimedAtMs: number,
    deadlineAt: number,
  ) {
    tally(report, claimed.kind, 'claimed');
    const job = registry.get(claimed.kind);

    if (!job) {
      throw new Error(`runtime: claimed unregistered kind ${claimed.kind}`);
    }

    const leaseEndsAtMs = claimedAtMs + job.leaseMs;
    const startedAtMs = clock.nowMs();
    // Fit was decided before the claim's await: a slow reply can leave too little lease (or
    // invocation) to run and settle. Never start user code then — its side effects could
    // overlap recovery's retry. The attempt closes as claim_late, charging nothing.
    const needMs = job.timeoutMs + job.graceMs + MIN_SETTLE_WINDOW_MS;

    if (leaseEndsAtMs - startedAtMs < needMs || deadlineAt - startedAtMs < needMs) {
      emit({
        type: 'claim_late',
        jobId: claimed.id,
        kind: claimed.kind,
        attemptNo: claimed.attemptNo,
        claimLatencyMs: startedAtMs - claimedAtMs,
      });

      // The lease and the window may both be spent already; the settle is fenced by the lease
      // token (recovery taking over yields lease_lost), so it gets a short window of its own.
      await settle(
        lane,
        job,
        claimed,
        { type: 'claim_late' },
        report,
        { thrown: false, recovered: false, trigger },
        { deadlineAt: clock.nowMs() + CLAIM_LATE_SETTLE_MS, abandoned: false },
      );

      return;
    }

    const execution = await executeHandler({
      job,
      claimed,
      db: lane.db,
      deps: config.deps,
      clock,
    });

    if (execution.thrown !== undefined) {
      emit({
        type: 'handler_threw',
        jobId: claimed.id,
        kind: claimed.kind,
        attemptNo: claimed.attemptNo,
        error: serializeError(execution.thrown),
      });
    }

    if (execution.outcome.type === 'timeout') {
      emit({
        type: 'handler_timeout',
        jobId: claimed.id,
        kind: claimed.kind,
        attemptNo: claimed.attemptNo,
        timeoutMs: job.timeoutMs,
      });
    }

    await settle(
      lane,
      job,
      claimed,
      execution.outcome,
      report,
      { thrown: execution.thrown !== undefined, recovered: false, trigger },
      startSettlement(leaseEndsAtMs, deadlineAt),
      // The runaway's reconnect is part of the settlement, so the watchdog bounds it too.
      execution.runaway ? () => lane.replace() : undefined,
    );
  }

  /**
   * Native timeouts bound SQL; this watchdog bounds everything else a settlement waits on —
   * connection acquisition, hooks and commit callbacks between statements. When it fires the
   * outcome is unknown (a timeout doesn't prove rollback): the lane's session is destroyed, the
   * lane retires, and recovery resolves the job through the fenced settle path.
   */
  async function settle(
    lane: Lane,
    job: AnyJob,
    claimed: ClaimedJob,
    outcome: SettleOutcome,
    report: RunReport,
    context: SettleContext,
    settlement: Settlement,
    before?: () => Promise<void>,
  ) {
    const work = (async () => {
      if (before) {
        await before();
      }

      return persistOutcome(lane, job, claimed, outcome, context, settlement);
    })();
    // A late rejection of abandoned work must not surface as unhandled.
    work.catch(() => undefined);

    // A settlement whose deadline already passed still gets the slack to report settle_failed
    // itself; the watchdog is for work that can't return, not for work that's merely late.
    const raced = await untilDeadline(
      clock,
      work,
      Math.max(settlement.deadlineAt, clock.nowMs()) + WATCHDOG_SLACK_MS,
    );

    if (!raced.done) {
      settlement.abandoned = true;
      tally(report, claimed.kind, 'settleFailed');
      emit({
        type: 'settle_failed',
        jobId: claimed.id,
        kind: claimed.kind,
        attemptNo: claimed.attemptNo,
        dbCode: null,
        transient: true,
        error: serializeError(new Error('settlement deadline exceeded; outcome left to recovery')),
      });
      await lane.retire();

      return;
    }

    if (raced.error !== undefined) {
      throw raced.error;
    }

    const settled = raced.value!;

    return afterSettle(
      job,
      claimed,
      settled.transition,
      settled.result,
      report,
      settled.context,
      lane,
      settlement,
    );
  }

  // Settle failures retry the SQL, never the handler; a settle that keeps failing is left to recovery.
  async function persistOutcome(
    lane: Lane,
    job: AnyJob,
    claimed: ClaimedJob,
    outcome: SettleOutcome,
    context: SettleContext,
    settlement: Settlement,
  ): Promise<{ transition: Transition; result: PersistResult; context: SettleContext }> {
    const budget: JobBudgetState = {
      id: claimed.id,
      spentAttempts: claimed.spentAttempts,
      codeStreak: claimed.codeStreak,
      lastErrorCode: claimed.lastErrorCode,
      stickyCode: claimed.stickyCode ?? null,
    };

    const transition = sanitize(
      decideTransition({
        outcome,
        job: budget,
        policy: job.retry,
        timing: { graceMs: job.graceMs },
      }),
    );

    const commit = outcome.type === 'ok' ? outcome.commit : undefined;
    let commitError: unknown;
    const applied = await persist(lane, claimed, transition, settlement, {
      hooked: true,
      requireExpiredLease: context.recovered,
      ...(commit ? { commit, onCommitError: (error: unknown) => (commitError = error) } : {}),
    });

    if (applied === 'commit_failed') {
      // The commit is handler code: its error is classified like a handler throw.
      emitWhileLive(settlement, {
        type: 'handler_threw',
        jobId: claimed.id,
        kind: claimed.kind,
        attemptNo: claimed.attemptNo,
        error: serializeError(commitError),
      });

      return persistOutcome(
        lane,
        job,
        claimed,
        classifySafely(job, commitError),
        {
          ...context,
          thrown: true,
        },
        settlement,
      );
    }

    if (applied === 'hook_failed') {
      // The fallback settles without the hook, so a hook that always throws ends dead, not looping.
      const fallback = sanitize(
        decideTransition({
          outcome: retry({ code: HOOK_FAILED_CODE }),
          job: budget,
          policy: job.retry,
          timing: { graceMs: job.graceMs },
        }),
      );
      const result = await persist(lane, claimed, fallback, settlement, {
        hooked: false,
        requireExpiredLease: context.recovered,
      });

      return { transition: fallback, result, context: { ...context, thrown: true } };
    }

    return { transition, result: applied, context };
  }

  function emitWhileLive(settlement: Settlement, event: OutboxEvent) {
    // Abandoned work may still unwind; the watchdog already reported its settlement.
    if (!settlement.abandoned) {
      emit(event);
    }
  }

  function sanitize(transition: Transition): Transition {
    if ('code' in transition && transition.message !== null) {
      return { ...transition, message: sanitizeErrorMessage(transition.message, config.redact) };
    }

    return transition;
  }

  async function persist(
    lane: Lane,
    claimed: ClaimedJob,
    transition: Transition,
    settlement: Settlement,
    options: {
      hooked: boolean;
      requireExpiredLease: boolean;
      commit?: (tx: OutboxDb) => Promise<void>;
      onCommitError?: (error: unknown) => void;
    },
  ): Promise<PersistResult> {
    const hook = options.hooked ? config.onTransition : undefined;
    const { commit } = options;
    const fenced = hook !== undefined || commit !== undefined;
    const settleInput = {
      jobId: claimed.id,
      leaseToken: claimed.leaseToken,
      transition,
      now: clock.dbNow(),
      requireExpiredLease: options.requireExpiredLease,
    };

    const deadlineFailure = () => {
      emitWhileLive(settlement, {
        type: 'settle_failed',
        jobId: claimed.id,
        kind: claimed.kind,
        attemptNo: claimed.attemptNo,
        dbCode: null,
        transient: true,
        error: serializeError(new Error('settlement deadline exceeded; outcome left to recovery')),
      });

      return 'settle_failed' as const;
    };

    // A dropped connection or a timeout leaves the write's fate unknown: neither proves rollback,
    // so a later lease_lost may be our own commit.
    let ambiguous = false;
    const resolve = async (result: 'applied' | 'lease_lost') => {
      if (result === 'lease_lost' && ambiguous) {
        if (remainingOf(settlement) <= 0) {
          return deadlineFailure();
        }

        const landed = await readAttemptOutcome(lane.db, settleInput);

        return landed === transition.attemptOutcome ? 'applied' : 'lease_lost';
      }

      return result;
    };

    for (let attempt = 0; ; attempt++) {
      const remaining = remainingOf(settlement);

      if (remaining <= 0) {
        return deadlineFailure();
      }

      try {
        // The session died while the handler ran (nothing was sent on it): settle on a fresh one.
        if (lane.broken) {
          await lane.replace();
        }

        // Every settlement is one transaction under native timeouts derived from what is left of
        // the shared deadline: a stuck lock, statement or idle callback can't outlive it.
        const result = await lane.db.transaction(async (tx) => {
          await setSettlementTimeouts(tx, {
            transactionMs: remaining,
            statementMs: remaining,
            lockMs: Math.min(config.settleRetry.lockTimeoutMs, remaining),
          });

          // App rows (hook) before the job row (settle), the order business transactions take:
          // settling first deadlocks with an enqueue whose ON CONFLICT waits on the job-row write.
          if (fenced && !(await isLeaseHeld(tx, settleInput))) {
            return 'lease_lost' as const;
          }

          if (commit) {
            try {
              await commit(tx);
            } catch (error) {
              throw new CommitError(error);
            }
          }

          if (hook) {
            try {
              await hook(tx, { job: claimed, transition });
            } catch (error) {
              throw new HookError(error);
            }
          }

          const settled = await settleJob(tx, settleInput);

          if (settled === 'lease_lost' && fenced) {
            // Lost between the check and the write: roll the hook's writes back too.
            throw new LeaseLostRollback();
          }

          return settled;
        });

        return await resolve(result);
      } catch (error) {
        if (error instanceof LeaseLostRollback) {
          return resolve('lease_lost');
        }

        const fault =
          error instanceof HookError || error instanceof CommitError ? error.original : error;
        const transient = isTransientDbError(fault);

        if (transient && attempt < config.settleRetry.attempts) {
          if (isConnectionError(fault) || isTimeoutError(fault)) {
            ambiguous = true;
          }

          if (isConnectionError(fault)) {
            await lane.replace();
          }

          if (settlement.abandoned) {
            return 'settle_failed';
          }

          emit({
            type: 'settle_retried',
            jobId: claimed.id,
            kind: claimed.kind,
            dbCode: sqlstateOf(fault),
            retry: attempt + 1,
          });
          const base = config.settleRetry.delaysMs[attempt] ?? 8_000;
          // Up to +25% jitter so lanes that failed together don't retry together, never past the deadline.
          const delay = Math.round(base * (1 + Math.random() * 0.25));
          await clock.sleep(Math.max(0, Math.min(delay, remainingOf(settlement))));
          continue;
        }

        if (error instanceof CommitError) {
          options.onCommitError?.(fault);

          return 'commit_failed';
        }

        if (error instanceof HookError) {
          emitWhileLive(settlement, {
            type: 'hook_failed',
            jobId: claimed.id,
            kind: claimed.kind,
            to: transition.to,
            error: serializeError(fault),
          });

          return 'hook_failed';
        }

        emitWhileLive(settlement, {
          type: 'settle_failed',
          jobId: claimed.id,
          kind: claimed.kind,
          attemptNo: claimed.attemptNo,
          dbCode: sqlstateOf(fault),
          transient,
          error: serializeError(fault),
        });

        return 'settle_failed';
      }
    }
  }

  async function afterSettle(
    job: AnyJob,
    claimed: ClaimedJob,
    transition: Transition,
    result: PersistResult,
    report: RunReport,
    context: SettleContext,
    lane: Lane,
    settlement: Settlement,
  ) {
    // commit_failed can't reach here: persistOutcome resolves it by classifying the error.
    if (result === 'settle_failed' || result === 'hook_failed' || result === 'commit_failed') {
      tally(report, claimed.kind, 'settleFailed');

      return;
    }

    if (result === 'lease_lost') {
      tally(report, claimed.kind, 'leaseLost');
      emit({
        type: 'lease_lost',
        jobId: claimed.id,
        kind: claimed.kind,
        attemptNo: claimed.attemptNo,
      });

      return;
    }

    tally(report, claimed.kind, transition.to);
    emit(transitionEvent(job, claimed, transition, context));

    if (
      (transition.to === 'retrying' || transition.to === 'dead') &&
      config.warnAtStreak !== undefined &&
      transition.codeStreak === config.warnAtStreak
    ) {
      emit({
        type: 'attempt_threshold',
        jobId: claimed.id,
        kind: claimed.kind,
        code: transition.code,
        codeStreak: transition.codeStreak,
        threshold: config.warnAtStreak,
      });
    }

    if (transition.to === 'retrying') {
      // The lane is idle again: its own session records the nudge, and the send shares the deadline.
      await nudgeJobs(
        [{ id: claimed.id, kind: claimed.kind }],
        Math.ceil(transition.delayMs / 1_000),
        {
          ...(lane.usable ? { lane } : {}),
          deadlineAt: settlement.deadlineAt,
        },
      );
    }
  }

  function transitionEvent(
    job: AnyJob,
    claimed: ClaimedJob,
    transition: Transition,
    context: SettleContext,
  ): OutboxEvent {
    const failing = 'code' in transition;
    const charged = transition.attemptOutcome === 'lease_expired' ? 0 : 1;

    return {
      type: 'transition',
      severity:
        transition.to === 'dead'
          ? 'error'
          : transition.to === 'retrying' && context.thrown
            ? 'warn'
            : 'info',
      jobId: claimed.id,
      kind: claimed.kind,
      tenantKey: claimed.tenantKey,
      correlationId: claimed.correlationId,
      attemptNo: claimed.attemptNo,
      to: transition.to,
      reason: transition.to === 'dead' || transition.to === 'cancelled' ? transition.reason : null,
      code: failing ? transition.code : null,
      codeStreak: failing ? transition.codeStreak : 0,
      attemptsUsed: claimed.spentAttempts + charged,
      maxAttempts: job.retry.maxAttempts,
      delayMs: transition.to === 'retrying' ? transition.delayMs : null,
      recovered: context.recovered,
      trigger: context.trigger,
    };
  }

  // Best effort: the cron sweep is the backstop, so a failure is an event, never a throw.
  async function nudgeJobs(
    jobs: readonly { id: string; kind?: string }[],
    delaySeconds = 0,
    via: NudgeVia = {},
  ) {
    const byLane = new Map<string, string[]>();

    for (const job of jobs) {
      const lane = job.kind === undefined ? 'default' : (registry.laneOf(job.kind) ?? 'default');
      const ids = byLane.get(lane) ?? [];
      ids.push(job.id);
      byLane.set(lane, ids);
    }

    for (const [lane, ids] of byLane) {
      await nudgeLane(lane, ids, delaySeconds, via);
    }
  }

  async function nudgeLane(
    lane: string,
    ids: readonly string[],
    delaySeconds: number,
    via: NudgeVia = {},
  ) {
    if (ids.length === 0) {
      return;
    }

    // A non-finite delay sends now rather than dropping the nudge (an early nudge is a harmless miss).
    const delay = Number.isFinite(delaySeconds)
      ? Math.min(MAX_QUEUE_DELAY_SECONDS, Math.max(0, Math.ceil(delaySeconds)))
      : 0;
    const queue = config.queues[lane];

    if (!queue) {
      emit({ type: 'nudge_skipped', ids: [...ids], lane, reason: 'no_queue' });
      await stampNudges(ids, `no_queue: lane ${lane} has no queue`, via);

      return;
    }

    for (let i = 0; i < ids.length; i += MAX_IDS_PER_NUDGE) {
      const chunk = ids.slice(i, i + MAX_IDS_PER_NUDGE);
      const message: OutboxMessage = { v: 1, type: 'nudge', ids: chunk };

      try {
        // A queue send is not SQL: no database timeout bounds it, so the runtime does.
        const limitMs =
          via.deadlineAt === undefined
            ? QUEUE_SEND_TIMEOUT_MS
            : Math.min(QUEUE_SEND_TIMEOUT_MS, via.deadlineAt - clock.nowMs());

        if (limitMs <= 0) {
          throw new Error('settlement deadline exceeded before the nudge was sent');
        }

        const sent = await untilDeadline(
          clock,
          queue.send(message, delay > 0 ? { delaySeconds: delay } : undefined),
          clock.nowMs() + limitMs,
        );

        if (!sent.done) {
          throw new Error(`queue send exceeded its ${limitMs}ms deadline`);
        }

        if (sent.error !== undefined) {
          throw sent.error;
        }

        emit({ type: 'nudge_sent', ids: chunk, lane, delaySeconds: delay });
        await stampNudges(chunk, null, via);
      } catch (error) {
        emit({ type: 'nudge_failed', ids: chunk, lane, error: serializeError(error) });
        await stampNudges(chunk, error instanceof Error ? error.message : String(error), via);
      }
    }
  }

  /** On `db` when the caller holds an idle connection: a second checkout can deadlock a full pool. */
  async function stampNudges(ids: readonly string[], error: string | null, via: NudgeVia = {}) {
    if (!config.recordNudges && !via.record) {
      return;
    }

    const deadlineAt = via.deadlineAt ?? clock.nowMs() + DEFAULT_PHASE_DEADLINE_MS;

    const record = (runner: SqlRunner) =>
      recordNudgeResult(runner, {
        ids,
        error: error === null ? null : sanitizeErrorMessage(error, config.redact),
        now: clock.dbNow(),
      });

    try {
      if (via.lane?.usable) {
        // The settlement's transaction has ended: its timeouts no longer apply, so bound it here.
        await via.lane.bounded(() => record(via.lane!.db), deadlineAt, 'nudge bookkeeping');
      } else if (via.db) {
        // The caller owns and bounds this connection (an operator action).
        await record(via.db);
      } else {
        await withLanes(
          1,
          (lane) => lane.bounded(() => record(lane.db), deadlineAt, 'nudge bookkeeping'),
          deadlineAt,
        );
      }
    } catch (stampError) {
      emit({
        type: 'infra_error',
        operation: 'record_nudge',
        dbCode: sqlstateOf(stampError),
        transient: isTransientDbError(stampError),
        error: serializeError(stampError),
      });
    }
  }

  async function processNudge(
    ids: readonly string[],
    budget: Budget,
    scope: Scope = {},
    options: { deadlineAt?: number } = {},
  ) {
    const report = emptyReport();
    const unique = [...new Set(ids)];
    const started = clock.nowMs();
    // The budget's own window, and never past the invocation's.
    const deadlineAt = Math.min(started + budget.deadlineMs, options.deadlineAt ?? Infinity);
    const laneCount = Math.min(config.lanes, unique.length);

    if (unique.length === 0) {
      return report;
    }

    const renudge: string[] = [];
    // Reserved before the claim's await, so parallel lanes can't overshoot maxJobs.
    let reserved = 0;

    await withLanes(
      laneCount,
      async (lane, index) => {
        const mine = unique.filter((_, i) => i % laneCount === index);

        for (const id of mine) {
          if (reserved >= budget.maxJobs) {
            renudge.push(id);
            continue;
          }

          // Reserved synchronously, before any await, so parallel lanes can't overshoot maxJobs.
          reserved++;

          // A retired lane (settlement watchdog) hands its share to the next invocation.
          if (!(await lane.ready(deadlineAt))) {
            reserved--;
            renudge.push(id);
            continue;
          }

          const remaining = deadlineAt - clock.nowMs();
          const claimedAtMs = clock.nowMs();
          const [result] = await lane.bounded(
            () =>
              claimByIds(lane.db, {
                ...gate,
                ids: [id],
                kinds: kindFilter(remaining, scope),
                leaseToken: uuidv7(),
                now: clock.dbNow(),
              }),
            deadlineAt,
            'claim',
          );

          if (!result) {
            reserved--;
            continue;
          }

          if (result.result !== 'claimed') {
            reserved--;
            emit({ type: 'nudge_miss', jobId: id, result: result.result });

            if (
              result.result === 'over_budget' ||
              result.result === 'locked' ||
              result.result === 'newer_version'
            ) {
              renudge.push(id);
            }

            continue;
          }

          await runClaimed(lane, result.job, report, 'nudge', claimedAtMs, deadlineAt);
        }
      },
      deadlineAt,
    );

    await nudgeLane(scope.lane ?? 'default', renudge, RENUDGE_DELAY_SECONDS, {
      ...(options.deadlineAt === undefined ? {} : { deadlineAt: options.deadlineAt }),
    });

    return report;
  }

  async function drain(
    budget: Budget,
    trigger: 'sweep' | 'drain',
    scope: Scope = {},
    options: { deadlineAt?: number } = {},
  ) {
    const report = emptyReport();
    const started = clock.nowMs();
    // The budget's own window, and never past the invocation's.
    const deadlineAt = Math.min(started + budget.deadlineMs, options.deadlineAt ?? Infinity);
    let reserved = 0;

    await withLanes(
      config.lanes,
      async (lane) => {
        for (;;) {
          // Awaited first: from the budget checks to `reserved++` nothing may yield to another lane.
          if (!(await lane.ready(deadlineAt))) {
            return;
          }

          const remaining = deadlineAt - clock.nowMs();

          if (reserved >= budget.maxJobs) {
            report.reachedLimit = true;

            return;
          }

          if (remaining <= 0) {
            report.reachedDeadline = true;

            return;
          }

          const kinds = kindFilter(remaining, scope);

          if (kinds.length > 0 && !kinds.some((k) => k.fits)) {
            report.reachedDeadline = true;

            return;
          }

          reserved++;
          const claimedAtMs = clock.nowMs();
          const [claimed] = await lane.bounded(
            () =>
              claimDue(lane.db, {
                ...gate,
                kinds,
                limit: 1,
                trigger,
                leaseToken: uuidv7(),
                now: clock.dbNow(),
              }),
            deadlineAt,
            'claim',
          );

          if (!claimed) {
            reserved--;

            return;
          }

          try {
            await runClaimed(lane, claimed, report, trigger, claimedAtMs, deadlineAt);
          } catch (error) {
            recordError(report, error);
            emit({
              type: 'infra_error',
              operation: 'run_job',
              dbCode: sqlstateOf(error),
              transient: isTransientDbError(error),
              error: serializeError(error),
            });
          }
        }
      },
      deadlineAt,
    );

    return report;
  }

  // Long kinds get a durable path through the queue (15 min budget) even when every nudge is lost.
  async function handoff(budgetMs: number, limit = 100, options: { deadlineAt?: number } = {}) {
    const deadlineAt = options.deadlineAt ?? clock.nowMs() + DEFAULT_PHASE_DEADLINE_MS;
    let handed = 0;

    for (const laneName of registry.lanes()) {
      const long = kindFilter(budgetMs, { lane: laneName })
        .filter((k) => !k.fits)
        .map((k) => k.kind);

      if (long.length === 0) {
        continue;
      }

      const [ids = []] = await withLanes(
        1,
        (lane) =>
          lane.bounded(
            () =>
              listDueJobIds(lane.db, {
                kinds: long,
                limit,
                now: clock.dbNow(),
                nudgeCooldownMs: HANDOFF_COOLDOWN_MS,
                ...gate,
              }),
            deadlineAt,
            'handoff',
          ),
        deadlineAt,
      );

      await nudgeLane(laneName, ids, 0, { deadlineAt, record: true });
      handed += ids.length;
    }

    return handed;
  }

  // Only this outbox's kinds: another app's jobs are its own sweep's business.
  // Count-limited *and* time-limited: many slow-but-completing settlements can't eat the invocation.
  async function recover(limit: number, options: { deadlineAt?: number } = {}) {
    const report = emptyReport();
    const kinds = registry.claimable().map((k) => k.kind);
    const deadlineAt = options.deadlineAt ?? clock.nowMs() + DEFAULT_RECOVERY_BUDGET_MS;

    await withLanes(
      1,
      async (lane) => {
        const expired = await lane.bounded(
          () => listExpiredLeases(lane.db, { limit, now: clock.dbNow(), kinds }),
          deadlineAt,
          'recovery listing',
        );

        for (const lease of expired) {
          const job = registry.get(lease.kind);

          // The rest stay expired and durable: the next sweep recovers them.
          if (deadlineAt - clock.nowMs() < MIN_SETTLE_WINDOW_MS) {
            report.reachedDeadline = true;

            return;
          }

          if (!(await lane.ready(deadlineAt))) {
            return;
          }

          if (!job) {
            continue;
          }

          const claimed: ClaimedJob = {
            id: lease.id,
            kind: lease.kind,
            payload: null,
            tenantKey: lease.tenantKey,
            correlationId: lease.correlationId,
            metadata: lease.metadata,
            leaseToken: lease.leaseToken,
            attemptNo: lease.attemptNo,
            spentAttempts: lease.spentAttempts,
            codeStreak: lease.codeStreak,
            lastErrorCode: lease.lastErrorCode,
            stickyCode: lease.stickyCode ?? null,
          };

          // The lease already expired: the settlement is capped by recovery's deadline.
          await settle(
            lane,
            job,
            claimed,
            { type: 'lease_expired' },
            report,
            { thrown: false, recovered: true, trigger: 'recovery' },
            startSettlement(undefined, deadlineAt),
          );
        }
      },
      deadlineAt,
    );

    return report;
  }

  async function cleanup(
    retention: {
      succeededRetentionMs: number;
      terminalRetentionMs: number;
      batch: number;
      maxBatches: number;
    },
    options: { deadlineAt?: number } = {},
  ) {
    const deadlineAt = options.deadlineAt ?? clock.nowMs() + DEFAULT_PHASE_DEADLINE_MS;
    let deleted = 0;

    await withLanes(
      1,
      async (lane) => {
        for (let i = 0; i < retention.maxBatches; i++) {
          const batch = await lane.bounded(
            () => deleteFinishedJobs(lane.db, { ...retention, now: clock.dbNow() }),
            deadlineAt,
            'cleanup',
          );
          deleted += batch;

          if (batch < retention.batch) {
            return;
          }
        }
      },
      deadlineAt,
    );

    emit({ type: 'cleanup', deleted });

    return deleted;
  }

  async function monitorBacklog(
    options: {
      agedMs?: number;
      thresholds?: Partial<BacklogThresholds>;
      lanes?: Readonly<Record<string, Partial<BacklogThresholds>>>;
      deadlineAt?: number;
    } = {},
  ) {
    const deadlineAt = options.deadlineAt ?? clock.nowMs() + DEFAULT_PHASE_DEADLINE_MS;
    const [counts = []] = await withLanes(
      1,
      (lane) =>
        lane.bounded(
          () =>
            countBacklog(lane.db, {
              agedMs: options.agedMs ?? DEFAULT_BACKLOG_AGED_MS,
              now: clock.dbNow(),
            }),
          deadlineAt,
          'backlog',
        ),
      deadlineAt,
    );
    const report = evaluateBacklog({
      counts,
      laneOf: (kind) => registry.laneOf(kind),
      ...(options.thresholds ? { thresholds: options.thresholds } : {}),
      ...(options.lanes ? { lanes: options.lanes } : {}),
    });

    emit({ type: 'backlog', report });

    return report;
  }

  async function hasBacklog(scope: Scope = {}, options: { deadlineAt?: number } = {}) {
    const deadlineAt = options.deadlineAt ?? clock.nowMs() + DEFAULT_PHASE_DEADLINE_MS;
    const [due] = await withLanes(
      1,
      (lane) =>
        lane.bounded(
          () =>
            hasDueJobs(lane.db, {
              ...gate,
              kinds: kindFilter(Number.POSITIVE_INFINITY, scope),
              now: clock.dbNow(),
            }),
          deadlineAt,
          'backlog check',
        ),
      deadlineAt,
    );

    return due ?? false;
  }

  async function continueDrain(lane = 'default', options: { deadlineAt?: number } = {}) {
    const queue = config.queues[lane];

    if (!queue) {
      emit({ type: 'continuation_skipped', lane, reason: 'no_queue' });

      return false;
    }

    const message: OutboxMessage = { v: 1, type: 'drain', lane };

    try {
      // Bounded like every targeted nudge: a queue send is not SQL, no database timeout applies.
      const deadlineAt = Math.min(
        options.deadlineAt ?? Infinity,
        clock.nowMs() + QUEUE_SEND_TIMEOUT_MS,
      );
      unwrap(await untilDeadline(clock, queue.send(message), deadlineAt), 'continuation send');
      emit({ type: 'continuation_sent', lane });

      return true;
    } catch (error) {
      emit({ type: 'continuation_failed', lane, error: serializeError(error) });

      return false;
    }
  }

  const nudge = (
    jobs: readonly (string | { id: string; kind: string })[],
    delaySeconds = 0,
    via: NudgeVia = {},
  ) =>
    nudgeJobs(
      jobs.map((job) => (typeof job === 'string' ? { id: job } : job)),
      delaySeconds,
      via,
    );

  return {
    emit,
    nudge,
    processNudge,
    drain,
    handoff,
    recover,
    cleanup,
    hasBacklog,
    monitorBacklog,
    continueDrain,
    lanes: () => registry.lanes(),
    nowMs: () => clock.nowMs(),
    sleep: (ms: number) => clock.sleep(ms),
  };
}

export type Runtime = ReturnType<typeof createRuntime>;
