import type { AttemptOutcome } from './status';

import { computeBackoff } from './backoff';
import {
  CLAIM_LATE_CODE,
  CLAIM_LATE_DELAY_MS,
  LEASE_EXPIRED_CODE,
  TIMEOUT_CODE,
  type SettleOutcome,
} from './outcome';
import { MAX_RETRY_AFTER_MS, type RetryPolicy } from './retry-policy';

/** Open: values may grow in minor versions. */
export type DeadReason = 'failed' | 'exhausted' | 'crash_loop' | (string & {});

export interface JobBudgetState {
  readonly id: string;
  /** Excludes the attempt being settled. */
  readonly spentAttempts: number;
  readonly codeStreak: number;
  readonly lastErrorCode: string | null;
  readonly stickyCode?: string | null;
}

export interface TransitionTiming {
  readonly graceMs: number;
}

/** Delays are relative to the DB clock. Absent `stickyCode` keeps the row's episode. */
export type Transition =
  | { to: 'succeeded'; attemptOutcome: 'succeeded' }
  | { to: 'cancelled'; reason: string; attemptOutcome: 'cancelled' }
  | {
      to: 'cancelled';
      reason: 'exhausted';
      code: string;
      message: string | null;
      codeStreak: number;
      stickyCode?: string;
      attemptOutcome: Extract<AttemptOutcome, 'retry' | 'timeout'>;
    }
  | {
      to: 'retrying';
      delayMs: number;
      code: string;
      message: string | null;
      codeStreak: number;
      stickyCode?: string;
      attemptOutcome: Extract<AttemptOutcome, 'retry' | 'timeout' | 'lease_expired'>;
    }
  | {
      to: 'dead';
      reason: DeadReason;
      code: string;
      message: string | null;
      codeStreak: number;
      stickyCode?: string;
      attemptOutcome: Extract<AttemptOutcome, 'failed' | 'retry' | 'timeout' | 'lease_expired'>;
    };

export function decideTransition(input: {
  outcome: SettleOutcome;
  job: JobBudgetState;
  policy: RetryPolicy;
  timing: TransitionTiming;
}): Transition {
  const { outcome, job, policy, timing } = input;

  switch (outcome.type) {
    case 'ok':
      return { to: 'succeeded', attemptOutcome: 'succeeded' };

    case 'cancel':
      return { to: 'cancelled', reason: outcome.reason, attemptOutcome: 'cancelled' };

    case 'fail':
      return {
        to: 'dead',
        reason: 'failed',
        code: outcome.code,
        message: outcome.message ?? null,
        codeStreak: nextStreak(job, outcome.code),
        attemptOutcome: 'failed',
      };

    case 'retry':
      return decideRetry({
        job,
        policy,
        code: outcome.code,
        message: outcome.message ?? null,
        attemptOutcome: 'retry',
        afterMs: outcome.afterMs,
        minDelayMs: 0,
      });

    case 'timeout':
      return decideRetry({
        job,
        policy,
        code: TIMEOUT_CODE,
        message: null,
        attemptOutcome: 'timeout',
        afterMs: undefined,
        minDelayMs: timing.graceMs,
      });

    case 'claim_late':
      // The handler never ran: no attempt budget and no crash budget are spent, so a slow
      // database can't dead-letter a job. `lease_expired` keeps spent_attempts unchanged.
      return {
        to: 'retrying',
        delayMs: CLAIM_LATE_DELAY_MS,
        code: CLAIM_LATE_CODE,
        message: null,
        codeStreak: nextStreak(job, CLAIM_LATE_CODE),
        ...episode(job.stickyCode ?? null),
        attemptOutcome: 'lease_expired',
      };

    case 'lease_expired':
      return decideRetry({
        job,
        policy,
        code: LEASE_EXPIRED_CODE,
        message: null,
        attemptOutcome: 'lease_expired',
        afterMs: undefined,
        minDelayMs: 0,
      });
  }
}

function decideRetry(input: {
  job: JobBudgetState;
  policy: RetryPolicy;
  code: string;
  message: string | null;
  attemptOutcome: 'retry' | 'timeout' | 'lease_expired';
  afterMs: number | undefined;
  minDelayMs: number;
}): Transition {
  const { job, policy, code, message, attemptOutcome } = input;
  const codeStreak = nextStreak(job, code);

  // Crashes retry at once, spend no attempt budget, and leave the episode untouched.
  if (attemptOutcome === 'lease_expired' || code === LEASE_EXPIRED_CODE) {
    const kept = episode(job.stickyCode ?? null);

    if (codeStreak >= policy.maxCrashes) {
      return {
        to: 'dead',
        reason: 'crash_loop',
        code,
        message,
        codeStreak,
        ...kept,
        attemptOutcome,
      };
    }

    return { to: 'retrying', delayMs: 0, code, message, codeStreak, ...kept, attemptOutcome };
  }

  const rule = policy.codes[code];
  const stickyCode = job.stickyCode ?? (rule?.sticky ? code : null);
  const failure = { code, message, codeStreak, ...episode(stickyCode), attemptOutcome };

  const exhausted = (): Transition =>
    rule?.onExhaustion === 'cancel'
      ? { to: 'cancelled', reason: 'exhausted', ...failure }
      : { to: 'dead', reason: 'exhausted', ...failure };

  const codeCap = policy.perCode[code];

  if (codeCap !== undefined && codeStreak >= codeCap) {
    return exhausted();
  }

  const attemptsUsed = job.spentAttempts + 1;

  if (attemptsUsed >= attemptCeiling(policy, code, stickyCode)) {
    return exhausted();
  }

  // A non-finite `after` falls back to backoff: neither a day-long park nor a hot loop.
  const requested =
    input.afterMs === undefined || !Number.isFinite(input.afterMs)
      ? computeBackoff(policy, job.id, attemptsUsed)
      : Math.min(MAX_RETRY_AFTER_MS, Math.max(0, Math.round(input.afterMs)));
  const capped = Math.min(requested, rule?.maxDelayMs ?? Number.POSITIVE_INFINITY);

  return {
    to: 'retrying',
    // The grace floor beats any ceiling: never overlap a timed-out handler still winding down.
    delayMs: Math.max(capped, input.minDelayMs),
    ...failure,
  };
}

function attemptCeiling(policy: RetryPolicy, code: string, stickyCode: string | null) {
  const own = policy.codes[code]?.maxAttempts ?? policy.maxAttempts;
  const carried =
    stickyCode === null ? policy.maxAttempts : (policy.codes[stickyCode]?.maxAttempts ?? 0);

  // Codes and episodes only widen `maxAttempts`; `streak` caps are how a code narrows it.
  return Math.max(policy.maxAttempts, own, carried);
}

function episode(stickyCode: string | null) {
  return stickyCode === null ? {} : { stickyCode };
}

function nextStreak(job: JobBudgetState, code: string) {
  return job.lastErrorCode === code ? job.codeStreak + 1 : 1;
}
