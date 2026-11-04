/** Budgets count attempts since the last replay; crashes are charged only to `maxCrashes`. */
export interface RetryPolicy {
  /** Handled attempts including the first; `Infinity` never gives up. */
  readonly maxAttempts: number;
  readonly backoff: {
    readonly baseMs: number;
    readonly maxMs: number;
    readonly factor: number;
  };
  /** Consecutive same-code attempt caps; reaching one is exhaustion. */
  readonly perCode: Readonly<Record<string, number>>;
  /** Consecutive crashes before `dead(crash_loop)`. */
  readonly maxCrashes: number;
  readonly codes: Readonly<Record<string, CodeRule>>;
}

export interface CodeRule {
  /** Only ever widens the job's `maxAttempts`. */
  readonly maxAttempts?: number;
  /** This code's `maxAttempts` keeps applying to every later code until replay. */
  readonly sticky: boolean;
  /** Milliseconds. */
  readonly maxDelayMs?: number;
  readonly onExhaustion: 'dead' | 'cancel';
}

/** A number is a streak cap; an object adds `CodeRule` fields plus `streak`. */
export type CodeRuleInput =
  | number
  | {
      streak?: number;
      maxAttempts?: number;
      sticky?: boolean;
      maxDelayMs?: number;
      onExhaustion?: 'dead' | 'cancel';
    };

export interface RetryPolicyInput {
  maxAttempts?: number;
  backoff?: { baseMs?: number; maxMs?: number; factor?: number };
  /** Merged over the defaults; an object without `streak` keeps the default cap. */
  perCode?: Record<string, CodeRuleInput>;
  maxCrashes?: number;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxAttempts: 20,
  backoff: { baseMs: 30_000, maxMs: 3_600_000, factor: 2 },
  perCode: { timeout: 5, invalid_payload: 3 },
  maxCrashes: 5,
  codes: {},
};

const RESERVED_CRASH_CODE = 'lease_expired';

/** Queues can't delay past a day. */
export const MAX_RETRY_AFTER_MS = 86_400_000;

export function resolveRetryPolicy(input: RetryPolicyInput = {}): RetryPolicy {
  const { perCode, codes } = splitPerCode(input.perCode ?? {});
  const policy: RetryPolicy = {
    maxAttempts: input.maxAttempts ?? DEFAULT_RETRY_POLICY.maxAttempts,
    backoff: {
      baseMs: input.backoff?.baseMs ?? DEFAULT_RETRY_POLICY.backoff.baseMs,
      maxMs: input.backoff?.maxMs ?? DEFAULT_RETRY_POLICY.backoff.maxMs,
      factor: input.backoff?.factor ?? DEFAULT_RETRY_POLICY.backoff.factor,
    },
    perCode: { ...DEFAULT_RETRY_POLICY.perCode, ...perCode },
    maxCrashes: input.maxCrashes ?? DEFAULT_RETRY_POLICY.maxCrashes,
    codes,
  };

  assertAttemptBudget('retry.maxAttempts', policy.maxAttempts);
  assertPositiveInteger('retry.maxCrashes', policy.maxCrashes);
  assertPositiveInteger('retry.backoff.baseMs', policy.backoff.baseMs);
  assertPositiveInteger('retry.backoff.maxMs', policy.backoff.maxMs);

  if (policy.backoff.maxMs < policy.backoff.baseMs) {
    throw new Error('retry.backoff.maxMs must be >= retry.backoff.baseMs');
  }

  if (policy.backoff.maxMs > MAX_RETRY_AFTER_MS) {
    throw new Error(`retry.backoff.maxMs must be <= ${MAX_RETRY_AFTER_MS} (24h)`);
  }

  if (!Number.isFinite(policy.backoff.factor) || policy.backoff.factor < 1) {
    throw new Error('retry.backoff.factor must be a finite number >= 1');
  }

  for (const [code, cap] of Object.entries(policy.perCode)) {
    assertPositiveInteger(`retry.perCode.${code}`, cap);
  }

  for (const [code, rule] of Object.entries(policy.codes)) {
    if (rule.maxAttempts !== undefined) {
      assertAttemptBudget(`retry.perCode.${code}.maxAttempts`, rule.maxAttempts);
    }

    if (rule.maxDelayMs !== undefined) {
      assertPositiveInteger(`retry.perCode.${code}.maxDelayMs`, rule.maxDelayMs);
    }

    if (rule.sticky && rule.maxAttempts === undefined) {
      throw new Error(`retry.perCode.${code}: sticky needs a maxAttempts to carry`);
    }
  }

  return policy;
}

function splitPerCode(input: Record<string, CodeRuleInput>) {
  const perCode: Record<string, number> = {};
  const codes: Record<string, CodeRule> = {};

  for (const [code, entry] of Object.entries(input)) {
    if (code === RESERVED_CRASH_CODE) {
      throw new Error(`retry.perCode.${code}: crashes are budgeted by retry.maxCrashes`);
    }

    if (typeof entry === 'number') {
      perCode[code] = entry;
      continue;
    }

    if (entry.streak !== undefined) {
      perCode[code] = entry.streak;
    }

    codes[code] = {
      ...(entry.maxAttempts === undefined ? {} : { maxAttempts: entry.maxAttempts }),
      ...(entry.maxDelayMs === undefined ? {} : { maxDelayMs: entry.maxDelayMs }),
      sticky: entry.sticky ?? false,
      onExhaustion: entry.onExhaustion ?? 'dead',
    };
  }

  return { perCode, codes };
}

function assertAttemptBudget(name: string, value: number) {
  if (value !== Number.POSITIVE_INFINITY) {
    assertPositiveInteger(name, value);
  }
}

function assertPositiveInteger(name: string, value: number) {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer, got ${value}`);
  }
}
