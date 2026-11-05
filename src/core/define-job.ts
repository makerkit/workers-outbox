import type { StandardSchemaV1 } from '@standard-schema/spec';

import type { OutboxDb } from '../runtime/types';

import { retry, type HandlerOutcome } from './outcome';
import { resolveRetryPolicy, type RetryPolicy, type RetryPolicyInput } from './retry-policy';

export const MAX_TIMEOUT_MS = 600_000;

export const HANDLER_GRACE_MS = 5_000;

export const LEASE_MARGIN_MS = 30_000;

/** The longest lease any legal job can hold. */
export const MAX_LEASE_MS = MAX_TIMEOUT_MS + HANDLER_GRACE_MS + LEASE_MARGIN_MS;

const KIND_PATTERN = /^[a-z0-9][a-z0-9._:-]{0,199}$/;

const LANE_PATTERN = /^[a-z0-9][a-z0-9._:-]{0,49}$/;

export const DEFAULT_LANE = 'default';

export interface JobContext<Deps = unknown> {
  readonly jobId: string;
  readonly kind: string;
  /** 1-based over the job's whole life. */
  readonly attempt: number;
  /** Stable across attempts; delivery is at-least-once, so pass it to providers that dedupe. */
  readonly idempotencyKey: string;
  /**
   * Aborts once the attempt has been settled as `timeout`, to stop abandoned I/O: pass it to every
   * I/O call. A result returned after the abort is discarded, so to stop early and still return
   * `ok()`, watch `deadline` / `remainingMs()` instead.
   */
  readonly signal: AbortSignal;
  /**
   * Epoch ms, on the outbox's clock, at which this attempt is settled as `timeout`: the attempt's
   * start plus `timeoutMs` (payload validation counts against it). Return before it, with room
   * for the work after your last check, and enqueue the rest as a continuation.
   */
  readonly deadline: number;
  /**
   * Milliseconds left until `deadline` on the outbox's clock, never below 0. A test clock moves it
   * only when advanced; the timeout itself is real time.
   */
  remainingMs(): number;
  /**
   * The lane's own session (the outbox's `connect`, its role and Hyperdrive config), not your
   * app's database. Fenced: once the attempt returns or is settled as `timeout`, every method call
   * on it, on a `tx` from its `transaction()`, or on a captured method rejects. A query builder
   * created before that and awaited after it is not covered. Typed as the minimal `OutboxDb`; the
   * object is whatever your connector's `db` is (e.g. a drizzle database). Never await an external
   * call inside `ctx.db.transaction()`; for writes that must commit with the job's completion, use
   * `ok({ commit })`.
   */
  readonly db: OutboxDb;
  readonly deps: Deps;
  readonly tenantKey: string | null;
  readonly correlationId: string | null;
  readonly metadata: Readonly<Record<string, unknown>>;
}

export interface JobDefinitionInput<K extends string, S extends StandardSchemaV1, D> {
  kind: K;
  payload: S;
  timeoutMs: number;
  lane?: string;
  retry?: RetryPolicyInput;
  /**
   * Maps a thrown error to an outcome. Return `undefined` to fall back to `defaultClassify` (retry
   * with code `unexpected`), which is also the default when `classify` is omitted.
   */
  classify?: (error: unknown) => HandlerOutcome | undefined;
  handle: (payload: StandardSchemaV1.InferOutput<S>, ctx: JobContext<D>) => Promise<HandlerOutcome>;
}

export interface JobDefinition<
  K extends string = string,
  S extends StandardSchemaV1 = StandardSchemaV1,
  D = unknown,
> {
  readonly kind: K;
  readonly payload: S;
  readonly timeoutMs: number;
  readonly lane: string;
  readonly graceMs: number;
  /** Derived: always outlives the handler plus its grace. */
  readonly leaseMs: number;
  readonly retry: RetryPolicy;
  readonly classify: (error: unknown) => HandlerOutcome;
  // Method syntax (bivariant) so every job fits `AnyJob`; the runtime only passes validated payloads.
  handle(payload: StandardSchemaV1.InferOutput<S>, ctx: JobContext<D>): Promise<HandlerOutcome>;
}

// oxlint-disable-next-line typescript/no-explicit-any -- registry erases per-job generics
export type AnyJob = JobDefinition<string, StandardSchemaV1, any>;

/** The `deps` a job's handler expects; `unknown` when it declares none. */
export type JobDeps<T> = T extends JobDefinition<string, StandardSchemaV1, infer D> ? D : never;

export function defineJob<K extends string, S extends StandardSchemaV1, D = unknown>(
  input: JobDefinitionInput<K, S, D>,
): JobDefinition<K, S, D> {
  if (!KIND_PATTERN.test(input.kind)) {
    throw new Error(
      `defineJob: kind ${JSON.stringify(input.kind)} must match ${KIND_PATTERN} (lowercase, ≤ 200 chars)`,
    );
  }

  if (
    !Number.isInteger(input.timeoutMs) ||
    input.timeoutMs < 1 ||
    input.timeoutMs > MAX_TIMEOUT_MS
  ) {
    throw new Error(
      `defineJob(${input.kind}): timeoutMs must be an integer in [1, ${MAX_TIMEOUT_MS}], got ${input.timeoutMs}`,
    );
  }

  const lane = input.lane ?? DEFAULT_LANE;

  if (!LANE_PATTERN.test(lane)) {
    throw new Error(
      `defineJob(${input.kind}): lane ${JSON.stringify(lane)} must match ${LANE_PATTERN} (lowercase, ≤ 50 chars)`,
    );
  }

  return {
    kind: input.kind,
    payload: input.payload,
    timeoutMs: input.timeoutMs,
    lane,
    graceMs: HANDLER_GRACE_MS,
    leaseMs: input.timeoutMs + HANDLER_GRACE_MS + LEASE_MARGIN_MS,
    retry: resolveRetryPolicy(input.retry),
    classify: withDefaultClassify(input.classify),
    handle: input.handle,
  };
}

/**
 * `defineJob` with `ctx.deps` typed, without annotating every handler:
 *
 * ```ts
 * const defineAppJob = defineJobFor<AppDeps>();
 * export const welcome = defineAppJob({ kind: 'welcome', …, handle: async (p, ctx) => ctx.deps.mailer… });
 * ```
 */
export function defineJobFor<D>() {
  return <K extends string, S extends StandardSchemaV1>(
    input: JobDefinitionInput<K, S, D>,
  ): JobDefinition<K, S, D> => defineJob<K, S, D>(input);
}

/** What a thrown error becomes when `classify` is omitted or returns `undefined`: retry with code `unexpected`. */
export function defaultClassify(error: unknown): HandlerOutcome {
  return retry({ code: 'unexpected', message: errorMessage(error) });
}

function withDefaultClassify(
  classify: JobDefinitionInput<string, StandardSchemaV1, unknown>['classify'],
) {
  if (!classify) {
    return defaultClassify;
  }

  return (error: unknown) => classify(error) ?? defaultClassify(error);
}

function errorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}
