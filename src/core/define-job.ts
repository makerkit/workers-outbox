import type { StandardSchemaV1 } from '@standard-schema/spec';

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
  /** Aborts at `timeoutMs`; pass it to every I/O call. */
  readonly signal: AbortSignal;
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
  /** Default: retry with code `unexpected`. */
  classify?: (error: unknown) => HandlerOutcome;
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
    classify: input.classify ?? defaultClassify,
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

function defaultClassify(error: unknown): HandlerOutcome {
  return retry({ code: 'unexpected', message: errorMessage(error) });
}

function errorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}
