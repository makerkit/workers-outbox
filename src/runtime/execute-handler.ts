import { sql } from 'drizzle-orm';

import type { AnyJob, JobContext } from '../core/define-job';
import type { ClaimedJob } from '../store/outbox-store';
import type { Clock, OutboxDb } from './types';

import { defaultClassify } from '../core/define-job';
import { errorMessage } from '../core/error-message';
import { retry, type HandlerOutcome, type SettleOutcome } from '../core/outcome';

export interface ExecutionResult {
  outcome: SettleOutcome;
  thrown?: unknown;
  /** Ran past its grace period: the lane's connection must not be reused. */
  runaway: boolean;
}

// A timeout is final: a late result is discarded so it can't race the retry.
export async function executeHandler(input: {
  job: AnyJob;
  claimed: ClaimedJob;
  db: OutboxDb;
  /** Retire the previous session and lend the attempt a fresh one. */
  replaceDb?: () => Promise<OutboxDb>;
  deps: unknown;
  clock: Clock;
}): Promise<ExecutionResult> {
  const { job, claimed } = input;
  const controller = new AbortController();
  // Closed once the handler is done: a stray query must not land inside the settle transaction.
  const handlerDb = closable(input.db);
  // The same instant the timer below measures from, on the injected clock.
  const deadline = input.clock.nowMs() + job.timeoutMs;
  // Timers cannot interrupt synchronous validation or handler work. Check elapsed time at
  // the boundaries too; an injected/frozen clock must not disable the real attempt timeout.
  const realDeadline = performance.now() + job.timeoutMs;
  const ctx: JobContext = {
    jobId: claimed.id,
    kind: claimed.kind,
    attempt: claimed.attemptNo,
    idempotencyKey: claimed.id,
    signal: controller.signal,
    deadline,
    remainingMs: () => Math.max(0, deadline - input.clock.nowMs()),
    deps: input.deps,
    tenantKey: claimed.tenantKey,
    correlationId: claimed.correlationId,
    metadata: claimed.metadata,
    db: handlerDb.db,
  };

  // Validation runs inside the timed attempt: an async validator is part of the lease's work.
  // Set by the timer itself, so a validation that resolves after the timeout can't slip through.
  let expired = false;
  const hasExpired = () => expired || performance.now() >= realDeadline;
  // Deployed Workers freeze both JavaScript clocks between I/O events. A timer
  // microtask alone cannot measure CPU spent validating or running the handler.
  const workersClock = 'WebSocketPair' in globalThis;
  const refreshClock = (db: OutboxDb) => db.execute(sql`SELECT clock_timestamp()`);
  // Armed before the attempt starts: a synchronous validator runs before `run`'s first await, and
  // must count against `timeoutMs` (and `ctx.deadline`) like the rest of the attempt.
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<{ settled: 'timeout' }>((resolve) => {
    timer = setTimeout(() => {
      expired = true;
      resolve({ settled: 'timeout' });
    }, job.timeoutMs);
  });

  const run = (async (): Promise<Settled> => {
    const validated = await validatePayload(job, claimed.payload);

    if (workersClock && !hasExpired()) await refreshClock(input.db);
    if (hasExpired()) {
      // The attempt was already abandoned: starting the handler now would be a new, unfenced run.
      return { settled: 'timeout' };
    }

    if (!validated.ok) {
      return { settled: 'invalid_payload', message: validated.message };
    }

    try {
      const value = await job.handle(validated.value, ctx);
      return hasExpired() ? { settled: 'timeout' } : { settled: 'value', value };
    } catch (error) {
      return hasExpired() ? { settled: 'timeout' } : { settled: 'error', error };
    }
  })().then(async (result): Promise<Settled> => {
    handlerDb.close();
    if (
      workersClock &&
      (result.settled === 'value' || result.settled === 'error') &&
      !hasExpired()
    ) {
      try {
        await refreshClock(input.db);
      } catch (error) {
        // The handler may have caught the error that discarded its connection. Keep
        // its result while measuring elapsed time on a fresh session, under the same timer.
        if (!hasExpired()) {
          if (!input.replaceDb) throw error;
          const fresh = await input.replaceDb();
          if (!hasExpired()) await refreshClock(fresh);
        }
      }
    }
    return result;
  });

  let first: Settled;
  try {
    first = await Promise.race([run, timeout]);
  } finally {
    clearTimeout(timer);
    handlerDb.close();
  }

  if (first.settled === 'timeout' || hasExpired()) {
    expired = true;
    controller.abort(new Error(`job ${claimed.id} timed out after ${job.timeoutMs}ms`));
    const unwound = await Promise.race([
      run.then(
        () => true,
        () => true,
      ),
      input.clock.sleep(job.graceMs).then(() => false),
    ]);

    return { outcome: { type: 'timeout' }, runaway: !unwound };
  }

  if (first.settled === 'invalid_payload') {
    return {
      // Not `fail`: a newer deploy may own this shape; the per-code cap ends it.
      outcome: { type: 'retry', code: 'invalid_payload', message: first.message },
      runaway: false,
    };
  }

  if (first.settled === 'error') {
    return { outcome: classifySafely(job, first.error), thrown: first.error, runaway: false };
  }

  if (!isHandlerOutcome(first.value)) {
    return {
      outcome: retry({
        code: 'invalid_outcome',
        message: 'handler returned a value that is not an outcome',
      }),
      runaway: false,
    };
  }

  return { outcome: first.value, runaway: false };
}

type Settled =
  | { settled: 'timeout' }
  | { settled: 'value'; value: unknown }
  | { settled: 'error'; error: unknown }
  | { settled: 'invalid_payload'; message: string };

async function validatePayload(
  job: AnyJob,
  payload: unknown,
): Promise<{ ok: true; value: unknown } | { ok: false; message: string }> {
  try {
    const validated = await job.payload['~standard'].validate(payload);

    if (validated.issues) {
      return { ok: false, message: validated.issues.map((issue) => issue.message).join('; ') };
    }

    return { ok: true, value: validated.value };
  } catch (error) {
    return { ok: false, message: errorMessage(error) };
  }
}

/**
 * Revocation is checked when a method is called, not when it is read, so a captured
 * `ctx.db.execute` or a `tx` handed to `transaction()` stops working too. Query builders
 * created before close and awaited after it are not covered: don't let work outlive the handler.
 */
function closable(db: OutboxDb) {
  let closed = false;
  const revoked = () => new Error('ctx.db used after the handler returned');

  const wrap = (target: OutboxDb): OutboxDb =>
    new Proxy(target, {
      get(inner, prop) {
        const value = Reflect.get(inner, prop, inner) as unknown;

        if (typeof value !== 'function') {
          if (closed) {
            throw revoked();
          }

          return value;
        }

        if (prop === 'transaction') {
          return (fn: (tx: OutboxDb) => Promise<unknown>, ...rest: unknown[]) =>
            closed
              ? Promise.reject(revoked())
              : (value as (...args: unknown[]) => unknown).call(
                  inner,
                  (tx: OutboxDb) => fn(wrap(tx)),
                  ...rest,
                );
        }

        return (...args: unknown[]) =>
          closed
            ? Promise.reject(revoked())
            : (value as (...a: unknown[]) => unknown).apply(inner, args);
      },
    });

  return {
    db: wrap(db),
    close: () => {
      closed = true;
    },
  };
}

export function classifySafely(job: AnyJob, error: unknown): HandlerOutcome {
  try {
    const outcome = job.classify(error);

    return isHandlerOutcome(outcome) ? outcome : retry({ code: 'unexpected' });
  } catch {
    return defaultClassify(error);
  }
}

// Nullish is absent: settlement treats a missing optional field and `null` alike.
const isOptional = (value: unknown, type: 'string' | 'function') =>
  value == null || typeof value === type;

/** Checks every field settlement reads, not just the tag: JS or an unsafe cast can return anything. */
function isHandlerOutcome(value: unknown): value is HandlerOutcome {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const outcome = value as Record<string, unknown>;

  switch (outcome['type']) {
    case 'ok':
      return isOptional(outcome['commit'], 'function');
    // A non-numeric `afterMs` is not checked: retry scheduling falls back to backoff.
    case 'retry':
    case 'fail':
      return isCode(outcome['code']) && isOptional(outcome['message'], 'string');
    case 'cancel':
      return typeof outcome['reason'] === 'string';
    default:
      return false;
  }
}

const isCode = (value: unknown) => typeof value === 'string' && value !== '';
