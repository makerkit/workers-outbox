import type { Clock } from './types';

/** A bounded wait ran out: the work may still be running, and its outcome is unknown. */
export class DeadlineExceededError extends Error {
  constructor(what: string) {
    super(`${what} exceeded its deadline`);
    this.name = 'DeadlineExceededError';
  }
}

export type Bounded<T> = { done: true; value?: T; error?: unknown } | { done: false };

// Re-checked at least this often, so a deadline measured on the runtime clock is honoured even
// when that clock jumps (a test clock, or a large NTP correction).
const POLL_MS = 250;

/**
 * Waits for `work` until `deadlineAt` on `clock` — or until the same span of real time passes,
 * whichever is first, so a stalled or frozen clock can't stretch it. Never rejects. A late result
 * is dropped: the caller must make abandoned work harmless (destroy its session, discard a late
 * connection).
 */
export async function untilDeadline<T>(
  clock: Clock,
  work: Promise<T>,
  deadlineAt: number,
): Promise<Bounded<T>> {
  let settled: Bounded<T> | undefined;
  const watched = work.then(
    (value) => {
      settled = { done: true, value };
    },
    (error: unknown) => {
      settled = { done: true, error: error ?? new Error('rejected') };
    },
  );

  let realDeadline = Infinity;

  for (;;) {
    if (settled) {
      return settled;
    }

    // Whenever the clock shows less time left (it jumped), real time catches up with it.
    realDeadline = Math.min(realDeadline, Date.now() + (deadlineAt - clock.nowMs()));
    const remaining = realDeadline - Date.now();

    if (remaining <= 0) {
      return { done: false };
    }

    let timer: ReturnType<typeof setTimeout> | undefined;

    await Promise.race([
      watched,
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, Math.min(remaining, POLL_MS));
      }),
    ]);
    clearTimeout(timer);
  }
}

/** Resolves when `work` settles or `ms` of real time pass, whichever is first; never rejects. */
export async function settledWithin<T>(work: Promise<T>, ms: number): Promise<Bounded<T>> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<{ done: false }>((resolve) => {
    timer = setTimeout(() => resolve({ done: false }), Math.max(0, ms));
  });

  try {
    return await Promise.race([
      work.then(
        (value) => ({ done: true as const, value }),
        (error: unknown) => ({ done: true as const, error: error ?? new Error('rejected') }),
      ),
      timeout,
    ]);
  } finally {
    clearTimeout(timer);
  }
}

/** The value of a bounded wait, or a throw: the work's own error, or `DeadlineExceededError`. */
export function unwrap<T>(result: Bounded<T>, what: string): T {
  if (!result.done) {
    throw new DeadlineExceededError(what);
  }

  if (result.error !== undefined) {
    throw result.error;
  }

  return result.value as T;
}
