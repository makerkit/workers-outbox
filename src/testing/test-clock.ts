import type { Clock } from '../runtime/types';

export interface TestClock extends Clock {
  /** Moves time forward; the database sees it too (`dbNow`), so due checks follow. */
  advance(ms: number): void;
  current(): Date;
}

/**
 * A frozen clock for tests: pass it to `createOutbox({ clock })` and to `runUntilIdle`. Sleeps
 * resolve at once; the runtime still bounds every wait in real time, so a frozen clock can't hang.
 */
export function createTestClock(start: Date = new Date()): TestClock {
  let current = new Date(start.getTime());

  return {
    dbNow: () => current,
    nowMs: () => current.getTime(),
    sleep: async () => undefined,
    advance: (ms) => {
      current = new Date(current.getTime() + ms);
    },
    current: () => current,
  };
}
