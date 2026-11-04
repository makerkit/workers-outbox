import type { SqlRunner } from '../drizzle/catalog';

export interface OutboxDb extends SqlRunner {
  transaction<T>(fn: (tx: OutboxDb) => Promise<T>): Promise<T>;
}

/** One client per lane per invocation: lanes never share a Hyperdrive session. */
export interface LaneConnection {
  db: OutboxDb;
  /** Returns a healthy client (e.g. `client.release()`). */
  close(): Promise<void>;
  /**
   * Discards the session: it must never be reused (e.g. `client.release(true)`, `client.end()`).
   * Called for a runaway or timed-out lane, before its replacement is acquired, so a one-client
   * pool can still reconnect. Must be idempotent, and make a later `close()` a no-op.
   */
  destroy(): Promise<void>;
  /**
   * Optional: true once the session is known dead (e.g. its socket errored while idle). The lane
   * is then replaced before its next claim or settlement instead of failing a round trip first.
   */
  isBroken?(): boolean;
}

export interface QueueSender {
  send(body: unknown, options?: { delaySeconds?: number }): Promise<unknown>;
}

/** Job ids or a lane, never payloads. */
export type OutboxMessage =
  | { v: 1; type: 'nudge'; ids: string[] }
  | { v: 1; type: 'drain'; lane: string };

export interface Clock {
  /** `null` = the database's own `now()`. */
  dbNow(): Date | null;
  nowMs(): number;
  sleep(ms: number): Promise<void>;
}

export const systemClock: Clock = {
  dbNow: () => null,
  nowMs: () => Date.now(),
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
};

export const MAX_QUEUE_DELAY_SECONDS = 86_400;

export const MAX_IDS_PER_NUDGE = 100;
