import { drizzle } from 'drizzle-orm/node-postgres';
import { Client, type ClientConfig, type Pool, type PoolClient } from 'pg';

import type { LaneConnection, OutboxDb } from '../runtime/types';

export interface PgConnectorOptions {
  /**
   * Errors a session raises outside a query, e.g. the FATAL `transaction_timeout` sends when it
   * ends a settlement past its deadline. They are always caught (node-pg would otherwise crash the
   * process); this only observes them. The outbox has already seen the failed query.
   */
  onError?: (error: Error) => void;
}

export interface PgClientConnectorOptions extends PgConnectorOptions {
  /** Bound on opening one session. Default 10 s. */
  connectionTimeoutMs?: number;
}

const DEFAULT_CONNECTION_TIMEOUT_MS = 10_000;

/**
 * A fresh `pg.Client` per lane — the shape Hyperdrive expects, since it pools upstream:
 * `createPgConnector(env.OUTBOX_DB.connectionString)`. `close()` and `destroy()` both end the
 * session (a client is never reused); a query still running server-side runs until it next
 * writes to the closed socket, which the settlement's own `transaction_timeout` bounds.
 */
export function createPgConnector(
  config: string | ClientConfig,
  options: PgClientConnectorOptions = {},
): () => Promise<LaneConnection> {
  const base: ClientConfig = typeof config === 'string' ? { connectionString: config } : config;
  const connectionTimeoutMillis =
    base.connectionTimeoutMillis ?? options.connectionTimeoutMs ?? DEFAULT_CONNECTION_TIMEOUT_MS;

  return async () => {
    const client = new Client({ ...base, connectionTimeoutMillis });
    // node-pg emits 'error' outside a query only when the session is dead; remember it, so the
    // lane is replaced before its next query instead of failing one.
    let broken = false;
    // Before connect(): an error between connect and the first query must not crash the process.
    client.on('error', (error) => {
      broken = true;
      options.onError?.(error);
    });

    try {
      await client.connect();
    } catch (error) {
      await client.end().catch(() => undefined);
      throw error;
    }

    const end = once(() => client.end());

    return { db: drizzleOf(client), close: end, destroy: end, isBroken: () => broken };
  };
}

/**
 * Checks clients out of a pool you own (Node, or a long-lived Worker-side pool). Set the pool's
 * `connectionTimeoutMillis`: the outbox bounds its own wait, but a checkout that never returns
 * still holds a pool slot. Also attach `pool.on('error', …)`: node-pg re-emits a returned
 * client's late errors (a FATAL after release) on the pool, which crashes the process unlistened.
 * `close()` returns the client; `destroy()` discards it
 * (`release(true)`), so a session abandoned mid-query is never reused.
 */
export function createPgPoolConnector(
  pool: Pool,
  options: PgConnectorOptions = {},
): () => Promise<LaneConnection> {
  return async () => {
    const client = await pool.connect();
    let broken = false;
    const onError = (error: Error) => {
      broken = true;
      options.onError?.(error);
    };
    client.on('error', onError);

    const release = (destroy: boolean) =>
      once(async () => {
        // A pooled client outlives this lane: don't leave a listener per checkout on it.
        client.off('error', onError);
        client.release(destroy);
      });
    // close and destroy share one flag: whichever runs first wins, the other is a no-op.
    let ended: Promise<void> | undefined;
    const close = release(false);
    const destroy = release(true);

    return {
      db: drizzleOf(client),
      // A dead session must not go back to the pool.
      close: () => (ended ??= broken ? destroy() : close()),
      destroy: () => (ended ??= destroy()),
      isBroken: () => broken,
    };
  };
}

const drizzleOf = (client: Client | PoolClient) => drizzle({ client }) as unknown as OutboxDb;

function once(fn: () => Promise<void>): () => Promise<void> {
  let done: Promise<void> | undefined;

  return () => (done ??= fn().catch(() => undefined));
}
