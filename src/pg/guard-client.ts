import type { Client, PoolClient } from 'pg';

import { isConnectionError } from '../runtime/db-errors';

const guardedClients = new WeakSet<object>();

export const isGuardedPgClient = (client: object) => guardedClients.has(client);

/** Invalidate the checkout before a caught read timeout can be followed by COMMIT. */
export function guardPgClient<T extends Client | PoolClient>(
  client: T,
  discard: () => Promise<void>,
) {
  let failure: unknown;
  const query = async (...args: unknown[]) => {
    if (failure !== undefined) throw failure;

    try {
      const result = await Reflect.apply(client.query, client, args);
      // COMMIT on an aborted transaction succeeds at the protocol level with a
      // ROLLBACK tag. An explicit ROLLBACK, including recovery to a savepoint, is fine.
      if (result?.command === 'ROLLBACK') {
        const statement =
          typeof args[0] === 'string' ? args[0] : (args[0] as { text?: string })?.text;
        if (statement && /^\s*(?:commit|end)\b/i.test(statement)) {
          throw new Error('PostgreSQL rolled back the transaction at COMMIT');
        }
      }
      return result;
    } catch (error) {
      if (isConnectionError(error)) {
        failure = error;
        await discard();
      }
      throw error;
    }
  };
  const guarded = new Proxy(client, {
    get(target, property, receiver) {
      return property === 'query' ? query : Reflect.get(target, property, receiver);
    },
  });
  guardedClients.add(guarded);
  return guarded;
}
