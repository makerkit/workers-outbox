import type { AsyncLocalStorage } from 'node:async_hooks';
import type { Client, Pool, PoolClient } from 'pg';

import { entityKind } from 'drizzle-orm';
import { PgAsyncDatabase, type PgTransactionConfig } from 'drizzle-orm/pg-core';

import { guardPgClient, isGuardedPgClient } from './guard-client';

type Work<Tx, R> = (tx: Tx) => Promise<R>;
type Transactional<Tx> = { transaction<R>(work: Work<Tx, R>): Promise<R> };
interface PgSession<Tx> {
  client: Client | Pool;
  transaction<R>(work: Work<Tx, R>, config?: PgTransactionConfig): Promise<R>;
}

function pgSession<Tx>(db: unknown) {
  const session = (db as { _?: { session?: PgSession<Tx> } } | null)?._?.session;
  const kind = (session?.constructor as { [entityKind]?: string } | undefined)?.[entityKind];
  return kind === 'NodePgSession' ? session : undefined;
}

/** Drizzle starts and finishes the transaction. We own its checkout, including a failed BEGIN. */
async function guardedTransaction<Tx, R>(
  session: PgSession<Tx>,
  work: Work<Tx, R>,
  config?: PgTransactionConfig,
) {
  const driver = session.client;
  const pooled = 'totalCount' in driver;
  const client = pooled ? await driver.connect() : driver;
  let discarded: Promise<void> | undefined;
  const discard = () => {
    if (!discarded) {
      discarded = pooled ? Promise.resolve((client as PoolClient).release(true)) : client.end();
    }
    return discarded;
  };
  const onError = () => {
    void discard().catch(() => undefined);
  };
  client.on('error', onError);
  try {
    const scoped: PgSession<Tx> = Object.create(session, {
      client: { value: guardPgClient(client, discard) },
    });
    return await scoped.transaction(work, config);
  } finally {
    client.off('error', onError);
    if (pooled && !discarded) (client as PoolClient).release();
  }
}

type Route = { callers: Set<symbol | undefined>; restore(): void };
const routes = new WeakMap<object, Route>();
let producerScope: AsyncLocalStorage<symbol | undefined> | undefined;

/**
 * A replica router or bound decorator calls the original database even if given a different
 * receiver. Intercept its session transaction before BEGIN, once the decorator has chosen the
 * callback and config. Concurrent ordinary calls and calls inside the callback use Drizzle as usual.
 */
async function routedTransaction<Tx, R>(
  db: Transactional<Tx>,
  session: PgSession<Tx>,
  work: Work<Tx, R>,
) {
  const { AsyncLocalStorage } = await import('node:async_hooks');
  const scope = (producerScope ??= new AsyncLocalStorage<symbol | undefined>());
  let route = routes.get(session);
  if (!route) {
    const callers = new Set<symbol | undefined>();
    const original = session.transaction;
    const own = Object.getOwnPropertyDescriptor(session, 'transaction');
    const dispatch: PgSession<Tx>['transaction'] = function (this: PgSession<Tx>, fn, config) {
      if (this === session && callers.has(scope.getStore())) {
        return scope.run(undefined, () => guardedTransaction(session, fn, config));
      }
      return original.bind(this)(fn, config);
    };
    session.transaction = dispatch;
    route = {
      callers,
      restore: () => {
        if (session.transaction !== dispatch) return;
        if (own) Object.defineProperty(session, 'transaction', own);
        else Reflect.deleteProperty(session, 'transaction');
      },
    };
    routes.set(session, route);
  }
  const caller = Symbol();
  route.callers.add(caller);
  try {
    return await scope.run(caller, () => db.transaction(work));
  } finally {
    route.callers.delete(caller);
    if (!route.callers.size) {
      route.restore();
      routes.delete(session);
    }
  }
}

// A custom adapter may expose only transaction(fn). If it supplies a node-pg transaction,
// protect the session it handed us. The adapter owns its original checkout and release.
async function delegatedTransaction<Tx, R>(db: Transactional<Tx>, work: Work<Tx, R>) {
  const cleanups: Array<() => void> = [];
  try {
    return await db.transaction((tx) => {
      const session = pgSession<Tx>(tx);
      const client = session?.client;
      if (session && client && !('totalCount' in client) && !isGuardedPgClient(client)) {
        let ending: Promise<void> | undefined;
        const discard = () => (ending ??= client.end());
        const onError = () => {
          void discard().catch(() => undefined);
        };
        client.on('error', onError);
        session.client = guardPgClient(client, discard);
        cleanups.push(() => {
          client.off('error', onError);
          session.client = client;
        });
      }
      return work(tx);
    });
  } finally {
    for (const cleanup of cleanups) cleanup();
  }
}

export function producerTransaction<Tx, R>(db: Transactional<Tx>, work: Work<Tx, R>): Promise<R> {
  const session = pgSession<Tx>(db);
  if (!session) return delegatedTransaction(db, work);
  if (typeof session.client?.query !== 'function' || isGuardedPgClient(session.client))
    return db.transaction(work);
  return db.transaction === PgAsyncDatabase.prototype.transaction
    ? guardedTransaction(session, work)
    : routedTransaction(db, session, work);
}
