import { sql } from 'drizzle-orm';

import { queryRows, type SqlRunner } from '../drizzle/catalog';

/**
 * Default bound on one inspection read. A long scan holds back the xmin horizon, so autovacuum
 * can't clean the queue table's dead tuples while it runs.
 */
export const DEFAULT_INSPECT_STATEMENT_TIMEOUT_MS = 30_000;

// statement_timeout is an int32 count of milliseconds.
const MAX_STATEMENT_TIMEOUT_MS = 2_147_483_647;

export interface InspectBound {
  /** Per-statement bound for this read. Default 30 s; `0` leaves the session's own setting. */
  statementTimeoutMs?: number | undefined;
}

type Transactional = SqlRunner & {
  transaction<T>(fn: (tx: SqlRunner) => Promise<T>): Promise<T>;
};

const canTransact = (db: SqlRunner): db is Transactional =>
  typeof (db as Partial<Transactional>).transaction === 'function';

/**
 * Runs `read` in its own transaction (a savepoint inside the caller's) with a transaction-local
 * `statement_timeout`, restored before returning, so nothing leaks into a caller's transaction or,
 * on Hyperdrive, into another client's session. A runner without `transaction` runs unbounded.
 */
export async function boundedRead<T>(
  db: SqlRunner,
  bound: InspectBound | undefined,
  read: (db: SqlRunner) => Promise<T>,
): Promise<T> {
  const timeoutMs = bound?.statementTimeoutMs ?? DEFAULT_INSPECT_STATEMENT_TIMEOUT_MS;

  if (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > MAX_STATEMENT_TIMEOUT_MS) {
    throw new Error(
      `inspect: statementTimeoutMs must be an integer in [0, ${MAX_STATEMENT_TIMEOUT_MS}], got ${timeoutMs}`,
    );
  }

  if (timeoutMs === 0 || !canTransact(db)) {
    return read(db);
  }

  return db.transaction(async (tx) => {
    const [previous] = await queryRows<{ previous: string }>(
      tx,
      sql`SELECT current_setting('statement_timeout') AS previous,
                 set_config('statement_timeout', ${String(timeoutMs)}, true) AS applied`,
    );
    const result = await read(tx);

    // A savepoint's release keeps its SET LOCAL until the outer commit: put the caller's back.
    await tx.execute(
      sql`SELECT set_config('statement_timeout', ${previous?.previous ?? '0'}, true)`,
    );

    return result;
  });
}
