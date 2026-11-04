import { sql, type SQL } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { PgDialect } from 'drizzle-orm/pg-core';

import type { TestDriver } from '../test-support/drivers';
import type { OutboxEvent } from './events';
import type { LaneConnection, OutboxDb, OutboxMessage, QueueSender } from './types';

import { queryRows } from '../drizzle/catalog';

/** pglite is one session shared by every lane: single-lane tests only. */
export function laneConnector(driver: TestDriver, faults?: SqlFaults) {
  const opened: string[] = [];
  /** How each connection ended, in order: exactly one `close` or `destroy` per connection. */
  const ends: { connection: number; how: 'close' | 'destroy' }[] = [];

  const connect = async (): Promise<LaneConnection> => {
    opened.push('lane');
    const connection = opened.length;
    let ended = false;
    const end = (how: 'close' | 'destroy', release: () => unknown) => async () => {
      // destroy() must be idempotent, and close() after destroy() a no-op.
      if (ended) {
        return;
      }

      ended = true;
      ends.push({ connection, how });
      await release();
    };

    if ('pool' in driver) {
      const client = await (driver as TestDriver & { pool: import('pg').Pool }).pool.connect();
      const db = drizzle({ client }) as unknown as OutboxDb;

      return {
        db: faults ? faults.wrap(db) : db,
        close: end('close', () => client.release()),
        destroy: end('destroy', () => client.release(true)),
      };
    }

    const db = driver.db as unknown as OutboxDb;

    return {
      db: faults ? faults.wrap(db) : db,
      close: end('close', () => undefined),
      destroy: end('destroy', () => undefined),
    };
  };

  return { connect, opened, ends };
}

/**
 * A pool with one client: a second acquisition while the first is checked out fails instead of
 * waiting forever, so a self-deadlock shows up as an error rather than a hung test.
 */
export function singleSlotConnector(driver: TestDriver, faults?: SqlFaults) {
  const base = laneConnector(driver, faults);
  let held = false;
  const log: string[] = [];

  const connect = async (): Promise<LaneConnection> => {
    if (held) {
      log.push('connect:exhausted');
      throw new Error('pool exhausted: the only client is checked out');
    }

    held = true;
    log.push('connect');
    const c = await base.connect();
    const release = (how: 'close' | 'destroy') => async () => {
      if (held) {
        log.push(how);
      }

      held = false;
      await c[how]();
    };

    return { db: c.db, close: release('close'), destroy: release('destroy') };
  };

  return { connect, log, ends: base.ends };
}

/**
 * A handler that runs away and makes its lane's replacement connection fail: a runtime
 * (infra) error mid-job. `runaway()` hangs forever; only the reconnect it arms fails.
 */
export function runawayCrash(connect: () => Promise<LaneConnection>) {
  let armed = 0;
  let crashes = 0;

  return {
    runaway: () => {
      armed++;

      return new Promise<never>(() => {});
    },
    connect: async () => {
      if (armed > 0) {
        armed--;
        crashes++;
        throw new Error(`reconnect crashed #${crashes}`);
      }

      return connect();
    },
    crashes: () => crashes,
  };
}

// One clock for the library's tests and its users'.
export { createTestClock as testClock } from '../testing/test-clock';

export function recordingQueue(options: { fail?: (message: OutboxMessage) => unknown } = {}) {
  const sent: { message: OutboxMessage; delaySeconds: number | undefined }[] = [];

  const queue: QueueSender = {
    send: async (body, opts) => {
      const message = body as OutboxMessage;

      if (options.fail) {
        const failure = options.fail(message);

        if (failure) {
          throw failure;
        }
      }

      sent.push({ message, delaySeconds: opts?.delaySeconds });
    },
  };

  return { queue, sent };
}

export function recordingEvents() {
  const events: OutboxEvent[] = [];
  const of = <T extends OutboxEvent['type']>(type: T) =>
    events.filter((event): event is Extract<OutboxEvent, { type: T }> => event.type === type);

  return { sink: (event: OutboxEvent) => events.push(event), events, of };
}

export class SqlFaults {
  private readonly rules: {
    pattern: RegExp;
    error: () => unknown;
    remaining: number;
    after: boolean;
  }[] = [];
  readonly statements: string[] = [];

  failNext(pattern: RegExp, error: () => unknown, times = 1) {
    this.rules.push({ pattern, error, remaining: times, after: false });
  }

  /** Runs the next matching statement, then throws: a commit whose reply was lost. */
  failAfterNext(pattern: RegExp, error: () => unknown) {
    this.rules.push({ pattern, error, remaining: 1, after: true });
  }

  private commitFaults: (() => unknown)[] = [];

  /** The next transaction commits, then its caller sees `error`: the COMMIT reply was lost. */
  failAfterNextCommit(error: () => unknown) {
    this.commitFaults.push(error);
  }

  wrap(db: OutboxDb): OutboxDb {
    const dialect = new PgDialect();
    const render = (query: SQL) => dialect.sqlToQuery(query).sql;

    const wrapped: OutboxDb = {
      execute: async (query: SQL) => {
        const text = render(query);
        this.statements.push(text);
        const rule = this.rules.find((r) => r.remaining > 0 && r.pattern.test(text));

        if (rule) {
          rule.remaining--;

          if (rule.after) {
            await db.execute(query);
          }

          throw rule.error();
        }

        return db.execute(query);
      },
      transaction: async (fn) => {
        const result = await db.transaction((tx) => fn(this.wrap(tx)));
        const lost = this.commitFaults.shift();

        if (lost) {
          throw lost();
        }

        return result;
      },
    };

    return wrapped;
  }
}

export const pgFault = (code: string, message = `pg error ${code}`) =>
  new Error('Failed query', { cause: Object.assign(new Error(message), { code }) });

export async function jobRow(driver: TestDriver, id: string) {
  const [row] = await queryRows<Record<string, unknown>>(
    driver.db,
    sql`SELECT * FROM "outbox"."job" WHERE id = ${id}`,
  );

  return row;
}

export async function attemptRows(driver: TestDriver, jobId: string) {
  return queryRows<Record<string, unknown>>(
    driver.db,
    sql`SELECT * FROM "outbox"."attempt" WHERE job_id = ${jobId} ORDER BY attempt_no`,
  );
}

/** A dropped connection as node-pg surfaces it: no SQLSTATE, a socket error code. */
export const connectionReset = () =>
  new Error('Failed query', {
    cause: Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }),
  });
