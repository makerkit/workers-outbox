import { sql } from 'drizzle-orm';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import type { OutboxDb } from '../runtime/types';

import { pgSuiteDrivers, pgUrls, type PgDriver } from '../test-support/drivers';
import { createPgConnector, createPgPoolConnector } from './index';

const drivers = pgSuiteDrivers();

describe.skipIf(pgUrls().length === 0).each(drivers)('pg connectors on $name', ({ create }) => {
  let driver: PgDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  const url = () => driver.pool.options.connectionString ?? '';

  const backendAlive = async (pid: number) => {
    const { rows } = await driver.pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM pg_stat_activity WHERE pid = $1`,
      [pid],
    );

    return rows[0]?.n === 1;
  };

  const pidOf = async (db: OutboxDb) => {
    const result = (await db.execute(sql`SELECT pg_backend_pid() AS pid`)) as {
      rows: { pid: number }[];
    };

    return result.rows[0]!.pid;
  };

  it('client: runs transactions, and close then destroy is one end', async () => {
    const connection = await createPgConnector(url())();

    const value = await connection.db.transaction(async (tx) => {
      const r = (await tx.execute(sql`SELECT 41 + 1 AS n`)) as { rows: { n: number }[] };

      return r.rows[0]?.n;
    });

    expect(value).toBe(42);
    await connection.close();
    await expect(connection.destroy()).resolves.toBeUndefined();
    await expect(connection.close()).resolves.toBeUndefined();
  });

  it('client: destroy returns at once on a session busy in a query', async () => {
    const connection = await createPgConnector(url())();
    const running = Promise.resolve(connection.db.execute(sql`SELECT pg_sleep(5)`)).catch(
      () => 'ended',
    );
    await new Promise((resolve) => setTimeout(resolve, 100));

    const started = Date.now();
    await connection.destroy();

    expect(Date.now() - started).toBeLessThan(1_000);
    expect(await running).toBe('ended');
  });

  it('client: a FATAL outside a query reaches onError instead of crashing the process', async () => {
    const errors: Error[] = [];
    const connection = await createPgConnector(url(), { onError: (e) => errors.push(e) })();
    const pid = await pidOf(connection.db);

    await driver.pool.query('SELECT pg_terminate_backend($1)', [pid]);

    for (let i = 0; i < 50 && errors.length === 0; i++) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    expect(errors.length).toBeGreaterThan(0);
    await connection.destroy();
  });

  it('client: an unreachable server rejects at connectionTimeoutMs', async () => {
    // A blackhole address: the TCP connect never completes.
    const connect = createPgConnector(
      { host: '10.255.255.1', port: 5432, user: 'x', database: 'x' },
      { connectionTimeoutMs: 300 },
    );
    const started = Date.now();

    // The timeout's own error, not a fast EHOSTUNREACH that would pass without the bound.
    await expect(connect()).rejects.toThrow(/timeout/i);
    expect(Date.now() - started).toBeLessThan(3_000);
  });

  it('pool: close returns the client, destroy discards it, and listeners never pile up', async () => {
    const pool = new Pool({ connectionString: url(), max: 1 });

    try {
      const connect = createPgPoolConnector(pool);

      const first = await connect();
      const pid = await pidOf(first.db);
      await first.close();
      await first.destroy(); // no-op: close already won

      const second = await connect();
      expect(await pidOf(second.db)).toBe(pid); // reused
      const client = (second.db as unknown as { $client: { listenerCount(e: string): number } })
        .$client;
      expect(client.listenerCount('error')).toBe(1);
      await second.destroy();
      await second.close(); // no-op: destroy already won

      const third = await connect();
      expect(await pidOf(third.db)).not.toBe(pid); // the destroyed session was not reused
      await third.close();

      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(await backendAlive(pid)).toBe(false);
    } finally {
      await pool.end();
    }
  });
});
