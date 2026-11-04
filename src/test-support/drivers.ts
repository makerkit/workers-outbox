import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import { v7 as uuidv7 } from 'uuid';

import type { SqlRunner } from '../drizzle/catalog';

import { OUTBOX_MIGRATIONS } from '../sql/migrations';
import { createOutboxTestDatabase } from '../testing/create-outbox-test-database';

export interface TestDb extends SqlRunner {
  transaction<T>(fn: (tx: SqlRunner) => Promise<T>): Promise<T>;
}

export interface TestDriver {
  db: TestDb;
  reset(): Promise<void>;
  close(): Promise<void>;
}

export interface DriverFactory {
  name: string;
  create(): Promise<TestDriver>;
}

export interface PgDriver extends TestDriver {
  pool: Pool;
}

export const pgUrls = () =>
  (process.env['OUTBOX_PG_URLS'] ?? '')
    .split(',')
    .map((url) => url.trim())
    .filter(Boolean);

export function testDrivers(): DriverFactory[] {
  return [
    {
      name: 'pglite',
      create: async () => {
        const testDb = await createOutboxTestDatabase();

        return { db: testDb.db, reset: testDb.reset, close: testDb.close };
      },
    },
    ...pgDrivers(),
  ];
}

export interface PgDriverFactory {
  name: string;
  create(): Promise<PgDriver>;
}

export function pgDrivers(): PgDriverFactory[] {
  return pgUrls().map((url) => ({
    name: `postgres ${new URL(url).port}`,
    create: () => createPgDriver(url),
  }));
}

// Yields a placeholder without Postgres so the suite is skipped, not "No test suite found".
export function pgSuiteDrivers(): PgDriverFactory[] {
  const drivers = pgDrivers();

  if (drivers.length > 0) {
    return drivers;
  }

  return [
    {
      name: 'postgres (skipped: set OUTBOX_PG_URLS, see pnpm test:pg)',
      create: () => Promise.reject(new Error('no Postgres configured')),
    },
  ];
}

async function createPgDriver(url: string): Promise<PgDriver> {
  const name = `outbox_${uuidv7().replaceAll('-', '')}`;
  const admin = new Pool({ connectionString: url, max: 1 });

  await admin.query(`CREATE DATABASE "${name}"`);

  const target = new URL(url);
  target.pathname = `/${name}`;

  const pool = new Pool({ connectionString: target.toString(), max: 10 });

  for (const migration of OUTBOX_MIGRATIONS) {
    for (const statement of migration.statements) {
      await pool.query(statement);
    }
  }

  const db = drizzlePg({ client: pool });

  return {
    db,
    pool,
    reset: async () => {
      await pool.query('TRUNCATE "outbox"."job" CASCADE');
    },
    close: async () => {
      await pool.end();
      await dropWhenIdle(admin, name);
      await admin.end();
    },
  };
}

// pool.end() resolves before sockets close; FORCE would kill them mid-shutdown (uncaught 57P01).
async function dropWhenIdle(admin: Pool, name: string) {
  for (let i = 0; i < 50; i++) {
    try {
      await admin.query(`DROP DATABASE "${name}"`);

      return;
    } catch (error) {
      if ((error as { code?: string }).code !== '55006') {
        throw error;
      }

      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }

  await admin.query(`DROP DATABASE "${name}" WITH (FORCE)`);
}
