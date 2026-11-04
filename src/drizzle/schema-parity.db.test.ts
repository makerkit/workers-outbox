import { PGlite } from '@electric-sql/pglite';
import { generateDrizzleJson, generateMigration } from 'drizzle-kit/api-postgres';
import { drizzle } from 'drizzle-orm/pglite';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  createOutboxTestDatabase,
  type OutboxTestDatabase,
} from '../testing/create-outbox-test-database';
import { readOutboxCatalog, type OutboxCatalog } from './catalog';
import * as schema from './schema';

describe('schema parity: drizzle-kit vs shipped migrations', () => {
  let shipped: OutboxTestDatabase;
  let generated: PGlite;

  beforeAll(async () => {
    shipped = await createOutboxTestDatabase();

    generated = await PGlite.create();
    const statements = await generateMigration(
      await generateDrizzleJson({}),
      await generateDrizzleJson(schema),
    );

    for (const statement of statements) {
      await generated.exec(statement);
    }
  });

  afterAll(async () => {
    await shipped.close();
    await generated.close();
  });

  it('produces an identical catalog', async () => {
    const fromMigrations = await readOutboxCatalog(shipped.db);
    const fromDrizzleKit = await readOutboxCatalog(drizzle({ client: generated }));

    expect(fromMigrations.columns.length).toBeGreaterThan(0);
    expect(normalize(fromMigrations)).toEqual(normalize(fromDrizzleKit));
  });
});

function normalize(catalog: OutboxCatalog) {
  const byKey = <T>(rows: T[], key: (row: T) => string) =>
    [...rows].sort((a, b) => key(a).localeCompare(key(b)));

  return {
    columns: byKey(catalog.columns, (c) => `${c.table}.${c.column}`),
    constraints: byKey(catalog.constraints, (c) => `${c.table}.${c.name}`),
    indexes: byKey(catalog.indexes, (i) => `${i.table}.${i.name}`),
  };
}
