import { sql } from 'drizzle-orm';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import {
  createOutboxTestDatabase,
  type OutboxTestDatabase,
} from '../testing/create-outbox-test-database';
import { readOutboxCatalog } from './catalog';
import { verifyOutboxSchema } from './verify-schema';

describe('verifyOutboxSchema', () => {
  // A fresh database per test: these tests mutate the schema.
  let testDb: OutboxTestDatabase;

  beforeEach(async () => {
    testDb = await createOutboxTestDatabase();
  });

  afterEach(() => testDb.close());

  it('reports no drift on a database built from the shipped migrations', async () => {
    expect(await verifyOutboxSchema(testDb.db)).toEqual([]);
  });

  // port: job-outbox-claim-indexes.schema.db:73
  it('reports a dropped claim index, which would silently degrade every claim', async () => {
    await testDb.db.execute(sql`DROP INDEX "outbox"."job_claim_due_idx"`);

    expect(await verifyOutboxSchema(testDb.db)).toEqual([
      { type: 'missing_index', table: 'job', name: 'job_claim_due_idx' },
    ]);
  });

  it('reports a dropped CHECK constraint', async () => {
    await testDb.db.execute(
      sql`ALTER TABLE "outbox"."job" DROP CONSTRAINT "job_retrying_requires_next_attempt_at"`,
    );

    expect(await verifyOutboxSchema(testDb.db)).toEqual([
      { type: 'missing_constraint', table: 'job', name: 'job_retrying_requires_next_attempt_at' },
    ]);
  });

  it('reports missing and changed columns', async () => {
    await testDb.db.execute(sql`ALTER TABLE "outbox"."attempt" DROP COLUMN "duration_ms"`);
    await testDb.db.execute(sql`ALTER TABLE "outbox"."job" ALTER COLUMN "kind" DROP NOT NULL`);

    expect(await verifyOutboxSchema(testDb.db)).toEqual([
      {
        type: 'column_mismatch',
        table: 'job',
        column: 'kind',
        expected: 'text not null',
        actual: 'text',
      },
      { type: 'missing_column', table: 'attempt', column: 'duration_ms' },
    ]);
  });

  it('reports missing tables when the schema was never migrated', async () => {
    await testDb.db.execute(sql`DROP SCHEMA "outbox" CASCADE`);

    expect(await verifyOutboxSchema(testDb.db)).toEqual([
      { type: 'missing_table', table: 'job' },
      { type: 'missing_table', table: 'attempt' },
    ]);
  });

  // port: job-outbox-claim-indexes.schema.db:44
  // port: job-outbox-claim-indexes.schema.db:54
  // port: job-outbox-claim-indexes.schema.db:73
  // port: job-outbox-claim-indexes.schema.db:118
  it('ships the claim, kind and tenant indexes in their load-bearing shapes', async () => {
    const { indexes } = await readOutboxCatalog(testDb.db);
    const definition = (name: string) =>
      indexes.find((i) => i.name === name)?.definition.replace(/^.* USING /, '');

    expect(definition('job_claim_due_idx')).toBe(
      "btree (COALESCE(next_attempt_at, available_at), created_at, id) WHERE (status = ANY (ARRAY['pending'::text, 'retrying'::text]))",
    );
    expect(definition('job_pending_kind_idx')).toBe(
      "btree (kind, available_at, created_at) WHERE (status = 'pending'::text)",
    );
    expect(definition('job_retrying_kind_idx')).toBe(
      "btree (kind, next_attempt_at, created_at) WHERE (status = 'retrying'::text)",
    );
    expect(definition('job_tenant_key_idx')).toBe(
      'btree (tenant_key) WHERE (tenant_key IS NOT NULL)',
    );
    expect(definition('job_status_created_idx')).toBe('btree (status, created_at, id)');
  });

  it.each(['job_pending_kind_idx', 'job_retrying_kind_idx', 'job_status_created_idx'])(
    'reports a dropped %s',
    async (name) => {
      await testDb.db.execute(sql.raw(`DROP INDEX "outbox"."${name}"`));

      expect(await verifyOutboxSchema(testDb.db)).toEqual([
        { type: 'missing_index', table: 'job', name },
      ]);
    },
  );

  it('reports an index an interrupted concurrent build left INVALID', async () => {
    // What a failed CREATE INDEX CONCURRENTLY leaves: listed by pg_indexes, unused by the planner.
    await testDb.db.execute(
      sql`UPDATE pg_index SET indisvalid = false WHERE indexrelid = '"outbox"."job_claim_due_idx"'::regclass`,
    );

    expect(await verifyOutboxSchema(testDb.db)).toEqual([
      { type: 'invalid_index', table: 'job', name: 'job_claim_due_idx' },
    ]);
  });
});
