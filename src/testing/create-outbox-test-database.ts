import { PGlite } from '@electric-sql/pglite';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/pglite';

import type { LaneConnection, OutboxDb } from '../runtime/types';

import { OUTBOX_MIGRATIONS } from '../sql/migrations';

/**
 * An in-memory pglite with the outbox schema applied. pglite is one session: it can't reproduce multi-connection races such as SKIP LOCKED contention.
 */
export async function createOutboxTestDatabase() {
  const client = await PGlite.create();

  for (const migration of OUTBOX_MIGRATIONS) {
    for (const statement of migration.statements) {
      await client.exec(statement);
    }
  }

  const db = drizzle({ client });
  // Every lane shares the one in-memory session, so ending a lane must never close it.
  const lane: LaneConnection = {
    db: db as unknown as OutboxDb,
    close: async () => undefined,
    destroy: async () => undefined,
  };

  return {
    db,
    client,
    /** Pass as `createOutbox({ connect })`. */
    connect: async () => lane,
    reset: async () => {
      await db.execute(sql`TRUNCATE "outbox"."job" CASCADE`);
    },
    close: () => client.close(),
  };
}

export type OutboxTestDatabase = Awaited<ReturnType<typeof createOutboxTestDatabase>>;
