import { createOutbox, defineJob, retry } from '@makerkit/workers-outbox';
import { createScheduledHandler } from '@makerkit/workers-outbox/cloudflare';
import { createPgConnector } from '@makerkit/workers-outbox/pg';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { withReplicas } from 'drizzle-orm/pg-core';
import { Pool } from 'pg';
import { z } from 'zod';

import example from '../src/index';

const job = defineJob({
  kind: 'e2e.read-timeout',
  payload: z.number(),
  timeoutMs: 5_000,
  async handle(_, ctx) {
    try {
      await ctx.db.execute(sql`SELECT pg_sleep(0.6)`);
    } catch (error) {
      if (
        error instanceof Error &&
        error.cause instanceof Error &&
        error.cause.message === 'Query read timeout'
      ) {
        // Also cover a decision made after the server-side sleep has finished.
        if (ctx.attempt % 2 === 0) await new Promise((resolve) => setTimeout(resolve, 600));
        return retry({ code: 'database_busy', after: 120_000 });
      }
      throw error;
    }
    throw new Error('expected a client-side timeout');
  },
});

const fixture: typeof example = {
  ...example,
  async fetch(...args: Parameters<typeof example.fetch>) {
    const [request, env] = args;
    if (request.method !== 'POST' || new URL(request.url).pathname !== '/test/read-timeout')
      return example.fetch(...args);
    const connectionString = env.OUTBOX_DB.connectionString;
    const connect = createPgConnector({ connectionString, query_timeout: 150 });
    const outbox = createOutbox({ jobs: [job], connect });
    // Exercise the delegated producer in workerd as well as the consumer's timeout recovery.
    const pool = new Pool({ connectionString, max: 1, connectionTimeoutMillis: 1_000 });
    const primary = drizzle({ client: pool });
    try {
      await outbox.transaction(withReplicas(primary, [primary]), (_tx, enqueue) =>
        enqueue(job, { payload: 1, dedupeKey: job.kind }),
      );
    } finally {
      await pool.end();
    }
    await createScheduledHandler(() => outbox)({}, {}, {});
    return new Response(null, { status: 204 });
  },
};

export default fixture;
