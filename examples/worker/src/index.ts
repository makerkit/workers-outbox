import { createOutbox, type QueueSender } from '@makerkit/workers-outbox';
import {
  createDlqHandler,
  createQueueHandler,
  createScheduledHandler,
  type QueueBatch,
} from '@makerkit/workers-outbox/cloudflare';
import { createPgConnector } from '@makerkit/workers-outbox/pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Client } from 'pg';

import { createMailer, welcomeEmail } from './jobs';
import { users } from './schema';

// Typed structurally, so the example needs no extra packages; `wrangler types` gives the real ones.
interface Env {
  /** The app's Hyperdrive config. */
  DB: { connectionString: string };
  /** A second Hyperdrive config for the outbox, same database, caching disabled. */
  OUTBOX_DB: { connectionString: string };
  OUTBOX_QUEUE: QueueSender;
  CF_VERSION_METADATA: { timestamp: string };
  RESEND_API_KEY: string;
  /** Optional: the mail API's base URL (the e2e test points it at a local stub). */
  MAILER_URL?: string;
}

interface ExecutionContext {
  waitUntil(promise: Promise<unknown>): void;
}

const log = (event: unknown) => console.log(JSON.stringify(event));

/** Postgres 23505, wherever drizzle nests the driver's error. */
function isUniqueViolation(error: unknown): boolean {
  for (let e = error; e instanceof Error; e = e.cause) {
    if ((e as { code?: unknown }).code === '23505') {
      return true;
    }
  }

  return false;
}

// One outbox per invocation: it opens connections lazily, never at construction.
const outboxFor = (env: Env) =>
  createOutbox({
    jobs: [welcomeEmail],
    deps: { mailer: createMailer(env.RESEND_API_KEY, env.MAILER_URL) },
    connect: createPgConnector(env.OUTBOX_DB.connectionString),
    queue: env.OUTBOX_QUEUE,
    version: env.CF_VERSION_METADATA,
    events: log,
  });

const queue = createQueueHandler(outboxFor);
const dlq = createDlqHandler({ events: log });

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (request.method !== 'POST' || url.pathname !== '/signup') {
      return new Response('not found', { status: 404 });
    }

    // Validate at the edge with the job's own schema: a bad request never opens a transaction.
    const body = welcomeEmail.payload.safeParse(await request.json().catch(() => null));

    if (!body.success) {
      return Response.json({ error: 'invalid email' }, { status: 400 });
    }

    const { email } = body.data;
    const client = new Client({ connectionString: env.DB.connectionString });
    // Before connect(): a session error outside a query would otherwise crash the Worker.
    client.on('error', (error) => log({ type: 'app_db_error', message: error.message }));
    await client.connect();

    try {
      const db = drizzle({ client });

      // The user row and the job commit together, or neither does. The job is nudged after
      // commit, so the email goes out now rather than at the next cron sweep.
      const user = await outboxFor(env).transaction(db, async (tx, enqueue) => {
        const [row] = await tx.insert(users).values({ email }).returning();
        const job = await enqueue(welcomeEmail, {
          payload: { email },
          dedupeKey: `welcome:${row!.id}`,
        });

        // enqueue reports a rejected job instead of throwing: throw, so the user row rolls back
        // with it rather than committing without its email.
        if (job.status !== 'created' && job.status !== 'exists') {
          throw new Error(`welcome email not enqueued: ${job.status}`);
        }

        return row!;
      });

      return Response.json({ id: user.id }, { status: 201 });
    } catch (error) {
      // The unique email rolled the whole transaction back: no user, and no job either.
      if (isUniqueViolation(error)) {
        return Response.json({ error: 'email already registered' }, { status: 409 });
      }

      throw error;
    } finally {
      ctx.waitUntil(client.end());
    }
  },

  // One queue() export serves both consumers: the outbox queue and its dead-letter queue.
  queue: (batch: QueueBatch, env: Env, ctx: ExecutionContext) =>
    batch.queue === 'outbox-dlq' ? dlq(batch) : queue(batch, env, ctx),

  scheduled: createScheduledHandler(outboxFor),
};
