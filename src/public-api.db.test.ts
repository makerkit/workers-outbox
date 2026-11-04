import type { NodePgDatabase } from 'drizzle-orm/node-postgres';

// The documented developer surface, imported only through the package entry points, so README
// drift shows up here as a type error or a failing test.
import { sql } from 'drizzle-orm';
import { afterAll, beforeEach, describe, expect, expectTypeOf, it } from 'vitest';
import { z } from 'zod';

import {
  createOutbox,
  defineJob,
  defineJobFor,
  ok,
  retry,
  type JobContext,
  type OutboxOptions,
  type QueueSender,
} from './index';
import { createOutboxTestDatabase, createTestClock, runUntilIdle } from './testing';

const test = await createOutboxTestDatabase();

afterAll(() => test.close());

beforeEach(() => test.reset());

const status = async (id: string) => {
  const result = await test.db.execute<{ status: string }>(
    sql`SELECT status FROM "outbox"."job" WHERE id = ${id}`,
  );

  return result.rows[0]?.status;
};

const idOf = (result: { status: string } & ({ id: string } | object)) =>
  'id' in result ? result.id : '';

function recordingQueue() {
  const sent: unknown[] = [];
  /** `delaySeconds` of each send, in order; 0 when sent without a delay. */
  const delays: number[] = [];
  const queue: QueueSender = {
    send: async (body, options) => {
      sent.push(body);
      delays.push(options?.delaySeconds ?? 0);
    },
  };

  return { queue, sent, delays };
}

describe('testing harness', () => {
  it('README: runs a job end to end on the in-memory database', async () => {
    const seen: string[] = [];
    const welcome = defineJob({
      kind: 'welcome',
      timeoutMs: 1_000,
      payload: z.object({ userId: z.string() }),
      handle: async ({ userId }) => {
        seen.push(userId);

        return ok();
      },
    });
    const outbox = createOutbox({ jobs: [welcome], connect: test.connect });

    await outbox.enqueue(test.db, welcome, { payload: { userId: 'u1' }, dedupeKey: 'u1' });
    const report = await runUntilIdle(outbox);

    expect(seen).toEqual(['u1']);
    expect(report).toMatchObject({ claimed: 1, succeeded: 1 });
  });

  it('with a test clock, skips ahead through retries until the job settles', async () => {
    let calls = 0;
    const flaky = defineJob({
      kind: 'flaky',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => (++calls < 3 ? retry({ code: 'busy', after: 60_000 }) : ok()),
    });
    const clock = createTestClock();
    const outbox = createOutbox({ jobs: [flaky], connect: test.connect, clock });

    const job = await outbox.enqueue(test.db, flaky, { payload: 1, dedupeKey: 'f' });
    const report = await runUntilIdle(outbox, { clock, db: test.db });

    expect(calls).toBe(3);
    expect(report).toMatchObject({ retrying: 2, succeeded: 1 });
    expect(await status(idOf(job))).toBe('succeeded');
  });

  it('without a clock, leaves jobs that are not due yet', async () => {
    const later = defineJob({
      kind: 'later',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({ jobs: [later], connect: test.connect });

    const job = await outbox.enqueue(test.db, later, {
      payload: 1,
      dedupeKey: 'l',
      delayMs: 3_600_000,
    });

    expect(await runUntilIdle(outbox)).toMatchObject({ claimed: 0 });
    expect(await status(idOf(job))).toBe('pending');
  });

  it('throws rather than hang on a job that retries forever', async () => {
    const forever = defineJob({
      kind: 'forever',
      timeoutMs: 1_000,
      payload: z.number(),
      retry: { maxAttempts: 1_000 },
      handle: async () => retry({ code: 'busy', after: 1_000 }),
    });
    const clock = createTestClock();
    const outbox = createOutbox({ jobs: [forever], connect: test.connect, clock });
    await outbox.enqueue(test.db, forever, { payload: 1, dedupeKey: 'x' });

    await expect(runUntilIdle(outbox, { clock, db: test.db, maxRounds: 5 })).rejects.toThrow(
      /still busy after 5 rounds/,
    );
  });

  it('requires db alongside clock', async () => {
    const noop = defineJob({
      kind: 'noop',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => ok(),
    });
    const outbox = createOutbox({ jobs: [noop], connect: test.connect });

    await expect(runUntilIdle(outbox, { clock: createTestClock() })).rejects.toThrow(/pass `db`/);
  });
});

describe('outbox.transaction', () => {
  const email = defineJob({
    kind: 'email',
    lane: 'mail',
    timeoutMs: 1_000,
    payload: z.string(),
    handle: async () => ok(),
  });

  it('nudges the jobs it created, on their lane, once the transaction commits', async () => {
    const mail = recordingQueue();
    const outbox = createOutbox({
      jobs: [email],
      connect: test.connect,
      queue: { mail: mail.queue },
    });
    let sentInside = -1;

    const value = await outbox.transaction(test.db, async (tx, enqueue) => {
      const first = await enqueue(email, { payload: 'a', dedupeKey: 'a' });
      await enqueue(email, { payload: 'a', dedupeKey: 'a' }); // exists: no second nudge
      sentInside = mail.sent.length;

      return idOf(first);
    });

    expect(sentInside).toBe(0);
    expect(mail.sent).toEqual([{ v: 1, type: 'nudge', ids: [value] }]);
    expect(await status(value)).toBe('pending');
  });

  it('nudges a delayed job for when it falls due, and leaves delays past 24 h to the sweep', async () => {
    const mail = recordingQueue();
    const outbox = createOutbox({
      jobs: [email],
      connect: test.connect,
      queue: { mail: mail.queue },
    });

    const ids = await outbox.transaction(test.db, async (_tx, enqueue) => {
      const now = await enqueue(email, { payload: 'now', dedupeKey: 'now' });
      const minute = await enqueue(email, { payload: 'm1', dedupeKey: 'm1', delayMs: 60_000 });
      const minuteToo = await enqueue(email, { payload: 'm2', dedupeKey: 'm2', delayMs: 60_000 });
      // Rounded up, so the nudge never arrives before the job is due.
      const partial = await enqueue(email, { payload: 'p', dedupeKey: 'p', delayMs: 1_500 });
      await enqueue(email, { payload: 'd', dedupeKey: 'd', delayMs: 2 * 86_400_000 });

      return [now, minute, minuteToo, partial].map(idOf);
    });

    const [now, minute, minuteToo, partial] = ids;
    const byDelay = Object.fromEntries(mail.delays.map((delay, i) => [delay, mail.sent[i]]));

    expect(mail.sent).toHaveLength(3); // one message per delay; nothing for the 2-day job
    expect(byDelay).toEqual({
      0: { v: 1, type: 'nudge', ids: [now] },
      60: { v: 1, type: 'nudge', ids: [minute, minuteToo] },
      2: { v: 1, type: 'nudge', ids: [partial] },
    });
  });

  it('nudges nothing and keeps nothing when the work throws', async () => {
    const mail = recordingQueue();
    const outbox = createOutbox({
      jobs: [email],
      connect: test.connect,
      queue: { mail: mail.queue },
    });

    await expect(
      outbox.transaction(test.db, async (_tx, enqueue) => {
        await enqueue(email, { payload: 'b', dedupeKey: 'b' });
        throw new Error('business rule failed');
      }),
    ).rejects.toThrow('business rule failed');

    expect(mail.sent).toEqual([]);
    const result = await test.db.execute<{ n: number }>(
      sql`SELECT count(*)::int AS n FROM "outbox"."job"`,
    );
    expect(result.rows[0]?.n).toBe(0);
  });

  it('accepts a node-postgres drizzle db as well (type-level)', () => {
    const outbox = createOutbox({ jobs: [email], connect: test.connect });
    const withPg = (db: NodePgDatabase) =>
      outbox.transaction(db, async (tx, enqueue) => {
        expectTypeOf(tx).toHaveProperty('select');

        return enqueue(email, { payload: 'c', dedupeKey: 'c' });
      });

    expectTypeOf(withPg).returns.resolves.toHaveProperty('status');
  });

  it("passes the driver's own transaction type through", async () => {
    const outbox = createOutbox({ jobs: [email], connect: test.connect });

    await outbox.transaction(test.db, async (tx) => {
      expectTypeOf(tx).toHaveProperty('select');
    });
  });
});

describe('deps typing', () => {
  interface Mailer {
    send(to: string): Promise<void>;
  }

  const needsMailer = defineJob({
    kind: 'needs-mailer',
    timeoutMs: 1_000,
    payload: z.string(),
    handle: async (to, ctx: JobContext<{ mailer: Mailer }>) => {
      await ctx.deps.mailer.send(to);

      return ok();
    },
  });

  const needsClock = defineJobFor<{ now: () => number }>()({
    kind: 'needs-clock',
    timeoutMs: 1_000,
    payload: z.string(),
    handle: async (_, ctx) => {
      expectTypeOf(ctx.deps.now).toEqualTypeOf<() => number>();

      return ok();
    },
  });

  const noDeps = defineJob({
    kind: 'no-deps',
    timeoutMs: 1_000,
    payload: z.string(),
    handle: async () => ok(),
  });

  const mailer: Mailer = { send: async () => undefined };

  it('requires deps once a job declares them, and checks their shape', () => {
    // @ts-expect-error deps is missing
    createOutbox({ jobs: [needsMailer], connect: test.connect });
    // @ts-expect-error deps has the wrong shape
    createOutbox({ jobs: [needsMailer], connect: test.connect, deps: { mailer: 1 } });

    createOutbox({ jobs: [needsMailer], connect: test.connect, deps: { mailer } });
  });

  it('requires every job’s deps at once', () => {
    // @ts-expect-error needs-clock's `now` is missing
    createOutbox({ jobs: [needsMailer, needsClock], connect: test.connect, deps: { mailer } });

    createOutbox({
      jobs: [needsMailer, needsClock, noDeps],
      connect: test.connect,
      deps: { mailer, now: () => 0 },
    });
  });

  it('keeps deps optional when no job declares any', () => {
    createOutbox({ jobs: [noDeps], connect: test.connect });
    expectTypeOf<OutboxOptions<[typeof noDeps]>>().toHaveProperty('jobs');
  });
});
