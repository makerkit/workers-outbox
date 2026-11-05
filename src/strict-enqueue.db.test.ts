// `createOutbox({ strictEnqueue: true })`, through the package entry points only.
import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, expectTypeOf, it } from 'vitest';
import { z } from 'zod';

import { createQueueHandler, createScheduledHandler, defineSchedule } from './cloudflare';
import {
  createOutbox,
  defineJob,
  EnqueueRefusedError,
  ok,
  type AcceptedEnqueueResult,
  type EnqueueResult,
  type Outbox,
  type OutboxEvent,
  type StrictOutbox,
} from './index';
import { createOutboxTestDatabase, runUntilIdle } from './testing';

const test = await createOutboxTestDatabase();

afterAll(() => test.close());

beforeAll(async () => {
  await test.db.execute(sql`CREATE TABLE note (id text PRIMARY KEY)`);
});

beforeEach(async () => {
  await test.reset();
  await test.db.execute(sql`TRUNCATE note`);
});

const welcome = defineJob({
  kind: 'welcome',
  timeoutMs: 1_000,
  payload: z.object({ userId: z.string() }),
  handle: async () => ok(),
});

const other = defineJob({
  kind: 'other',
  timeoutMs: 1_000,
  payload: z.object({ userId: z.string() }),
  handle: async () => ok(),
});

const notes = async () =>
  (await test.db.execute<{ id: string }>(sql`SELECT id FROM note ORDER BY id`)).rows.map(
    (row) => row.id,
  );

const jobCount = async () =>
  (await test.db.execute<{ n: number }>(sql`SELECT count(*)::int AS n FROM "outbox"."job"`)).rows[0]
    ?.n;

const strictOutbox = (events?: (event: OutboxEvent) => void) =>
  createOutbox({
    jobs: [welcome, other],
    connect: test.connect,
    strictEnqueue: true,
    ...(events ? { events } : {}),
  });

describe('strictEnqueue: types', () => {
  it('returns a StrictOutbox whose enqueues resolve only to accepted results', () => {
    const strict = strictOutbox();
    const lenient = createOutbox({ jobs: [welcome], connect: test.connect });

    expectTypeOf(strict).toEqualTypeOf<StrictOutbox<readonly [typeof welcome, typeof other]>>();
    expectTypeOf(strict.enqueue).returns.resolves.toEqualTypeOf<AcceptedEnqueueResult>();
    expectTypeOf(lenient).toEqualTypeOf<Outbox<readonly [typeof welcome]>>();
    expectTypeOf(lenient.enqueue).returns.resolves.toEqualTypeOf<EnqueueResult>();

    // A non-literal flag can't promise a throw: it gets the plain Outbox.
    const flag = Math.random() > 2;
    expectTypeOf(
      createOutbox({ jobs: [welcome], connect: test.connect, strictEnqueue: flag }),
    ).toEqualTypeOf<Outbox<readonly [typeof welcome]>>();
  });

  it('is accepted wherever an Outbox is', () => {
    const strict = strictOutbox();
    const asOutbox: Outbox = strict;

    createQueueHandler(() => strict);
    createScheduledHandler(() => strict);
    expectTypeOf(runUntilIdle).toBeCallableWith(strict);
    expect(asOutbox).toBe(strict);

    // Not the other way round: a lenient outbox's enqueue can resolve to a refusal.
    const lenient = createOutbox({ jobs: [welcome, other], connect: test.connect });
    // @ts-expect-error -- Outbox is not a StrictOutbox
    const notStrict: StrictOutbox = lenient;
    void notStrict;
  });
});

describe('strictEnqueue: refusals throw', () => {
  it('enqueue(tx) throws EnqueueRefusedError on invalid_payload, rolling back the write', async () => {
    const outbox = strictOutbox();

    const write = test.db.transaction(async (tx) => {
      await tx.execute(sql`INSERT INTO note (id) VALUES ('n1')`);
      // @ts-expect-error -- wrong payload shape on purpose
      await outbox.enqueue(tx, welcome, { payload: { userId: 1 }, dedupeKey: 'u1' });
    });

    const error = await write.catch((thrown: unknown) => thrown);

    expect(error).toBeInstanceOf(EnqueueRefusedError);
    expect(error).toMatchObject({
      name: 'EnqueueRefusedError',
      kind: 'welcome',
      result: { status: 'invalid_payload' },
    });
    expect(await notes()).toEqual([]);
    expect(await jobCount()).toBe(0);
  });

  it('transaction enqueue throws on dedupe_conflict, rolling back the whole transaction', async () => {
    const outbox = strictOutbox();

    await outbox.enqueue(test.db, welcome, { payload: { userId: 'u1' }, dedupeKey: 'shared' });

    const error = await outbox
      .transaction(test.db, async (tx, enqueue) => {
        await tx.execute(sql`INSERT INTO note (id) VALUES ('n1')`);
        await enqueue(other, { payload: { userId: 'u1' }, dedupeKey: 'shared' });
      })
      .catch((thrown: unknown) => thrown);

    expect(error).toBeInstanceOf(EnqueueRefusedError);
    expect((error as EnqueueRefusedError).result).toMatchObject({
      status: 'dedupe_conflict',
      existingKind: 'welcome',
    });
    expect((error as Error).message).toMatch(/"other" refused \(dedupe_conflict\)/);
    expect(await notes()).toEqual([]);
    expect(await jobCount()).toBe(1);
  });

  it('returns created and exists as usual', async () => {
    const outbox = strictOutbox();
    const request = { payload: { userId: 'u1' }, dedupeKey: 'u1' };

    const first = await outbox.enqueue(test.db, welcome, request);
    const second = await outbox.enqueue(test.db, welcome, request);

    expect(first.status).toBe('created');
    expect(second).toEqual({ status: 'exists', id: first.id, kind: 'welcome' });
  });

  it("a run schedule's refused enqueue rolls back the occurrence and reports schedule_failed", async () => {
    const events: OutboxEvent[] = [];
    const outbox = strictOutbox((event) => events.push(event));
    const cron = '0 0 * * *';
    const fanOut = defineSchedule({
      cron,
      name: 'fan-out',
      run: async ({ tx, enqueue }) => {
        await tx.execute(sql`INSERT INTO note (id) VALUES ('n1')`);
        await enqueue(welcome, { payload: { userId: 'ok' }, dedupeKey: 'ok' });
        await enqueue(welcome, { payload: { userId: 2 } as never, dedupeKey: 'bad' });
      },
    });
    const tick = createScheduledHandler(() => outbox, { schedules: [fanOut] });

    await tick({ cron, scheduledTime: Date.UTC(2026, 0, 5) }, {}, {});

    expect(await notes()).toEqual([]);
    expect(await jobCount()).toBe(0);
    expect(events.find((event) => event.type === 'schedule_failed')).toMatchObject({
      name: 'fan-out',
      attempts: 1,
      error: { name: 'EnqueueRefusedError' },
    });
  });

  it('strict outboxes still run jobs through runUntilIdle', async () => {
    const outbox = strictOutbox();

    await outbox.enqueue(test.db, welcome, { payload: { userId: 'u1' }, dedupeKey: 'u1' });

    expect(await runUntilIdle(outbox)).toMatchObject({ claimed: 1, succeeded: 1 });
  });
});

describe('default mode is unchanged', () => {
  it('returns refusals instead of throwing', async () => {
    const outbox = createOutbox({ jobs: [welcome, other], connect: test.connect });

    await outbox.enqueue(test.db, welcome, { payload: { userId: 'u1' }, dedupeKey: 'shared' });

    const conflict = await outbox.enqueue(test.db, other, {
      payload: { userId: 'u1' },
      dedupeKey: 'shared',
    });
    const invalid = await outbox.enqueue(test.db, welcome, {
      payload: { userId: 1 } as never,
      dedupeKey: 'x',
    });

    expect(conflict).toMatchObject({ status: 'dedupe_conflict', existingKind: 'welcome' });
    expect(invalid).toMatchObject({ status: 'invalid_payload' });
  });
});
