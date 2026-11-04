import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { v7 as uuidv7 } from 'uuid';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import type { OutboxDb } from './types';

import { defineJob } from '../core/define-job';
import { ok } from '../core/outcome';
import { insertJob } from '../store/outbox-store';
import { pgSuiteDrivers, pgUrls, type PgDriver } from '../test-support/drivers';
import { createOutbox, RUNTIME } from './create-outbox';
import { jobRow, laneConnector, recordingEvents, testClock } from './runtime-harness.test-support';

const drivers = pgSuiteDrivers();

describe.skipIf(pgUrls().length === 0).each(drivers)('runtime sessions on $name', ({ create }) => {
  let driver: PgDriver;

  beforeAll(async () => {
    driver = await create();
    await driver.pool.query(
      'CREATE TABLE IF NOT EXISTS app_row (id int PRIMARY KEY, v int NOT NULL)',
    );
  });

  beforeEach(async () => {
    await driver.reset();
    await driver.pool.query('TRUNCATE app_row');
    await driver.pool.query('INSERT INTO app_row VALUES (1, 0)');
  });

  afterAll(() => driver.close());

  const job = defineJob({
    kind: 'test.job',
    payload: z.object({ n: z.number() }),
    timeoutMs: 1_000,
    handle: async () => ok(),
  });

  // port: submission-lock-order.postgres:77
  // port: submission-lock-order.postgres:263
  it('a hooked settle and a business transaction that holds its app row and re-enqueues the job never deadlock', async () => {
    const events = recordingEvents();
    const outbox = createOutbox({
      jobs: [job],
      connect: laneConnector(driver).connect,
      events: events.sink,
      clock: testClock(new Date(Date.now() + 60_000)),
      onTransition: async (tx: OutboxDb) => {
        await tx.execute(sql`UPDATE app_row SET v = v + 10 WHERE id = 1`);
      },
    });
    const { id } = await insertJob(
      driver.db,
      {
        id: uuidv7(),
        kind: job.kind,
        dedupeKey: 'shared',
        payload: { n: 1 },
        tenantKey: null,
        correlationId: null,
        metadata: {},
        delayMs: 0,
      },
      null,
    );

    const business = await driver.pool.connect();

    try {
      await business.query('BEGIN');
      await business.query('UPDATE app_row SET v = v + 1 WHERE id = 1');

      const sweep = outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 60_000 }, 'sweep');
      await new Promise((resolve) => setTimeout(resolve, 300));

      const again = await insertJob(
        drizzle({ client: business }),
        {
          id: uuidv7(),
          kind: job.kind,
          dedupeKey: 'shared',
          payload: { n: 2 },
          tenantKey: null,
          correlationId: null,
          metadata: {},
          delayMs: 0,
        },
        null,
      );
      await business.query('COMMIT');
      const report = await sweep;

      expect(again).toEqual({ id, kind: job.kind, tenantKey: null, created: false });
      expect(report).toMatchObject({ succeeded: 1 });
      expect(events.of('settle_retried')).toEqual([]);
      expect(await jobRow(driver, id)).toMatchObject({ status: 'succeeded' });
      const { rows } = await driver.pool.query('SELECT v FROM app_row WHERE id = 1');
      expect(rows[0].v).toBe(11);
    } finally {
      business.release();
    }
  });

  it('re-nudges a job another session holds (locked) instead of counting it as a miss', async () => {
    const events = recordingEvents();
    const sent: unknown[] = [];
    const outbox = createOutbox({
      jobs: [job],
      connect: laneConnector(driver).connect,
      events: events.sink,
      queue: { send: async (body, opts) => void sent.push({ body, opts }) },
      clock: testClock(new Date(Date.now() + 60_000)),
    });
    const created = (await outbox.enqueue(driver.db, job, {
      payload: { n: 1 },
      dedupeKey: 'held',
    })) as { id: string };
    const holder = await driver.pool.connect();

    try {
      await holder.query('BEGIN');
      await holder.query('SELECT 1 FROM "outbox"."job" WHERE id = $1 FOR UPDATE', [created.id]);

      await outbox[RUNTIME].processNudge([created.id], { maxJobs: 10, deadlineMs: 60_000 });

      expect(events.of('nudge_miss')).toEqual([
        { type: 'nudge_miss', jobId: created.id, result: 'locked' },
      ]);
      expect(sent).toEqual([
        { body: { v: 1, type: 'nudge', ids: [created.id] }, opts: { delaySeconds: 5 } },
      ]);
    } finally {
      await holder.query('ROLLBACK');
      holder.release();
    }
  });

  // port: submission-outbox-queue.server:170
  it('lanes run jobs on their own clients, never sharing one', async () => {
    const seen = new Set<unknown>();
    const concurrent = { now: 0, max: 0 };
    const slow = defineJob({
      kind: 'slow.job',
      payload: z.object({ n: z.number() }),
      timeoutMs: 5_000,
      handle: async (_, ctx) => {
        const { rows } = (await (ctx as unknown as { db: OutboxDb }).db.execute(
          sql`SELECT pg_backend_pid() AS pid`,
        )) as unknown as { rows: { pid: number }[] };
        seen.add(rows[0]!.pid);
        concurrent.now++;
        concurrent.max = Math.max(concurrent.max, concurrent.now);
        await new Promise((resolve) => setTimeout(resolve, 50));
        concurrent.now--;

        return ok();
      },
    });
    const outbox = createOutbox({
      jobs: [slow],
      connect: laneConnector(driver).connect,
      lanes: 3,
      clock: testClock(new Date(Date.now() + 60_000)),
    });

    for (let i = 0; i < 6; i++) {
      await outbox.enqueue(driver.db, slow, { payload: { n: i }, dedupeKey: `s${i}` });
    }

    const report = await outbox[RUNTIME].drain({ maxJobs: 50, deadlineMs: 60_000 }, 'sweep');

    expect(report.succeeded).toBe(6);
    expect(seen.size).toBe(3);
    expect(concurrent.max).toBe(3);
  });
});
