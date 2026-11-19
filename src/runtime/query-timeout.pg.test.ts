import { sql } from 'drizzle-orm';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import { defineJob } from '../core/define-job';
import { ok } from '../core/outcome';
import { createPgPoolConnector } from '../pg';
import { getJob } from '../store/outbox-store';
import { pgSuiteDrivers, pgUrls, type PgDriver } from '../test-support/drivers';
import { createOutbox, RUNTIME } from './create-outbox';
import { recordingEvents, testClock } from './runtime-harness.test-support';

describe.skipIf(pgUrls().length === 0).each(pgSuiteDrivers())(
  'query timeout fencing on $name',
  ({ create }) => {
    let driver: PgDriver;
    beforeAll(async () => {
      driver = await create();
      await driver.pool.query(
        'CREATE TABLE timeout_effect (id int PRIMARY KEY, value int NOT NULL)',
      );
      await driver.pool.query('INSERT INTO timeout_effect VALUES (1, 0)');
    });
    beforeEach(async () => {
      await driver.reset();
      await driver.pool.query('UPDATE timeout_effect SET value = 0');
    });
    afterAll(() => driver.close());

    it('retries a timed-out fenced commit on a fresh session and applies its effect exactly once', async () => {
      const pool = new Pool({ ...driver.pool.options, max: 1, query_timeout: 100 });
      pool.on('error', () => undefined);
      const events = recordingEvents();
      const pids: number[] = [];
      let handled = 0;
      const job = defineJob({
        kind: 'timeout.commit',
        payload: z.number(),
        timeoutMs: 1_000,
        handle: async (value) => {
          handled++;
          return value === 2
            ? ok()
            : ok({
                commit: async (tx) => {
                  const r = (await tx.execute(sql`SELECT pg_backend_pid() AS pid`)) as {
                    rows: { pid: number }[];
                  };
                  pids.push(r.rows[0]!.pid);
                  await tx.execute(sql`UPDATE timeout_effect SET value = value + 1 WHERE id = 1`);
                  if (pids.length === 1) await tx.execute(sql`SELECT pg_sleep(0.5)`);
                },
              });
        },
      });
      const outbox = createOutbox({
        jobs: [job],
        connect: createPgPoolConnector(pool),
        events: events.sink,
      });
      try {
        const a = await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'a' });
        const b = await outbox.enqueue(driver.db, job, { payload: 2, dedupeKey: 'b' });
        if (!('id' in a) || !('id' in b)) throw new Error('enqueue refused');
        const first = await outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 60_000 }, 'sweep');
        expect(first).toMatchObject({ succeeded: 1, settleFailed: 0 });
        expect(handled).toBe(1);
        // The old backend can hold its lock until its sleeping query ends; an intermediate
        // retry may time out on that lock too. Every retry must still use a different session.
        expect(pids.length).toBeGreaterThanOrEqual(2);
        expect(new Set(pids).size).toBe(pids.length);
        expect(events.of('settle_retried').length).toBeGreaterThanOrEqual(1);
        expect((await driver.pool.query('SELECT value FROM timeout_effect')).rows[0].value).toBe(1);
        await outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 60_000 }, 'sweep');
        expect(await getJob(driver.db, { id: a.id })).toMatchObject({
          status: 'succeeded',
          finishedAttempts: 1,
        });
        expect(await getJob(driver.db, { id: b.id })).toMatchObject({ status: 'succeeded' });
        // Recovery must not find an unfinished A and repeat its already committed effect.
        const later = createOutbox({
          jobs: [job],
          connect: createPgPoolConnector(pool),
          clock: testClock(new Date(Date.now() + 60_000)),
        });
        expect((await later[RUNTIME].recover(10)).retrying).toBe(0);
        expect((await driver.pool.query('SELECT value FROM timeout_effect')).rows[0].value).toBe(1);
      } finally {
        await pool.end();
      }
    });

    it('destroys a timed-out schedule transaction even when the callback catches the query error', async () => {
      const pool = new Pool({ ...driver.pool.options, max: 1, query_timeout: 100 });
      pool.on('error', () => undefined);
      const job = defineJob({
        kind: 'timeout.schedule',
        payload: z.number(),
        timeoutMs: 1_000,
        handle: async () => ok(),
      });
      const outbox = createOutbox({ jobs: [job], connect: createPgPoolConnector(pool) });
      try {
        await expect(
          outbox[RUNTIME].withConnection((c) =>
            c.db.transaction(async (tx) => {
              await tx.execute(sql`UPDATE timeout_effect SET value = value + 1 WHERE id = 1`);
              await Promise.resolve(tx.execute(sql`SELECT pg_sleep(0.5)`)).catch(() => undefined);
              // The connection must already be unusable: catching the timeout cannot permit a commit.
            }),
          ),
        ).rejects.toThrow();
        await new Promise((resolve) => setTimeout(resolve, 600));
        const c = await pool.connect();
        try {
          const pid = (await c.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
          expect(
            (await driver.pool.query('SELECT state FROM pg_stat_activity WHERE pid = $1', [pid]))
              .rows[0].state,
          ).toBe('idle');
          await c.query('BEGIN');
          await c.query('COMMIT');
        } finally {
          c.release();
        }
        expect((await driver.pool.query('SELECT value FROM timeout_effect')).rows[0].value).toBe(0);
      } finally {
        await pool.end();
      }
    });

    it('discards the final timed-out session when settlement retries are exhausted', async () => {
      const pool = new Pool({ ...driver.pool.options, max: 1, query_timeout: 100 });
      pool.on('error', () => undefined);
      const clock = testClock(); // Retry sleeps resolve immediately; SQL timeouts remain real.
      let slow = true;
      const job = defineJob({
        kind: 'timeout.exhausted',
        payload: z.number(),
        timeoutMs: 1_000,
        handle: async (value) =>
          value === 2
            ? ok()
            : ok({
                commit: async (tx) => {
                  await tx.execute(sql`UPDATE timeout_effect SET value = value + 1 WHERE id = 1`);
                  if (slow) await tx.execute(sql`SELECT pg_sleep(0.5)`);
                },
              }),
      });
      const outbox = createOutbox({ jobs: [job], connect: createPgPoolConnector(pool), clock });
      try {
        const a = await outbox.enqueue(driver.db, job, { payload: 1, dedupeKey: 'a' });
        const b = await outbox.enqueue(driver.db, job, { payload: 2, dedupeKey: 'b' });
        if (!('id' in a) || !('id' in b)) throw new Error('enqueue refused');
        expect(
          (await outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 60_000 }, 'sweep')).settleFailed,
        ).toBe(1);
        expect(pool.totalCount).toBe(0);
        await new Promise((resolve) => setTimeout(resolve, 600));
        await outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 60_000 }, 'sweep');
        expect(await getJob(driver.db, { id: b.id })).toMatchObject({ status: 'succeeded' });
        expect(await getJob(driver.db, { id: a.id })).toMatchObject({ status: 'processing' });
        expect((await driver.pool.query('SELECT value FROM timeout_effect')).rows[0].value).toBe(0);
        slow = false;
        clock.advance(60_000);
        await outbox[RUNTIME].recover(10);
        expect(
          (await outbox[RUNTIME].drain({ maxJobs: 1, deadlineMs: 60_000 }, 'sweep')).succeeded,
        ).toBe(1);
        expect((await driver.pool.query('SELECT value FROM timeout_effect')).rows[0].value).toBe(1);
      } finally {
        await pool.end();
      }
    });
  },
);
