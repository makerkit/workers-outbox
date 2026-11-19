import type { Socket } from 'node:net';

import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { withReplicas } from 'drizzle-orm/pg-core';
import { errorMonitor } from 'node:events';
import { Pool, type PoolClient } from 'pg';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { defineJob } from '../core/define-job';
import { ok } from '../core/outcome';
import { createPgPoolConnector } from '../pg';
import { createOutbox } from '../runtime/create-outbox';
import { recordingQueue } from '../runtime/runtime-harness.test-support';
import { pgSuiteDrivers, pgUrls } from '../test-support/drivers';
import { isGuardedPgClient } from './guard-client';
import { producerTransaction } from './producer-transaction';

function producerDatabase(pool: Pool, replica: Pool, mode: string) {
  const primary = drizzle({ client: pool });
  if (mode === 'bound transaction') {
    const transact = primary.transaction.bind(primary);
    primary.transaction = (fn, config) =>
      transact(fn, { isolationLevel: 'repeatable read', ...config });
  }
  const db = mode === 'replicas' ? withReplicas(primary, [drizzle({ client: replica })]) : primary;
  return { primary, db };
}

describe.skipIf(pgUrls().length === 0).each(pgSuiteDrivers())(
  'producer rollback on $name',
  ({ create }) => {
    it.each(['pool', 'checked-out client', 'replicas', 'bound transaction'] as const)(
      'a failed enqueue on a %s cannot commit with the next producer',
      async (mode) => {
        const driver = await create();
        const pool = new Pool({
          ...driver.pool.options,
          max: 1,
          query_timeout: 150,
          connectionTimeoutMillis: 500,
        });
        const { primary, db: appDb } = producerDatabase(pool, driver.pool, mode);
        const locker = await driver.pool.connect();
        const job = defineJob({
          kind: 'review.producer',
          payload: z.string(),
          timeoutMs: 1000,
          handle: async () => ok(),
        });
        const queue = recordingQueue();
        const outbox = createOutbox({
          jobs: [job],
          connect: createPgPoolConnector(pool),
          queue: queue.queue,
        });
        const orders = async () =>
          (await driver.pool.query('SELECT name FROM review_orders ORDER BY name')).rows.map(
            (r) => r.name,
          );
        const jobs = async () =>
          (
            await driver.pool.query('SELECT dedupe_key FROM outbox.job ORDER BY dedupe_key')
          ).rows.map((r) => r.dedupe_key);
        let pid: number | undefined;
        try {
          await driver.pool.query('CREATE TABLE review_orders (name text PRIMARY KEY)');
          await locker.query('BEGIN');
          await locker.query('LOCK TABLE outbox.job IN ACCESS EXCLUSIVE MODE');
          const held = mode === 'checked-out client' ? await pool.connect() : undefined;
          const firstDb = held ? drizzle({ client: held }) : appDb;
          const first = outbox.transaction(firstDb, async (tx, enqueue) => {
            const result = await tx.execute<{ pid: number; isolation: string }>(
              sql`SELECT pg_backend_pid() AS pid, current_setting('transaction_isolation') AS isolation`,
            );
            pid = result.rows[0]!.pid;
            if (mode === 'bound transaction')
              expect(result.rows[0]!.isolation).toBe('repeatable read');
            await tx.execute(sql`INSERT INTO review_orders VALUES ('A')`);
            const pending = enqueue(job, { payload: 'A', dedupeKey: 'A' });
            if (held || mode === 'replicas') await pending.catch(() => undefined);
            else await pending;
          });
          try {
            await expect(first).rejects.toThrow();
          } finally {
            held?.release();
          }
          expect(pid).toEqual(expect.any(Number));
          expect(queue.sent).toHaveLength(0);
          await locker.query('ROLLBACK');
          await expect
            .poll(
              async () =>
                (
                  await driver.pool.query('SELECT state FROM pg_stat_activity WHERE pid = $1', [
                    pid,
                  ])
                ).rows[0]?.state,
            )
            .toBeUndefined();
          expect(await orders()).toEqual([]);
          expect(await jobs()).toEqual([]);

          // A database wrapper can supply transaction policy through the ordinary method.
          const configuredDb: typeof primary = Object.create(primary, {
            transaction: {
              value(this: typeof primary, fn: Parameters<typeof primary.transaction>[0]) {
                return primary.transaction.call(this, fn, { isolationLevel: 'repeatable read' });
              },
            },
          });
          const secondDb =
            mode === 'replicas' || mode === 'bound transaction' ? appDb : configuredDb;
          const second = await outbox.transaction(secondDb, async (tx, enqueue) => {
            const result = await tx.execute<{ pid: number; isolation: string }>(
              sql`SELECT pg_backend_pid() AS pid, current_setting('transaction_isolation') AS isolation`,
            );
            expect(result.rows[0]!.isolation).toBe(
              mode === 'replicas' ? 'read committed' : 'repeatable read',
            );
            expect(result.rows[0]!.pid).not.toBe(pid);
            await tx.execute(sql`INSERT INTO review_orders VALUES ('B')`);
            return enqueue(job, { payload: 'B', dedupeKey: 'B' });
          });
          if (second.status !== 'created') throw new Error('expected B to be created');
          expect(await orders()).toEqual(['B']);
          expect(await jobs()).toEqual(['B']);
          expect(queue.sent.map(({ message }) => message)).toEqual([
            { v: 1, type: 'nudge', ids: [second.id] },
          ]);

          await expect(
            outbox.transaction(appDb, async (tx) => {
              await tx.execute(sql`INSERT INTO review_orders VALUES ('C')`);
              throw new Error('ordinary failure');
            }),
          ).rejects.toThrow('ordinary failure');
          expect(await orders()).toEqual(['B']);
        } finally {
          await locker.query('ROLLBACK');
          locker.release();
          await pool.end();
          await driver.close();
        }
      },
    );

    describe.each(['replicas', 'bound transaction'])('%s checkout lifecycle', (mode) => {
      it.each(['BEGIN read timeout', 'idle transaction timeout'] as const)(
        'commits the next producer on the same one-client pool after %s',
        async (failure) => {
          const driver = await create();
          const pool = new Pool({
            ...driver.pool.options,
            max: 1,
            connectionTimeoutMillis: 500,
            query_timeout: 200,
          });
          const borrowed = new Set<PoolClient>();
          pool.on('acquire', (client) => borrowed.add(client));
          pool.on('release', (_, client) => borrowed.delete(client));
          const { db } = producerDatabase(pool, driver.pool, mode);
          const job = defineJob({
            kind: 'checkout',
            payload: z.number(),
            timeoutMs: 1_000,
            handle: async () => ok(),
          });
          const queue = recordingQueue();
          const outbox = createOutbox({
            jobs: [job],
            queue: queue.queue,
            connect: createPgPoolConnector(pool),
          });
          try {
            await driver.pool.query('CREATE TABLE checkout_receipts (id int PRIMARY KEY)');
            const client = await pool.connect();
            const pid = (await client.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
            client.release();

            if (failure === 'BEGIN read timeout') {
              // PostgreSQL receives BEGIN. Hold its response until node-pg's read deadline.
              const { stream } = (client as unknown as { connection: { stream: Socket } })
                .connection;
              stream.pause();
              let called = false;
              try {
                await expect(
                  outbox.transaction(db, async () => {
                    called = true;
                  }),
                ).rejects.toMatchObject({
                  cause: { message: 'Query read timeout' },
                });
              } finally {
                stream.resume();
              }
              expect(called).toBe(false);
            } else {
              // errorMonitor observes PostgreSQL's FATAL without consuming the client's error event.
              const terminated = new Promise<{ code: string; handled: boolean }>((resolve) =>
                client.once(errorMonitor, (error: { code: string }) =>
                  resolve({
                    code: error.code,
                    handled: client.listenerCount('error') > 0,
                  }),
                ),
              );
              await expect(
                outbox.transaction(db, async (tx, enqueue) => {
                  await tx.execute(sql`INSERT INTO checkout_receipts VALUES (1)`);
                  await enqueue(job, { payload: 1, dedupeKey: 'failed' });
                  await tx.execute(sql`SET LOCAL idle_in_transaction_session_timeout = '100ms'`);
                  await terminated;
                }),
              ).rejects.toThrow();
              expect(await terminated).toEqual({ code: '25P03', handled: true });
            }
            expect(queue.sent).toHaveLength(0);

            const created = await outbox.transaction(db, async (tx, enqueue) => {
              const result = await tx.execute<{ pid: number }>(sql`SELECT pg_backend_pid() AS pid`);
              expect(result.rows[0]!.pid).not.toBe(pid);
              await tx.execute(sql`INSERT INTO checkout_receipts VALUES (2)`);
              return enqueue(job, { payload: 2, dedupeKey: 'next' });
            });
            if (created.status !== 'created') throw new Error('expected the next job to commit');
            expect((await driver.pool.query('SELECT id FROM checkout_receipts')).rows).toEqual([
              { id: 2 },
            ]);
            expect((await driver.pool.query('SELECT id FROM outbox.job')).rows).toEqual([
              { id: created.id },
            ]);
            expect(queue.sent.map(({ message }) => message)).toEqual([
              { v: 1, type: 'nudge', ids: [created.id] },
            ]);
            expect(borrowed.size).toBe(0);
            await expect
              .poll(
                async () =>
                  (
                    await driver.pool.query('SELECT pid FROM pg_stat_activity WHERE pid = $1', [
                      pid,
                    ])
                  ).rowCount,
              )
              .toBe(0);
          } finally {
            // If a regression strands the checkout, teardown must still finish.
            for (const client of borrowed) client.release(true);
            await pool.end();
            await driver.close();
          }
        },
      );
    });

    it('isolates overlapping async decorators from ordinary transactions on the same database', async () => {
      const driver = await create();
      const pool = new Pool({ ...driver.pool.options, max: 1, connectionTimeoutMillis: 500 });
      const db = drizzle({ client: pool });
      const native = db.transaction.bind(db);
      const originalSessionMethod = db._.session.transaction;
      let resume!: () => void;
      let entered!: () => void;
      const paused = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const resumeFirst = new Promise<void>((resolve) => {
        resume = resolve;
      });
      let first = true;
      db.transaction = async (fn, config) => {
        if (first) {
          first = false;
          entered();
          await resumeFirst;
        }
        return native((tx) => fn(tx), { isolationLevel: 'repeatable read', ...config });
      };
      const inspect = async (tx: Parameters<Parameters<typeof native>[0]>[0]) => {
        const session = tx._.session as unknown as { client: PoolClient };
        const { rows } = await tx.execute<{ level: string }>(
          sql`SELECT current_setting('transaction_isolation') AS level`,
        );
        return { guarded: isGuardedPgClient(session.client), isolation: rows[0]!.level };
      };
      const pending = producerTransaction(db, inspect);
      try {
        await paused;
        expect(await producerTransaction(db, inspect)).toEqual({
          guarded: true,
          isolation: 'repeatable read',
        });
        expect(await native(inspect)).toEqual({ guarded: false, isolation: 'read committed' });
        resume();
        expect(await pending).toEqual({ guarded: true, isolation: 'repeatable read' });
        expect(db._.session.transaction).toBe(originalSessionMethod);
      } finally {
        resume();
        await pending.catch(() => undefined);
        await pool.end();
        await driver.close();
      }
    });

    it('announces a job only after a real commit when a statement error was caught', async () => {
      const driver = await create();
      const db = drizzle({ client: driver.pool });
      const queue = recordingQueue();
      const job = defineJob({
        kind: 'review.receipt',
        payload: z.number(),
        timeoutMs: 1000,
        handle: async () => ok(),
      });
      const outbox = createOutbox({
        jobs: [job],
        connect: createPgPoolConnector(driver.pool),
        queue: queue.queue,
      });
      try {
        await driver.pool.query('CREATE TABLE receipts (id int PRIMARY KEY)');
        const write = (id: number, savepoint: boolean) =>
          outbox.transaction(db, async (tx, enqueue) => {
            await tx.execute(sql`INSERT INTO receipts VALUES (${id})`);
            const result = await enqueue(job, { payload: id, dedupeKey: String(id) });
            const optional = async (t: typeof tx) => {
              await t.execute(sql`SET LOCAL statement_timeout = '50ms'`);
              await t.execute(sql`SELECT pg_sleep(0.4)`);
            };
            await expect(savepoint ? tx.transaction(optional) : optional(tx)).rejects.toMatchObject(
              { cause: { code: '57014' } },
            );
            return result;
          });
        await expect(write(1, false)).rejects.toMatchObject({
          cause: { message: 'PostgreSQL rolled back the transaction at COMMIT' },
        });
        expect(queue.sent).toHaveLength(0);
        const result = await write(2, true);
        if (result.status !== 'created') throw new Error('expected a committed job');
        expect(queue.sent.map(({ message }) => message)).toEqual([
          { v: 1, type: 'nudge', ids: [result.id] },
        ]);
        expect((await driver.pool.query('SELECT id FROM receipts')).rows).toEqual([{ id: 2 }]);
        expect((await driver.pool.query('SELECT id FROM outbox.job')).rows).toEqual([
          { id: result.id },
        ]);
      } finally {
        await driver.close();
      }
    });
  },
);
