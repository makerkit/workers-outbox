// R2/R3 against real PostgreSQL: pool exhaustion, connection destruction, concurrent locks and
// native timeout enforcement. pglite shares the engine version but not connection or timer behavior.
import { sql } from 'drizzle-orm';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import type { Clock, LaneConnection, OutboxDb } from './types';

import { defineJob, HANDLER_GRACE_MS, LEASE_MARGIN_MS } from '../core/define-job';
import { ok, retry } from '../core/outcome';
import { createPgConnector, createPgPoolConnector } from '../pg';
import { pgSuiteDrivers, pgUrls, type PgDriver } from '../test-support/drivers';
import { createOutbox, RUNTIME } from './create-outbox';
import { recordingEvents, recordingQueue } from './runtime-harness.test-support';

const drivers = pgSuiteDrivers();
const BUDGET = { maxJobs: 10, deadlineMs: 180_000 };

/** Records each lane's first end, so a test can assert destroy-before-reconnect. */
function logged(connector: () => Promise<LaneConnection>, { health = false } = {}) {
  const log: string[] = [];

  const connect = async (): Promise<LaneConnection> => {
    const connection = await connector();
    log.push('connect');
    let ended = false;
    const end = (how: 'close' | 'destroy') => () => {
      if (!ended) {
        ended = true;
        log.push(how);
      }

      return connection[how]();
    };

    const isBroken = connection.isBroken?.bind(connection);

    return {
      db: connection.db,
      close: end('close'),
      destroy: end('destroy'),
      ...(health && isBroken ? { isBroken } : {}),
    };
  };

  return { connect, log };
}

/** The shipped pool adapter over a real pool of exactly one client. */
function oneClientPool(driver: PgDriver) {
  const pool = new Pool({ ...driver.pool.options, max: 1 });

  return { pool, ...logged(createPgPoolConnector(pool)) };
}

/** The shipped per-lane `pg.Client` adapter (the Hyperdrive shape). */
function clientConnector(driver: PgDriver) {
  const { connect, log } = logged(createPgConnector(driver.pool.options.connectionString ?? ''));

  return { pool: { end: async () => undefined }, connect, log };
}

const connectors = [
  { name: 'pool adapter', make: oneClientPool },
  { name: 'client adapter', make: clientConnector },
];

/** Real time, shifted forward on demand: a handler can "use up" most of its lease instantly. */
function shiftedClock() {
  let offset = 0;
  const clock: Clock & { skip(ms: number): void } = {
    dbNow: () => null,
    nowMs: () => Date.now() + offset,
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    skip: (ms) => {
      offset += ms;
    },
  };

  return clock;
}

/** Leaves `remainingMs` of settlement budget once the handler returns. */
const consumeLease = (timeoutMs: number, remainingMs: number) =>
  timeoutMs + HANDLER_GRACE_MS + LEASE_MARGIN_MS - 2_000 - remainingMs;

const within = <T>(work: Promise<T>, ms: number) =>
  Promise.race([
    work,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`still running after ${ms}ms`)), ms),
    ),
  ]);

describe.skipIf(pgUrls().length === 0).each(drivers)('settlement on $name', ({ create }) => {
  let driver: PgDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  const status = async (id: string) => {
    const { rows } = await driver.pool.query<{ status: string }>(
      `SELECT status FROM "outbox"."job" WHERE id = $1`,
      [id],
    );

    return rows[0]?.status;
  };

  it.each([
    { name: 'pool adapter, isBroken', pooled: true, health: true },
    { name: 'pool adapter, classifier only', pooled: true, health: false },
    { name: 'client adapter, isBroken', pooled: false, health: true },
    { name: 'client adapter, classifier only', pooled: false, health: false },
  ])(
    'settles on a fresh lane when the session died while the handler ran ($name)',
    async ({ pooled, health }) => {
      const pool = pooled ? new Pool({ ...driver.pool.options, max: 1 }) : undefined;
      const { connect, log } = logged(
        pool
          ? createPgPoolConnector(pool)
          : createPgConnector(driver.pool.options.connectionString ?? ''),
        { health },
      );
      const events = recordingEvents();
      let calls = 0;
      const job = defineJob({
        kind: 'pg.idle-drop',
        timeoutMs: 5_000,
        payload: z.number(),
        handle: async (_, ctx) => {
          calls++;
          const db = (ctx as unknown as { db: OutboxDb }).db;
          const { rows } = (await db.execute(sql`SELECT pg_backend_pid() AS pid`)) as unknown as {
            rows: { pid: number }[];
          };
          // Hyperdrive failover / origin restart while the handler waits on an external call.
          await driver.pool.query('SELECT pg_terminate_backend($1)', [rows[0]?.pid]);
          await new Promise((resolve) => setTimeout(resolve, 300));

          return ok();
        },
      });
      const outbox = createOutbox({ jobs: [job], connect, events: events.sink });

      try {
        const enqueued = await outbox.enqueue(driver.db as never, job, {
          payload: 1,
          dedupeKey: 'idle-drop',
        });
        const report = await within(outbox[RUNTIME].drain(BUDGET, 'sweep'), 15_000);

        expect(report).toMatchObject({ succeeded: 1 });
        expect(calls).toBe(1);
        expect(await status('id' in enqueued ? enqueued.id : '')).toBe('succeeded');
        expect(events.events.filter((e) => e.type === 'settle_failed')).toEqual([]);
        // The dead session is destroyed, never closed back into a pool.
        expect(log.slice(0, 3)).toEqual(['connect', 'destroy', 'connect']);
      } finally {
        await pool?.end();
      }
    },
  );

  it('R2: records nudges on the settling lane, so a one-client pool does not deadlock', async () => {
    const pool = oneClientPool(driver);
    const job = defineJob({
      kind: 'pg.retry',
      timeoutMs: 1_000,
      payload: z.number(),
      handle: async () => retry({ code: 'busy' }),
    });
    const outbox = createOutbox({
      jobs: [job],
      connect: pool.connect,
      recordNudges: true,
      queue: recordingQueue().queue,
    });

    try {
      const result = await outbox.enqueue(driver.db as never, job, { payload: 1, dedupeKey: 'r2' });
      const report = await within(outbox[RUNTIME].drain(BUDGET, 'sweep'), 10_000);

      expect(report.retrying).toBe(1);
      const { rows } = await driver.pool.query(
        `SELECT nudge_sent_at FROM "outbox"."job" WHERE id = $1`,
        ['id' in result ? result.id : ''],
      );
      expect(rows[0]?.nudge_sent_at).not.toBeNull();

      expect(await within(outbox.retryNow('id' in result ? result.id : ''), 10_000)).toBe(
        'rescheduled',
      );
    } finally {
      await pool.pool.end();
    }
  });

  it('R2: destroys a runaway session before reconnecting, on a one-client pool', async () => {
    const pool = oneClientPool(driver);
    let calls = 0;
    const job = defineJob({
      kind: 'pg.runaway',
      timeoutMs: 50,
      payload: z.number(),
      handle: async (_, ctx) => {
        if (++calls === 1) {
          // Ignores its signal; its session is busy in a query the runtime can't cancel.
          await Promise.resolve(
            (ctx as unknown as { db: OutboxDb }).db.execute(sql`SELECT pg_sleep(30)`),
          ).catch(() => undefined);
        }

        return ok();
      },
    });
    const outbox = createOutbox({ jobs: [job], connect: pool.connect });

    try {
      await outbox.enqueue(driver.db as never, job, { payload: 1, dedupeKey: 'a' });
      const second = await outbox.enqueue(driver.db as never, job, { payload: 2, dedupeKey: 'b' });

      const report = await within(outbox[RUNTIME].drain(BUDGET, 'sweep'), 15_000);

      expect(report).toMatchObject({ retrying: 1, succeeded: 1 });
      expect(pool.log).toEqual(['connect', 'destroy', 'connect', 'close']);
      expect(await status('id' in second ? second.id : '')).toBe('succeeded');
    } finally {
      await pool.pool.end();
    }
  });

  it.each(connectors)(
    'R3: transaction_timeout ends a hook that runs past the settlement deadline ($name)',
    async ({ make }) => {
      const clock = shiftedClock();
      const events = recordingEvents();
      const job = defineJob({
        kind: 'pg.slow-hook',
        timeoutMs: 1_000,
        payload: z.number(),
        handle: async () => {
          clock.skip(consumeLease(1_000, 1_500));

          return ok();
        },
      });
      const pool = make(driver);
      const outbox = createOutbox({
        jobs: [job],
        clock,
        events: events.sink,
        connect: pool.connect,
        // A hook that keeps the transaction busy across statements, then idles in application code.
        onTransition: async (tx) => {
          await tx.execute(sql`SELECT pg_sleep(0.4)`);
          await tx.execute(sql`SELECT pg_sleep(0.4)`);
          await new Promise((resolve) => setTimeout(resolve, 30_000));
        },
      });

      try {
        const enqueued = await outbox.enqueue(driver.db as never, job, {
          payload: 1,
          dedupeKey: 'r3',
        });
        const id = 'id' in enqueued ? enqueued.id : '';
        const started = Date.now();

        const report = await within(outbox[RUNTIME].drain(BUDGET, 'sweep'), 10_000);

        // ~1.5 s of budget plus the watchdog's slack, never the hook's 30 s.
        expect(Date.now() - started).toBeLessThan(5_000);
        expect(report.settleFailed).toBe(1);
        expect(await status(id)).toBe('processing');
        // The session holding the abandoned transaction was destroyed, not pooled.
        expect(pool.log).toContain('destroy');
        const { rows } = await driver.pool.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM pg_stat_activity WHERE state LIKE 'idle in transaction%' AND datname = current_database()`,
        );
        expect(rows[0]?.n).toBe(0);
      } finally {
        await pool.pool.end();
      }
    },
  );

  it.each(connectors)(
    'R3: a plain settle blocked by another session gives up at the deadline ($name)',
    async ({ make }) => {
      const clock = shiftedClock();
      const events = recordingEvents();
      let locked!: () => void;
      const rowLocked = new Promise<void>((resolve) => {
        locked = resolve;
      });
      let running!: () => void;
      const handlerRunning = new Promise<void>((resolve) => {
        running = resolve;
      });
      const job = defineJob({
        kind: 'pg.locked',
        timeoutMs: 5_000,
        payload: z.number(),
        handle: async () => {
          running();
          await rowLocked;
          clock.skip(consumeLease(5_000, 2_000));

          return ok();
        },
      });
      const pool = make(driver);
      const holder = await driver.pool.connect();
      const outbox = createOutbox({
        jobs: [job],
        clock,
        events: events.sink,
        connect: pool.connect,
      });

      try {
        const enqueued = await outbox.enqueue(driver.db as never, job, {
          payload: 1,
          dedupeKey: 'lock',
        });
        const id = 'id' in enqueued ? enqueued.id : '';
        const draining = outbox[RUNTIME].drain(BUDGET, 'sweep');

        // Take the job row's lock once claimed (a claim SKIPs locked rows), before the settle.
        await handlerRunning;
        await holder.query('BEGIN');
        await holder.query(`SELECT 1 FROM "outbox"."job" WHERE id = $1 FOR UPDATE`, [id]);
        locked();
        const started = Date.now();

        const report = await within(draining, 10_000);

        expect(Date.now() - started).toBeLessThan(5_000);
        expect(report.settleFailed).toBe(1);
        expect(events.of('settle_retried').length).toBeGreaterThan(0);
        await holder.query('ROLLBACK');
        expect(await status(id)).toBe('processing');
      } finally {
        await holder.query('ROLLBACK').catch(() => undefined);
        holder.release();
        await pool.pool.end();
      }
    },
  );
});
