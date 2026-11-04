import { drizzle } from 'drizzle-orm/node-postgres';
import { v7 as uuidv7 } from 'uuid';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { pgSuiteDrivers, pgUrls, type PgDriver } from '../test-support/drivers';
import { recordingRunner } from '../test-support/recording-runner';
import {
  claimByIds,
  claimDue,
  deleteFinishedJobs,
  insertJob,
  listJobs,
  replayJob,
  settleJob,
  type KindFilter,
  type NewJob,
} from './outbox-store';

const drivers = pgSuiteDrivers();

const EMAIL: KindFilter = { kind: 'email.welcome', leaseMs: 60_000, fits: true };
const RARE: KindFilter = { kind: 'rare.kind', leaseMs: 60_000, fits: true };

const newJob = (overrides: Partial<NewJob> = {}): NewJob => ({
  id: uuidv7(),
  kind: EMAIL.kind,
  dedupeKey: `key:${uuidv7()}`,
  payload: {},
  tenantKey: null,
  correlationId: null,
  metadata: {},
  delayMs: 0,
  ...overrides,
});

describe.skipIf(pgUrls().length === 0).each(drivers)(
  'outbox store sessions on $name',
  ({ create }) => {
    let driver: PgDriver;

    beforeAll(async () => {
      driver = await create();
    });

    beforeEach(() => driver.reset());

    afterAll(() => driver.close());

    it('never hands one job to two concurrent claimers', async () => {
      const total = 200;

      for (let i = 0; i < total; i++) {
        await insertJob(driver.db, newJob(), null);
      }

      const lanes = await Promise.all(
        Array.from({ length: 8 }, async () => {
          const client = await driver.pool.connect();
          const db = drizzle({ client });
          const ids: string[] = [];

          try {
            for (;;) {
              const batch = await claimDue(db, {
                kinds: [EMAIL],
                limit: 5,
                trigger: 'drain',
                leaseToken: uuidv7(),
                now: null,
              });

              if (batch.length === 0) {
                return ids;
              }

              ids.push(...batch.map((job) => job.id));
            }
          } finally {
            client.release();
          }
        }),
      );

      const claimed = lanes.flat();

      expect(claimed).toHaveLength(total);
      expect(new Set(claimed).size).toBe(total);
    });

    it('resolves two transactions racing on one dedupe key to one created and one existing', async () => {
      const first = await driver.pool.connect();
      const second = await driver.pool.connect();

      try {
        const a = newJob({ dedupeKey: 'race' });
        const b = newJob({ dedupeKey: 'race' });

        await first.query('BEGIN');
        await second.query('BEGIN');

        const created = await insertJob(drizzle({ client: first }), a, null);
        const pending = insertJob(drizzle({ client: second }), b, null);

        await first.query('COMMIT');
        const existing = await pending;
        await second.query('COMMIT');

        expect(created).toEqual({ id: a.id, kind: a.kind, tenantKey: null, created: true });
        expect(existing).toEqual({ id: a.id, kind: a.kind, tenantKey: null, created: false });
      } finally {
        first.release();
        second.release();
      }
    });

    it('surfaces a serialization failure under REPEATABLE READ when the key holder is invisible', async () => {
      const first = await driver.pool.connect();
      const second = await driver.pool.connect();

      try {
        await second.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
        await second.query('SELECT 1');

        await insertJob(drizzle({ client: first }), newJob({ dedupeKey: 'rr-race' }), null);

        await expect(
          insertJob(drizzle({ client: second }), newJob({ dedupeKey: 'rr-race' }), null),
        ).rejects.toMatchObject({ cause: { code: '40001' } });
      } finally {
        await second.query('ROLLBACK');
        first.release();
        second.release();
      }
    });

    it('does not block an enqueue on a row lock held over the existing job', async () => {
      const { id } = await insertJob(driver.db, newJob({ dedupeKey: 'held' }), null);
      const settle = await driver.pool.connect();

      try {
        await settle.query('BEGIN');
        await settle.query('SELECT 1 FROM "outbox"."job" WHERE id = $1 FOR UPDATE', [id]);

        const result = await Promise.race([
          insertJob(driver.db, newJob({ dedupeKey: 'held' }), null),
          new Promise((resolve) => setTimeout(() => resolve('blocked'), 2_000)),
        ]);

        expect(result).toEqual({ id, kind: 'email.welcome', tenantKey: null, created: false });
        await settle.query('ROLLBACK');
      } finally {
        settle.release();
      }
    });

    it('does not delete a dead job replayed while cleanup waited on its row lock', async () => {
      const { id } = await insertJob(driver.db, newJob({ dedupeKey: 'replayed' }), null);
      await driver.pool.query(
        `UPDATE "outbox"."job" SET status = 'dead', terminal_reason = 'failed', finished_at = now() - interval '200 days' WHERE id = $1`,
        [id],
      );
      const replay = await driver.pool.connect();

      try {
        await replay.query('BEGIN');
        expect(await replayJob(drizzle({ client: replay }), { id, now: null })).toBe('replayed');

        const cleanup = deleteFinishedJobs(driver.db, {
          succeededRetentionMs: 86_400_000,
          terminalRetentionMs: 86_400_000,
          batch: 100,
          now: null,
        });
        // Commit only once the DELETE is blocked on the row lock: committing earlier lets its
        // snapshot already see `pending`, and the test would pass without the fix.
        const deadline = Date.now() + 5_000;

        for (;;) {
          const { rows: waiting } = await driver.pool.query<{ n: number }>(
            `SELECT count(*)::int AS n FROM pg_locks WHERE NOT granted`,
          );

          if ((waiting[0]?.n ?? 0) > 0) {
            break;
          }

          if (Date.now() > deadline) {
            throw new Error('cleanup never blocked on the replayed row');
          }

          await new Promise((resolve) => setTimeout(resolve, 10));
        }

        await replay.query('COMMIT');

        expect(await cleanup).toBe(0);
        const { rows } = await driver.pool.query(
          `SELECT status FROM "outbox"."job" WHERE id = $1`,
          [id],
        );
        expect(rows).toEqual([{ status: 'pending' }]);
      } finally {
        replay.release();
      }
    });

    // port: submission-job-transition.service:887
    // port: submission-job-transition.service:1129
    it('lets exactly one of a racing worker settle and recovery settle apply, never both', async () => {
      const rounds = 20;
      const T = new Date('2026-01-01T00:00:00.000Z');
      const afterLease = new Date(T.getTime() + EMAIL.leaseMs + 1);

      for (let i = 0; i < rounds; i++) {
        await insertJob(driver.db, newJob(), T);
      }

      const claimed = await claimDue(driver.db, {
        kinds: [EMAIL],
        limit: rounds,
        trigger: 'sweep',
        leaseToken: uuidv7(),
        now: T,
      });
      const session = async () => drizzle({ client: await driver.pool.connect() });
      const [workerDb, recoveryDb] = await Promise.all([session(), session()]);

      try {
        for (const job of claimed) {
          const results = await Promise.all([
            settleJob(workerDb, {
              jobId: job.id,
              leaseToken: job.leaseToken,
              transition: { to: 'succeeded', attemptOutcome: 'succeeded' },
              now: afterLease,
            }),
            settleJob(recoveryDb, {
              jobId: job.id,
              leaseToken: job.leaseToken,
              transition: {
                to: 'retrying',
                delayMs: 0,
                code: 'lease_expired',
                message: null,
                codeStreak: 1,
                attemptOutcome: 'lease_expired',
              },
              now: afterLease,
              requireExpiredLease: true,
            }),
          ]);

          expect(results.filter((r) => r === 'applied')).toHaveLength(1);
        }

        const { rows } = await driver.pool.query(
          `SELECT count(*)::int AS open FROM "outbox"."attempt" WHERE finished_at IS NULL`,
        );
        const { rows: jobs } = await driver.pool.query(
          `SELECT count(*)::int AS bad FROM "outbox"."job" WHERE finished_attempts <> 1`,
        );

        expect(rows[0].open).toBe(0);
        expect(jobs[0].bad).toBe(0);
      } finally {
        workerDb.$client.release();
        recoveryDb.$client.release();
      }
    });

    describe('query plans', () => {
      beforeEach(async () => {
        await driver.pool.query(`
        INSERT INTO "outbox"."job" (id, kind, dedupe_key, payload, available_at)
        SELECT gen_random_uuid(),
               CASE WHEN g % 2000 = 0 THEN 'rare.kind' ELSE 'email.welcome' END,
               'seed:' || g, '{}',
               now() + CASE WHEN g % 2 = 0 THEN interval '1 hour' ELSE interval '0' END
        FROM generate_series(1, 20000) AS g`);
        await driver.pool.query('ANALYZE "outbox"."job"');
      });

      const planOf = async (
        run: (db: ReturnType<typeof recordingRunner>['runner']) => Promise<unknown>,
      ) => {
        const client = await driver.pool.connect();

        try {
          await client.query('BEGIN');
          const { runner, recorded } = recordingRunner(drizzle({ client }));
          await run(runner);
          await client.query('ROLLBACK');

          const statement = recorded.at(-1)!;
          const { rows } = await client.query(
            `EXPLAIN (FORMAT JSON) ${statement.sql}`,
            statement.params,
          );

          if (process.env['PLAN_OUT']) {
            const text = await client.query(`EXPLAIN ${statement.sql}`, statement.params);
            (await import('node:fs')).appendFileSync(
              process.env['PLAN_OUT'],
              text.rows.map((r) => r['QUERY PLAN']).join('\n') + '\n=====\n',
            );
          }

          return JSON.stringify(rows[0]['QUERY PLAN']);
        } finally {
          client.release();
        }
      };

      const seqScansJob = (plan: string) =>
        /"Node Type":"Seq Scan","Parent Relationship":"[^"]*","Parallel Aware":(true|false),"Async Capable":(true|false),"Relation Name":"job"|"Node Type":"Seq Scan"[^}]*"Relation Name":"job"/.test(
          plan,
        );

      it('claims the common kind through an index, never a sequential scan', async () => {
        const plan = await planOf((db) =>
          claimDue(db, {
            kinds: [EMAIL, RARE],
            limit: 10,
            trigger: 'sweep',
            leaseToken: uuidv7(),
            now: null,
          }),
        );

        expect(seqScansJob(plan)).toBe(false);
        expect(plan).toMatch(/job_claim_due_idx|job_pending_kind_idx|job_retrying_kind_idx/);
      });

      it('claims a rare kind through the kind-filtered partial index', async () => {
        const plan = await planOf((db) =>
          claimDue(db, {
            kinds: [RARE],
            limit: 10,
            trigger: 'sweep',
            leaseToken: uuidv7(),
            now: null,
          }),
        );

        expect(seqScansJob(plan)).toBe(false);
        expect(plan).toMatch(/job_pending_kind_idx|job_retrying_kind_idx/);
      });

      // port: job-outbox-claim-indexes.schema.db:62
      it('lists a rare status through the inspection index, never a sequential scan', async () => {
        await driver.pool.query(`
          UPDATE "outbox"."job" SET status = 'dead', terminal_reason = 'failed', finished_at = now()
          WHERE dedupe_key IN ('seed:1', 'seed:3', 'seed:5')`);
        await driver.pool.query('ANALYZE "outbox"."job"');

        const plan = await planOf((db) => listJobs(db, { status: 'dead', limit: 50 }));

        expect(seqScansJob(plan)).toBe(false);
        expect(plan).toContain('job_status_created_idx');
      });

      it('claims by id through the primary key', async () => {
        const plan = await planOf((db) =>
          claimByIds(db, { ids: [uuidv7()], kinds: [EMAIL], leaseToken: uuidv7(), now: null }),
        );

        expect(seqScansJob(plan)).toBe(false);
        expect(plan).toContain('job_pkey');
      });
    });
  },
);
