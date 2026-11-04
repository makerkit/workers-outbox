// A2 against a real pg.Pool: a schedule's post-commit nudge bookkeeping must not wait on a second
// client while the schedule still holds the first.
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import { defineJob } from '../core/define-job';
import { ok } from '../core/outcome';
import { createPgPoolConnector } from '../pg';
import { createOutbox } from '../runtime/create-outbox';
import { recordingEvents, recordingQueue } from '../runtime/runtime-harness.test-support';
import { pgSuiteDrivers, pgUrls, type PgDriver } from '../test-support/drivers';
import { runSchedules } from './schedules';

describe.skipIf(pgUrls().length === 0).each(pgSuiteDrivers())(
  'schedules on a one-client pool: $name',
  ({ create }) => {
    let driver: PgDriver;

    beforeAll(async () => {
      driver = await create();
    });

    beforeEach(() => driver.reset());

    afterAll(() => driver.close());

    it('records the nudge after releasing the schedule’s client', async () => {
      const pool = new Pool({ ...driver.pool.options, max: 1, connectionTimeoutMillis: 2_000 });
      pool.on('error', () => undefined);
      const events = recordingEvents();
      const { queue, sent } = recordingQueue();
      const job = defineJob({
        kind: 'audit.schedule',
        payload: z.number(),
        timeoutMs: 100,
        handle: async () => ok(),
      });
      const outbox = createOutbox({
        jobs: [job],
        connect: createPgPoolConnector(pool),
        recordNudges: true,
        operationTimeoutMs: 400,
        queue,
        events: events.sink,
      });

      let peakWaiting = 0;
      const timer = setInterval(() => {
        peakWaiting = Math.max(peakWaiting, pool.waitingCount);
      }, 5);

      try {
        await runSchedules(
          outbox,
          [{ name: 'audit', cron: '* * * * *', job, payload: () => 1 }],
          '* * * * *',
          Date.now(),
          Date.now() + 5_000,
        );

        expect(peakWaiting).toBe(0);
        expect(events.of('schedule_failed')).toEqual([]);
        expect(events.of('schedule_ran')).toMatchObject([{ created: 1 }]);
        expect(events.of('infra_error')).toEqual([]);
        expect(sent).toHaveLength(1);

        const { rows } = await driver.pool.query(
          'SELECT status, nudge_sent_at IS NOT NULL AS stamped FROM outbox.job',
        );
        expect(rows).toEqual([{ status: 'pending', stamped: true }]);
      } finally {
        clearInterval(timer);
        await pool.end();
      }
    });
  },
);
