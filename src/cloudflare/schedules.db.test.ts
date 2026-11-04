import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import { defineJob } from '../core/define-job';
import { ok } from '../core/outcome';
import { queryRows } from '../drizzle/catalog';
import { createOutbox } from '../runtime/create-outbox';
import {
  laneConnector,
  pgFault,
  recordingEvents,
  recordingQueue,
  SqlFaults,
  testClock,
} from '../runtime/runtime-harness.test-support';
import { testDrivers, type TestDriver } from '../test-support/drivers';
import { createScheduledHandler } from './scheduled-handler';
import { defineSchedule, type Schedule } from './schedules';

const WEEKLY = '0 9 * * MON';
const SWEEP = '*/5 * * * *';
const MONDAY = Date.parse('2026-11-02T09:00:00.000Z');
const INSERT_JOB = /INSERT INTO "outbox"\."job"/;

describe.each(testDrivers())('schedules on $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  const newsletter = defineJob({
    kind: 'newsletter.weekly',
    payload: z.object({ week: z.string() }),
    timeoutMs: 1_000,
    handle: async () => ok(),
  });
  const sync = defineJob({
    kind: 'insights.sync',
    payload: z.object({ shop: z.string(), day: z.string() }),
    timeoutMs: 1_000,
    handle: async () => ok(),
  });

  function setup(schedules: readonly Schedule[], faults?: SqlFaults) {
    const recorded = recordingEvents();
    const { queue, sent } = recordingQueue();
    const outbox = createOutbox({
      jobs: [newsletter, sync],
      connect: laneConnector(driver, faults).connect,
      events: recorded.sink,
      queue,
      clock: testClock(),
    });
    const tick = createScheduledHandler(() => outbox, { schedules });
    const fire = (cron: string, scheduledTime = MONDAY) => tick({ cron, scheduledTime }, {}, {});

    return { fire, recorded, sent };
  }

  const jobs = () =>
    queryRows<{ kind: string; dedupe_key: string; payload: unknown; tenant_key: string | null }>(
      driver.db,
      sql`SELECT kind, dedupe_key, payload, tenant_key FROM "outbox"."job" ORDER BY dedupe_key`,
    );

  const weekly = defineSchedule({
    cron: WEEKLY,
    name: 'weekly',
    job: newsletter,
    payload: ({ scheduledAt }) => ({ week: scheduledAt.toISOString().slice(0, 10) }),
  });

  it('enqueues one job per occurrence, keyed by the scheduled time, and nudges it', async () => {
    const { fire, recorded, sent } = setup([weekly]);

    await fire(WEEKLY);

    expect(await jobs()).toEqual([
      {
        kind: 'newsletter.weekly',
        dedupe_key: 'schedule:weekly:2026-11-02T09:00:00.000Z',
        payload: { week: '2026-11-02' },
        tenant_key: null,
      },
    ]);
    expect(sent.some((s) => s.message.type === 'nudge')).toBe(true);
    expect(recorded.of('schedule_ran')).toMatchObject([
      { name: 'weekly', cron: WEEKLY, created: 1, existing: 0, rejected: 0, attempts: 1 },
    ]);
  });

  it('a repeated tick finds its job; the next occurrence gets a new one', async () => {
    const { fire, recorded } = setup([weekly]);

    await fire(WEEKLY);
    await fire(WEEKLY); // same scheduledTime: a duplicate delivery
    await fire(WEEKLY, MONDAY + 7 * 86_400_000);

    expect((await jobs()).map((j) => j.dedupe_key)).toEqual([
      'schedule:weekly:2026-11-02T09:00:00.000Z',
      'schedule:weekly:2026-11-09T09:00:00.000Z',
    ]);
    expect(recorded.of('schedule_ran').map((e) => [e.created, e.existing])).toEqual([
      [1, 0],
      [0, 1],
      [1, 0],
    ]);
  });

  it('runs only the schedules whose cron fired, and the sweep on every tick', async () => {
    const { fire, recorded } = setup([weekly]);

    await fire(SWEEP);

    expect(await jobs()).toEqual([]);
    expect(recorded.of('schedule_ran')).toEqual([]);
    expect(recorded.of('sweep_finished')).toHaveLength(1);

    await fire(WEEKLY);

    expect(recorded.of('sweep_finished')).toHaveLength(2);
  });

  it('a run schedule fans out in one transaction and counts rejected enqueues', async () => {
    const daily = defineSchedule({
      cron: '0 2 * * *',
      name: 'insights',
      run: async ({ scheduledAt, enqueue }) => {
        const day = scheduledAt.toISOString().slice(0, 10);

        for (const shop of ['a', 'b']) {
          await enqueue(sync, {
            payload: { shop, day },
            dedupeKey: `insights:${shop}:${day}`,
            tenantKey: shop,
          });
        }

        // @ts-expect-error -- the payload is checked against the job's schema
        await enqueue(sync, { payload: { shop: 'c' }, dedupeKey: `insights:c:${day}` });
      },
    });
    const { fire, recorded } = setup([daily]);

    await fire('0 2 * * *');

    expect((await jobs()).map((j) => [j.dedupe_key, j.tenant_key])).toEqual([
      ['insights:a:2026-11-02', 'a'],
      ['insights:b:2026-11-02', 'b'],
    ]);
    expect(recorded.of('schedule_ran')).toMatchObject([{ created: 2, existing: 0, rejected: 1 }]);
  });

  it('retries a transient failure within the tick', async () => {
    const faults = new SqlFaults();
    faults.failNext(INSERT_JOB, () => pgFault('40001'));
    const { fire, recorded } = setup([weekly], faults);

    await fire(WEEKLY);

    expect(await jobs()).toHaveLength(1);
    expect(recorded.of('schedule_ran')).toMatchObject([{ created: 1, attempts: 2 }]);
  });

  it('a failing schedule enqueues nothing and reports it; the others and the sweep still run', async () => {
    const broken = defineSchedule({
      cron: WEEKLY,
      name: 'broken',
      run: async ({ scheduledAt, enqueue }) => {
        await enqueue(newsletter, {
          payload: { week: 'w' },
          dedupeKey: `broken:${scheduledAt.toISOString()}`,
        });
        throw new Error('no subscribers table');
      },
    });
    const { fire, recorded } = setup([broken, weekly]);

    await fire(WEEKLY);

    expect((await jobs()).map((j) => j.dedupe_key)).toEqual([
      'schedule:weekly:2026-11-02T09:00:00.000Z',
    ]);
    expect(recorded.of('schedule_failed')).toMatchObject([
      {
        name: 'broken',
        severity: 'error',
        attempts: 1,
        error: { message: 'no subscribers table' },
      },
    ]);
    expect(recorded.of('sweep_finished')).toHaveLength(1);
  });

  it('a single-job schedule whose payload fails its schema reports a failure', async () => {
    const bad = defineSchedule({
      cron: WEEKLY,
      job: newsletter,
      payload: () => ({ week: 42 }) as unknown as { week: string },
    });
    const { fire, recorded } = setup([bad]);

    await fire(WEEKLY);

    expect(await jobs()).toEqual([]);
    expect(recorded.of('schedule_failed')).toMatchObject([{ name: 'newsletter.weekly' }]);
  });

  it('rejects schedules that cannot work when the handler is created', () => {
    const make = (schedules: readonly Schedule[]) => () =>
      createScheduledHandler(
        () => {
          throw new Error('unused');
        },
        { schedules },
      );

    expect(make([{ ...weekly, cron: ' ' }])).toThrow(/needs a cron/);
    expect(make([weekly, weekly])).toThrow(/two schedules are named "weekly"/);
    expect(make([{ cron: WEEKLY, name: '', run: async () => {} }])).toThrow(/needs a name/);
    expect(
      make([
        {
          cron: WEEKLY,
          name: 'both',
          job: newsletter,
          payload: () => ({ week: 'w' }),
          run: async () => {},
        } as unknown as Schedule,
      ]),
    ).toThrow(/either job \+ payload, or run/);
  });
});
