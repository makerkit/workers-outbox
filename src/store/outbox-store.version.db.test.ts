import { v7 as uuidv7 } from 'uuid';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { testDrivers, type TestDriver } from '../test-support/drivers';
import {
  claimByIds,
  claimDue,
  hasDueJobs,
  insertJob,
  type KindFilter,
  type NewJob,
} from './outbox-store';

const T0 = new Date('2026-01-01T00:00:00.000Z');
const EMAIL: KindFilter = { kind: 'email.welcome', leaseMs: 60_000, fits: true };
const V1 = Date.parse('2026-01-01T00:00:00Z');
const V2 = Date.parse('2026-01-02T00:00:00Z');
const GATE_MS = 3_600_000;

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

describe.each(testDrivers())('deploy-version gating on $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  beforeEach(() => driver.reset());

  afterAll(() => driver.close());

  const claimAs = (version: number | undefined, now: Date) =>
    claimDue(driver.db, {
      kinds: [EMAIL],
      limit: 5,
      trigger: 'sweep',
      leaseToken: uuidv7(),
      now,
      ...(version === undefined ? {} : { version: { mineMs: version, gateMs: GATE_MS } }),
    });

  it('an older deploy skips jobs enqueued by a newer one; the newer deploy claims them', async () => {
    await insertJob(driver.db, newJob({ minVersionMs: V2 }), T0);

    expect(await claimAs(V1, T0)).toEqual([]);
    expect(
      await hasDueJobs(driver.db, {
        kinds: [EMAIL],
        now: T0,
        version: { mineMs: V1, gateMs: GATE_MS },
      }),
    ).toBe(false);
    expect(await claimAs(V2, T0)).toHaveLength(1);
  });

  it('unstamped jobs and unversioned workers are never gated', async () => {
    await insertJob(driver.db, newJob(), T0);
    await insertJob(driver.db, newJob({ minVersionMs: V2 }), T0);

    expect(await claimAs(V1, T0)).toHaveLength(1);
    expect(await claimAs(undefined, T0)).toHaveLength(1);
  });

  it('the gate expires, so a rollback never strands newer jobs', async () => {
    await insertJob(driver.db, newJob({ minVersionMs: V2 }), T0);

    expect(await claimAs(V1, new Date(T0.getTime() + GATE_MS + 1))).toHaveLength(1);
  });

  it('claimByIds reports a gated job as newer_version and leaves it pending', async () => {
    const job = newJob({ minVersionMs: V2 });
    await insertJob(driver.db, job, T0);

    expect(
      await claimByIds(driver.db, {
        ids: [job.id],
        kinds: [EMAIL],
        leaseToken: uuidv7(),
        now: T0,
        version: { mineMs: V1, gateMs: GATE_MS },
      }),
    ).toEqual([{ id: job.id, result: 'newer_version' }]);
  });
});
