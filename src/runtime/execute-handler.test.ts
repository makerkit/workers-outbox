import type { StandardSchemaV1 } from '@standard-schema/spec';

import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import type { ClaimedJob } from '../store/outbox-store';

import { defineJob, type JobContext } from '../core/define-job';
import { ok, type HandlerOutcome } from '../core/outcome';
import { createTestClock } from '../testing/test-clock';
import { executeHandler } from './execute-handler';
import { systemClock, type OutboxDb } from './types';

const claimed: ClaimedJob = {
  id: '00000000-0000-7000-8000-000000000001',
  kind: 'steps',
  payload: 0,
  tenantKey: null,
  correlationId: null,
  metadata: {},
  leaseToken: 'token',
  attemptNo: 1,
  spentAttempts: 0,
  codeStreak: 0,
  lastErrorCode: null,
};

const fakeDb: OutboxDb = {
  execute: async () => [],
  transaction: (fn) => fn(fakeDb),
};

function run(timeoutMs: number, handle: (ctx: JobContext) => Promise<HandlerOutcome>) {
  const clock = createTestClock(new Date('2026-01-01T00:00:00Z'));
  const job = defineJob({
    kind: 'steps',
    payload: z.number(),
    timeoutMs,
    handle: (_payload, ctx) => handle(ctx),
  });

  return { clock, execution: executeHandler({ job, claimed, db: fakeDb, deps: {}, clock }) };
}

describe('executeHandler: soft deadline', () => {
  it('sets ctx.deadline to the attempt start plus timeoutMs, on the outbox clock', async () => {
    let seen: { deadline: number; remainingMs: number } | undefined;
    const { clock, execution } = run(30_000, async (ctx) => {
      seen = { deadline: ctx.deadline, remainingMs: ctx.remainingMs() };

      return ok();
    });

    await execution;

    expect(seen).toEqual({ deadline: clock.nowMs() + 30_000, remainingMs: 30_000 });
  });

  it('lets a handler stop before the deadline and return ok(), not timeout', async () => {
    const steps: number[] = [];
    const { clock, execution } = run(60_000, async (ctx) => {
      // Each "batch" takes 10 s of clock time; stop while one more batch still fits.
      while (ctx.remainingMs() > 15_000) {
        steps.push(ctx.remainingMs());
        clock.advance(10_000);
      }

      return ok();
    });

    const result = await execution;

    expect(result).toEqual({ outcome: { type: 'ok' }, runaway: false });
    expect(steps).toEqual([60_000, 50_000, 40_000, 30_000, 20_000]);
  });

  it('with the system clock, the deadline is the real timeout: work stopped before it is ok()', async () => {
    const job = defineJob({
      kind: 'steps',
      payload: z.number(),
      timeoutMs: 2_000,
      handle: async (_payload, ctx) => {
        while (ctx.remainingMs() > 1_000) {
          await new Promise((resolve) => setTimeout(resolve, 20));
        }

        return ok();
      },
    });

    const result = await executeHandler({
      job,
      claimed,
      db: fakeDb,
      deps: {},
      clock: systemClock,
    });

    expect(result.outcome).toEqual({ type: 'ok' });
  });

  it('never reports less than zero remaining', async () => {
    let remaining: number | undefined;
    const { clock, execution } = run(1_000, async (ctx) => {
      clock.advance(5_000);
      remaining = ctx.remainingMs();

      return ok();
    });

    await execution;

    expect(remaining).toBe(0);
  });
});

describe('executeHandler: ctx.db', () => {
  it('passes a fenced db that rejects once the handler has returned', async () => {
    let leaked: OutboxDb | undefined;
    const { execution } = run(1_000, async (ctx) => {
      await ctx.db.execute({} as never);
      leaked = ctx.db;

      return ok();
    });

    await execution;

    await expect(leaked!.execute({} as never)).rejects.toThrow(/after the handler returned/);
  });
});

describe('executeHandler: the timeout starts with the attempt', () => {
  // A synchronous validator runs before the first await: the timer must already be armed.
  const slowSchema = (busyMs: number): StandardSchemaV1<number, number> => ({
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: (value) => {
        const until = Date.now() + busyMs;

        while (Date.now() < until) {
          // busy: synchronous validation work
        }

        return { value: value as number };
      },
    },
  });

  it('counts synchronous payload validation against timeoutMs and ctx.deadline', async () => {
    const startedAt = Date.now();
    let deadline = 0;
    const job = defineJob({
      kind: 'steps',
      payload: slowSchema(200),
      timeoutMs: 300,
      handle: async (_payload, ctx) => {
        deadline = ctx.deadline;
        await new Promise((resolve) => setTimeout(resolve, 200));

        return ok();
      },
    });

    // 200 ms validating + 200 ms handling is past the 300 ms limit.
    const result = await executeHandler({
      job,
      claimed,
      db: fakeDb,
      deps: {},
      clock: systemClock,
    });

    expect(result.outcome).toEqual({ type: 'timeout' });
    expect(deadline - startedAt).toBeLessThanOrEqual(300 + 20);
  });
});
