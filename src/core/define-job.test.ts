import { describe, expect, expectTypeOf, it } from 'vitest';
import { z } from 'zod';

import {
  defineJob,
  HANDLER_GRACE_MS,
  LEASE_MARGIN_MS,
  MAX_TIMEOUT_MS,
  type JobContext,
} from './define-job';
import { ok } from './outcome';

const payload = z.object({ userId: z.string() });

describe('defineJob', () => {
  // port: stuck-recovery.lease-margin:27
  it('derives the lease from the timeout so it always outlives the handler', () => {
    const job = defineJob({
      kind: 'email.welcome',
      payload,
      timeoutMs: 20_000,
      handle: async () => ok(),
    });

    expect(job.leaseMs).toBe(20_000 + HANDLER_GRACE_MS + LEASE_MARGIN_MS);
    expect(job.retry.maxAttempts).toBe(20);
  });

  it('types the handler payload from the schema and deps from the context', () => {
    interface Deps {
      mailer: { send(id: string): Promise<void> };
    }

    defineJob({
      kind: 'email.welcome',
      payload,
      timeoutMs: 1_000,
      handle: async (input, ctx: JobContext<Deps>) => {
        expectTypeOf(input).toEqualTypeOf<{ userId: string }>();
        await ctx.deps.mailer.send(input.userId);

        return ok();
      },
    });
  });

  it('classifies unexpected throws as retries by default', () => {
    const job = defineJob({ kind: 'a', payload, timeoutMs: 1_000, handle: async () => ok() });

    expect(job.classify(new Error('socket hang up'))).toEqual({
      type: 'retry',
      code: 'unexpected',
      message: 'socket hang up',
    });
  });

  // port: submission-job.repository.claim.db:275
  it.each(['', 'Email.Welcome', 'has space', 'x'.repeat(201)])('rejects kind %j', (kind) => {
    expect(() => defineJob({ kind, payload, timeoutMs: 1_000, handle: async () => ok() })).toThrow(
      /kind/,
    );
  });

  it.each([0, -1, 1.5, 600_001])('rejects timeoutMs %j', (timeoutMs) => {
    expect(() => defineJob({ kind: 'a', payload, timeoutMs, handle: async () => ok() })).toThrow(
      /timeoutMs/,
    );
  });

  // port: process-submission-outbox.service:3186
  // port: stuck-recovery.lease-margin:31
  it('keeps every deadline below its lease, up to the 10-minute cap', () => {
    const longest = defineJob({
      kind: 'erase',
      payload,
      timeoutMs: MAX_TIMEOUT_MS,
      handle: async () => ok(),
    });

    expect(MAX_TIMEOUT_MS).toBe(600_000);
    expect(longest.leaseMs).toBeGreaterThan(longest.timeoutMs + longest.graceMs);
    expect(() =>
      defineJob({
        kind: 'erase',
        payload,
        timeoutMs: MAX_TIMEOUT_MS + 1,
        handle: async () => ok(),
      }),
    ).toThrow(/timeoutMs/);
  });

  // port: process-submission-outbox.service:3220
  it('has no implicit default deadline: a job without timeoutMs is rejected', () => {
    const input = { kind: 'a', payload, handle: async () => ok() };

    // @ts-expect-error -- timeoutMs is required: every kind states its own deadline
    expect(() => defineJob(input)).toThrow(/timeoutMs/);
  });

  // port: process-submission-outbox.service:2954
  // port: process-submission-outbox.service:3014
  it("derives each kind's lease from its own timeout, never another kind's", () => {
    const short = defineJob({
      kind: 'short',
      payload,
      timeoutMs: 10_000,
      handle: async () => ok(),
    });
    const long = defineJob({ kind: 'long', payload, timeoutMs: 500_000, handle: async () => ok() });

    expect(short.leaseMs).toBe(10_000 + HANDLER_GRACE_MS + LEASE_MARGIN_MS);
    expect(long.leaseMs).toBe(500_000 + HANDLER_GRACE_MS + LEASE_MARGIN_MS);
  });

  it("puts a job on the 'default' lane unless it names one", () => {
    const job = (lane?: string) =>
      defineJob({
        kind: 'a',
        payload,
        timeoutMs: 1_000,
        ...(lane ? { lane } : {}),
        handle: async () => ok(),
      });

    expect(job().lane).toBe('default');
    expect(job('bulk').lane).toBe('bulk');
    expect(() => job('Bulk')).toThrow(/lane/);
    expect(() => job('x'.repeat(51))).toThrow(/lane/);
  });
});
