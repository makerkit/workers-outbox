import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { defineJob } from './define-job';
import { ok } from './outcome';
import { createJobRegistry } from './registry';

const job = (kind: string, timeoutMs: number, lane?: string) =>
  defineJob({
    kind,
    payload: z.object({}),
    timeoutMs,
    ...(lane === undefined ? {} : { lane }),
    handle: async () => ok(),
  });

describe('createJobRegistry', () => {
  it('rejects duplicate kinds and empty registries', () => {
    expect(() => createJobRegistry([job('a', 1_000), job('a', 2_000)])).toThrow(/duplicate kind/);
    expect(() => createJobRegistry([])).toThrow(/at least one/);
  });

  it('resolves jobs by kind', () => {
    const a = job('a', 1_000);

    expect(createJobRegistry([a]).get('a')).toBe(a);
    expect(createJobRegistry([a]).get('b')).toBeUndefined();
  });

  // port: submission-outbox-queue.server:545
  it('offers only kinds whose lease fits the remaining budget', () => {
    const short = job('short', 1_000);
    const long = job('long', 300_000);
    const registry = createJobRegistry([short, long]);

    expect(registry.claimable().map((k) => k.kind)).toEqual(['short', 'long']);
    expect(registry.claimable(180_000)).toEqual([{ kind: 'short', leaseMs: short.leaseMs }]);
  });

  it('restricts claims to one lane and maps kinds to lanes', () => {
    const registry = createJobRegistry([job('email', 1_000), job('backfill', 1_000, 'bulk')]);

    expect(registry.lanes()).toEqual(['default', 'bulk']);
    expect(registry.claimable(undefined, { lane: 'bulk' }).map((k) => k.kind)).toEqual([
      'backfill',
    ]);
    expect(registry.claimable(undefined, { lane: 'default' }).map((k) => k.kind)).toEqual([
      'email',
    ]);
    expect(registry.claimable(undefined, { lane: 'nope' })).toEqual([]);
    expect(registry.laneOf('backfill')).toBe('bulk');
    expect(registry.laneOf('unknown')).toBeUndefined();
  });
});
