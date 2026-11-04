import { describe, expect, it } from 'vitest';

import { computeBackoff } from './backoff';
import { resolveRetryPolicy } from './retry-policy';

const policy = resolveRetryPolicy({ backoff: { baseMs: 1_000, maxMs: 60_000, factor: 2 } });

describe('computeBackoff', () => {
  it('grows exponentially within equal-jitter bounds', () => {
    for (const [attempt, raw] of [
      [1, 1_000],
      [2, 2_000],
      [3, 4_000],
      [6, 32_000],
    ] as const) {
      const delay = computeBackoff(policy, 'job-a', attempt);

      expect(delay).toBeGreaterThanOrEqual(raw / 2);
      expect(delay).toBeLessThanOrEqual(raw);
    }
  });

  // port: process-submission-outbox.service:717
  it('caps at maxMs and never exceeds it, however high the attempt', () => {
    const delay = computeBackoff(policy, 'job-a', 50);

    expect(delay).toBeGreaterThanOrEqual(30_000);
    expect(delay).toBeLessThanOrEqual(60_000);

    for (let attempt = 1; attempt <= 2_000; attempt += 37) {
      expect(computeBackoff(policy, `job-${attempt}`, attempt)).toBeLessThanOrEqual(60_000);
    }
  });

  it('is deterministic per (job, attempt) so incidents replay exactly', () => {
    expect(computeBackoff(policy, 'job-a', 3)).toBe(computeBackoff(policy, 'job-a', 3));
  });

  it('spreads different jobs failing together', () => {
    const delays = new Set(
      Array.from({ length: 20 }, (_, i) => computeBackoff(policy, `job-${i}`, 4)),
    );

    expect(delays.size).toBeGreaterThan(10);
  });
});
