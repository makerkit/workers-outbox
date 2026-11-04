import { describe, expect, it } from 'vitest';

import { DEFAULT_RETRY_POLICY, resolveRetryPolicy } from './retry-policy';

describe('resolveRetryPolicy', () => {
  it('fills defaults', () => {
    expect(resolveRetryPolicy()).toEqual(DEFAULT_RETRY_POLICY);
  });

  it('merges per-code caps over the defaults', () => {
    expect(resolveRetryPolicy({ perCode: { rate_limited: 10 } }).perCode).toEqual({
      timeout: 5,
      invalid_payload: 3,
      rate_limited: 10,
    });
  });

  it.each([
    [{ maxAttempts: 0 }, /maxAttempts/],
    [{ maxAttempts: 1.5 }, /maxAttempts/],
    [{ maxCrashes: 0 }, /maxCrashes/],
    [{ backoff: { baseMs: 0 } }, /baseMs/],
    [{ backoff: { baseMs: 10_000, maxMs: 5_000 } }, /maxMs must be >= /],
    [{ backoff: { maxMs: 90_000_000 } }, /24h/],
    [{ backoff: { factor: 0.5 } }, /factor/],
    [{ perCode: { timeout: 0 } }, /perCode.timeout/],
    [{ perCode: { x: { maxAttempts: 0 } } }, /perCode.x.maxAttempts/],
    [{ perCode: { x: { maxDelayMs: 0 } } }, /perCode.x.maxDelayMs/],
    [{ perCode: { x: { sticky: true } } }, /sticky needs a maxAttempts/],
    [{ perCode: { lease_expired: 3 } }, /maxCrashes/],
    [{ perCode: { lease_expired: { maxAttempts: 3 } } }, /maxCrashes/],
  ])('rejects %j', (input, message) => {
    expect(() => resolveRetryPolicy(input)).toThrow(message);
  });

  it('splits per-code entries into streak caps and richer rules, keeping default caps', () => {
    const policy = resolveRetryPolicy({
      perCode: {
        rate_limited: { streak: 10, maxAttempts: Number.POSITIVE_INFINITY, maxDelayMs: 60_000 },
        timeout: { onExhaustion: 'cancel' },
        http_4xx: 3,
      },
    });

    expect(policy.perCode).toEqual({
      timeout: 5,
      invalid_payload: 3,
      rate_limited: 10,
      http_4xx: 3,
    });
    expect(policy.codes).toEqual({
      rate_limited: {
        maxAttempts: Number.POSITIVE_INFINITY,
        maxDelayMs: 60_000,
        sticky: false,
        onExhaustion: 'dead',
      },
      timeout: { sticky: false, onExhaustion: 'cancel' },
    });
  });
});
