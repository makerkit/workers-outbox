import type { RetryPolicy } from './retry-policy';

// Equal jitter hashed from (jobId, attempt), not Math.random, so a schedule is reproducible.
export function computeBackoff(policy: RetryPolicy, jobId: string, attempt: number) {
  const { baseMs, maxMs, factor } = policy.backoff;
  const exponent = Math.max(0, attempt - 1);
  const raw = Math.min(maxMs, baseMs * factor ** exponent);
  const half = raw / 2;

  return Math.round(half + half * unitHash(`${jobId}:${attempt}`));
}

function unitHash(input: string) {
  let hash = 0x811c9dc5;

  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }

  return (hash >>> 0) / 0x1_0000_0000;
}
