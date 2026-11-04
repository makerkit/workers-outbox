import type { OutboxDb } from '../runtime/types';

/** Database-only and re-runnable: it may run again after a transient fault. */
export type CommitFn = (tx: OutboxDb) => Promise<void>;

/** Expected results are values; throw only for unanticipated faults (`classify` maps them). */
export type HandlerOutcome =
  | { type: 'ok'; commit?: CommitFn }
  | { type: 'retry'; code: string; afterMs?: number; message?: string }
  | { type: 'fail'; code: string; message?: string }
  | { type: 'cancel'; reason: string };

/** `commit` runs in the settle transaction: its writes and `succeeded` land together, lease-fenced. */
export const ok = (options: { commit?: CommitFn } = {}): HandlerOutcome =>
  options.commit ? { type: 'ok', commit: options.commit } : { type: 'ok' };

/** `after` overrides the backoff (e.g. a provider's Retry-After). */
// `| undefined` so `retry({ code, after: res.retryAfterMs })` compiles under exactOptionalPropertyTypes.
export const retry = (input: {
  code: string;
  after?: number | undefined;
  message?: string | undefined;
}): HandlerOutcome => {
  const outcome: HandlerOutcome = { type: 'retry', code: input.code };

  if (input.after !== undefined) {
    outcome.afterMs = input.after;
  }

  if (input.message !== undefined) {
    outcome.message = input.message;
  }

  return outcome;
};

export const fail = (input: { code: string; message?: string | undefined }): HandlerOutcome =>
  input.message === undefined
    ? { type: 'fail', code: input.code }
    : { type: 'fail', code: input.code, message: input.message };

export const cancel = (input: { reason: string }): HandlerOutcome => ({
  type: 'cancel',
  reason: input.reason,
});

export const TIMEOUT_CODE = 'timeout';
export const LEASE_EXPIRED_CODE = 'lease_expired';
export const HOOK_FAILED_CODE = 'hook_failed';
/** The claim's reply arrived too late to run the handler safely; it never started. */
export const CLAIM_LATE_CODE = 'claim_late';
/** Retried soon: lateness is the infrastructure's, not the job's. */
export const CLAIM_LATE_DELAY_MS = 5_000;

export type SettleOutcome =
  | HandlerOutcome
  | { type: 'timeout' }
  | { type: 'lease_expired' }
  | { type: 'claim_late' };
