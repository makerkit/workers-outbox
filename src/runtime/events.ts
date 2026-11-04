import type { ClaimByIdResult } from '../store/outbox-store';
import type { SerializedError } from './serialize-error';

/** Transition events fire only after the settle (and its hook) committed. */
export type OutboxEvent =
  | { type: 'enqueued'; jobId: string; kind: string; created: boolean }
  | {
      type: 'transition';
      severity: 'info' | 'warn' | 'error';
      jobId: string;
      kind: string;
      tenantKey: string | null;
      correlationId: string | null;
      attemptNo: number;
      to: 'succeeded' | 'retrying' | 'dead' | 'cancelled';
      reason: string | null;
      code: string | null;
      codeStreak: number;
      attemptsUsed: number;
      maxAttempts: number;
      delayMs: number | null;
      recovered: boolean;
      trigger: 'nudge' | 'sweep' | 'drain' | 'recovery';
    }
  | {
      type: 'attempt_threshold';
      jobId: string;
      kind: string;
      code: string;
      codeStreak: number;
      threshold: number;
    }
  | {
      type: 'handler_threw';
      jobId: string;
      kind: string;
      attemptNo: number;
      error: SerializedError;
    }
  | { type: 'handler_timeout'; jobId: string; kind: string; attemptNo: number; timeoutMs: number }
  | { type: 'lease_lost'; jobId: string; kind: string; attemptNo: number }
  | { type: 'hook_failed'; jobId: string; kind: string; to: string; error: SerializedError }
  | {
      type: 'settle_retried';
      jobId: string;
      kind: string;
      dbCode: string | null;
      retry: number;
    }
  | {
      type: 'settle_failed';
      jobId: string;
      kind: string;
      attemptNo: number;
      dbCode: string | null;
      transient: boolean;
      error: SerializedError;
    }
  | {
      type: 'nudge_miss';
      jobId: string;
      result: Exclude<ClaimByIdResult['result'], 'claimed'>;
    }
  | {
      /** The claim's reply arrived too late to run the handler: it never started. */
      type: 'claim_late';
      jobId: string;
      kind: string;
      attemptNo: number;
      claimLatencyMs: number;
    }
  | { type: 'nudge_sent'; ids: string[]; lane: string; delaySeconds: number }
  | { type: 'nudge_failed'; ids: string[]; lane: string; error: SerializedError }
  | { type: 'nudge_skipped'; ids: string[]; lane: string; reason: 'no_queue' }
  | { type: 'cleanup'; deleted: number }
  | { type: 'backlog'; report: import('../core/backlog').BacklogReport }
  | { type: 'continuation_sent'; lane: string }
  | { type: 'continuation_skipped'; lane: string; reason: 'no_queue' }
  | { type: 'continuation_failed'; lane: string; error: SerializedError }
  | { type: 'message_received'; messageType: 'nudge' | 'drain'; ids: string[]; attempts: number }
  | { type: 'message_rejected'; reason: 'malformed' | 'unknown_version'; body: string }
  | {
      type: 'message_retried';
      messageType: 'nudge' | 'drain';
      delaySeconds: number;
      attempts: number;
      error: SerializedError;
    }
  | { type: 'dlq_message'; queue: string; attempts: number; body: string }
  | {
      type: 'phase_failed';
      phase: 'recover' | 'reserved' | 'drain' | 'handoff' | 'cleanup' | 'backlog';
      cron: string | null;
      error: SerializedError;
    }
  | {
      /** A schedule's occurrence committed. `existing` > 0 means a repeated tick found its jobs. */
      type: 'schedule_ran';
      name: string;
      cron: string;
      scheduledAt: string;
      created: number;
      existing: number;
      /** Enqueues a `run` schedule got back as `invalid_payload` or `dedupe_conflict`. */
      rejected: number;
      attempts: number;
    }
  | {
      /** The occurrence enqueued nothing: it is skipped, not retried on a later tick. */
      type: 'schedule_failed';
      severity: 'error';
      name: string;
      cron: string;
      scheduledAt: string;
      attempts: number;
      error: SerializedError;
    }
  | {
      type: 'sweep_finished';
      cron: string | null;
      recovered: number;
      report: import('./runtime').RunReport;
      deleted: number;
      continued: boolean;
    }
  | {
      type: 'infra_error';
      operation: string;
      dbCode: string | null;
      transient: boolean;
      error: SerializedError;
    };

export type EventSink = (event: OutboxEvent) => void;

export function createEmitter(sink: EventSink | undefined) {
  return (event: OutboxEvent) => {
    if (!sink) {
      return;
    }

    try {
      sink(event);
    } catch {
      // A throwing sink must never break job processing.
    }
  };
}

export type Emit = ReturnType<typeof createEmitter>;
