// Frozen for v1: new terminal causes become a `terminal_reason`, never a new status.
export const JOB_STATUSES = [
  'pending',
  'processing',
  'retrying',
  'succeeded',
  'dead',
  'cancelled',
] as const;

export type JobStatus = (typeof JOB_STATUSES)[number];

export const CLAIMABLE_STATUSES = ['pending', 'retrying'] as const satisfies readonly JobStatus[];

export const TERMINAL_STATUSES = [
  'succeeded',
  'dead',
  'cancelled',
] as const satisfies readonly JobStatus[];

export const REASONED_TERMINAL_STATUSES = [
  'dead',
  'cancelled',
] as const satisfies readonly JobStatus[];

export const ATTEMPT_TRIGGERS = ['nudge', 'sweep', 'drain'] as const;

export type AttemptTrigger = (typeof ATTEMPT_TRIGGERS)[number];

export const ATTEMPT_OUTCOMES = [
  'succeeded',
  'retry',
  'failed',
  'cancelled',
  'timeout',
  'lease_expired',
] as const;

export type AttemptOutcome = (typeof ATTEMPT_OUTCOMES)[number];
