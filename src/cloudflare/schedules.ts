import type { AnyJob } from '../core/define-job';
import type {
  EnqueueRequest,
  EnqueueResult,
  Outbox,
  ScopedEnqueue,
} from '../runtime/create-outbox';
import type { OutboxDb } from '../runtime/types';

import { RUNTIME } from '../runtime/create-outbox';
import { isConnectionError, isTransientDbError } from '../runtime/db-errors';
import { serializeError } from '../runtime/serialize-error';

/** One firing of a schedule's cron. */
export interface ScheduleOccurrence {
  /**
   * The time this tick was scheduled for (`event.scheduledTime`), not when it ran: a repeated
   * delivery of the same tick has the same value, so keys derived from it dedupe.
   */
  readonly scheduledAt: Date;
  readonly cron: string;
}

/** Enqueues one job per occurrence. */
export interface JobSchedule<T extends AnyJob = AnyJob> {
  /** Must match one of the Worker's `triggers.crons` exactly, as Cloudflare passes it back. */
  readonly cron: string;
  /** Names the schedule in events and in the default dedupe key. Default: the job's kind. */
  readonly name?: string;
  readonly job: T;
  readonly payload: (occurrence: ScheduleOccurrence) => EnqueueRequest<T>['payload'];
  /** Default `schedule:<name>:<scheduledAt as ISO>`, so a repeated tick finds its job. */
  readonly dedupeKey?: (occurrence: ScheduleOccurrence) => string;
  readonly tenantKey?: string | null;
}

/** Enqueues any number of jobs per occurrence, in one transaction (e.g. one per tenant). */
export interface RunSchedule {
  readonly cron: string;
  readonly name: string;
  /**
   * Runs in a transaction on the outbox's connection, so reading app tables needs grants for the
   * outbox role. Derive every dedupe key from `scheduledAt`, so a repeated tick enqueues nothing new.
   */
  readonly run: (
    occurrence: ScheduleOccurrence & {
      readonly tx: OutboxDb;
      readonly enqueue: ScopedEnqueue<readonly AnyJob[]>;
    },
  ) => Promise<void>;
}

// oxlint-disable-next-line no-explicit-any -- each entry checks its own job's payload
export type Schedule = JobSchedule<any> | RunSchedule;

/** Type-checks a schedule's payload against its job. A plain object works too, untyped. */
export function defineSchedule<T extends AnyJob>(schedule: JobSchedule<T>): JobSchedule<T>;
export function defineSchedule(schedule: RunSchedule): RunSchedule;
export function defineSchedule(schedule: Schedule): Schedule {
  return schedule;
}

const nameOf = (schedule: Schedule) =>
  'run' in schedule ? schedule.name : (schedule.name ?? schedule.job.kind);

/** Rejects what can't work, at handler creation rather than on the first tick. */
export function validateSchedules(schedules: readonly Schedule[]) {
  const names = new Set<string>();

  for (const schedule of schedules) {
    const name = nameOf(schedule);

    if (typeof schedule.cron !== 'string' || schedule.cron.trim() === '') {
      throw new Error(`createScheduledHandler: schedule ${JSON.stringify(name)} needs a cron`);
    }

    if ('run' in schedule === 'job' in schedule) {
      throw new Error(
        `createScheduledHandler: schedule ${JSON.stringify(name)} needs either job + payload, or run`,
      );
    }

    if (!name) {
      throw new Error('createScheduledHandler: a run schedule needs a name');
    }

    if (names.has(name)) {
      throw new Error(`createScheduledHandler: two schedules are named ${JSON.stringify(name)}`);
    }

    names.add(name);
  }
}

/** Waits between attempts; a transient failure retries within the tick, as dedupe makes it safe. */
const RETRY_DELAYS_MS = [1_000, 4_000];

/** Runs every schedule whose cron is this tick's, each in its own transaction. Never throws. */
export async function runSchedules(
  outbox: Outbox,
  schedules: readonly Schedule[],
  cron: string | null,
  scheduledAtMs: number,
  deadlineAt: number,
) {
  const runtime = outbox[RUNTIME];
  const scheduledAt = new Date(scheduledAtMs);
  const due = schedules.filter((schedule) => schedule.cron === cron);

  for (const schedule of due) {
    const name = nameOf(schedule);
    const occurrence = { scheduledAt, cron: schedule.cron };
    const tally = { created: 0, existing: 0, rejected: 0 };

    for (let attempt = 1; ; attempt++) {
      tally.created = tally.existing = tally.rejected = 0;

      try {
        // Commits on this connection, but nudges only once it is released: recording a nudge may
        // check out another client, which a full pool would make wait on this one.
        const { pending } = await runtime.withConnection(
          (connection) =>
            runtime.collectTransaction(connection.db, async (tx, enqueue) => {
              const counted: ScopedEnqueue<readonly AnyJob[]> = async (job, request) => {
                const result = await enqueue(job, request);
                count(tally, result);

                return result;
              };

              if ('run' in schedule) {
                await schedule.run({ ...occurrence, tx, enqueue: counted });

                return;
              }

              const result = await counted(schedule.job, {
                payload: schedule.payload(occurrence),
                dedupeKey:
                  schedule.dedupeKey?.(occurrence) ??
                  `schedule:${name}:${scheduledAt.toISOString()}`,
                ...(schedule.tenantKey === undefined ? {} : { tenantKey: schedule.tenantKey }),
              });

              // One job is the whole occurrence: a rejected one is a failure, not a partial run.
              if (result.status !== 'created' && result.status !== 'exists') {
                throw new Error(`schedule ${name}: enqueue returned ${result.status}`);
              }
            }),
          deadlineAt,
        );

        // Committed: the outcome is settled before the best-effort nudge, which never throws.
        runtime.emit({
          type: 'schedule_ran',
          name,
          cron: schedule.cron,
          scheduledAt: scheduledAt.toISOString(),
          ...tally,
          attempts: attempt,
        });
        await runtime.nudgePending(pending, deadlineAt);
        break;
      } catch (error) {
        const delay = RETRY_DELAYS_MS[attempt - 1];
        const transient = isTransientDbError(error) || isConnectionError(error);

        if (transient && delay !== undefined && runtime.nowMs() + delay < deadlineAt) {
          await runtime.sleep(delay);
          continue;
        }

        runtime.emit({
          type: 'schedule_failed',
          severity: 'error',
          name,
          cron: schedule.cron,
          scheduledAt: scheduledAt.toISOString(),
          attempts: attempt,
          error: serializeError(error),
        });
        break;
      }
    }
  }
}

function count(
  tally: { created: number; existing: number; rejected: number },
  result: EnqueueResult,
) {
  if (result.status === 'created') {
    tally.created++;
  } else if (result.status === 'exists') {
    tally.existing++;
  } else {
    tally.rejected++;
  }
}
