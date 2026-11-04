import type { Outbox } from '../runtime/create-outbox';
import type { Budget, RunReport } from '../runtime/runtime';

import { RUNTIME } from '../runtime/create-outbox';
import { DEFAULT_RECOVERY_BUDGET_MS } from '../runtime/runtime';
import { serializeError } from '../runtime/serialize-error';
import {
  DEFAULT_SUCCEEDED_RETENTION_MS,
  DEFAULT_TERMINAL_RETENTION_MS,
} from '../store/outbox-store';
import { DEFAULT_INVOCATION_BUDGET_MS, MAX_INVOCATION_BUDGET_MS } from './limits';
import { runSchedules, validateSchedules, type Schedule } from './schedules';

export interface ScheduledHandlerOptions {
  /**
   * Wall time shared by every phase of one tick. Default 840 s; at most 870 s, under
   * Cloudflare's 15-minute limit for a scheduled invocation.
   */
  invocationBudgetMs?: number;
  /** Default: 50 jobs within 180 s. */
  budget?: Budget;
  /** Default 50. */
  recoveryLimit?: number;
  /**
   * Recovery's share of the tick, so a crash backlog can't starve fresh work. Default 120 s.
   * What doesn't fit stays expired for the next tick. Counted from when recovery starts, and held
   * back from `schedules` (up to half the invocation), so schedules can't consume it.
   */
  recoveryBudgetMs?: number;
  /** Drained before the catch-all so other kinds can't starve these; keep each ≤ 45 s. */
  reserved?: readonly { name: string; kinds: readonly string[]; budget: Budget }[];
  backlog?: Parameters<Outbox[typeof RUNTIME]['monitorBacklog']>[0];
  retention?: {
    succeededRetentionMs?: number;
    terminalRetentionMs?: number;
    batch?: number;
    maxBatches?: number;
  };
  /**
   * Recurring jobs. On a tick whose cron matches a schedule's, its jobs are enqueued (keyed by the
   * tick's scheduled time, so a repeated tick enqueues nothing new) before the sweep runs. A tick
   * that never fires is skipped, not caught up.
   */
  schedules?: readonly Schedule[];
}

export const DEFAULT_SWEEP_BUDGET: Budget = { maxJobs: 50, deadlineMs: 180_000 };

export interface ScheduledEventLike {
  readonly cron?: string;
  /** Milliseconds since the epoch; Cloudflare sets it. Default: when the tick runs. */
  readonly scheduledTime?: number;
}

/** The durable backstop: each phase is isolated, and the handler always resolves. */
export function createScheduledHandler<Env, Ctx = unknown>(
  outboxFor: (env: Env, ctx: Ctx) => Outbox,
  options: ScheduledHandlerOptions = {},
) {
  const budget = options.budget ?? DEFAULT_SWEEP_BUDGET;
  const recoveryLimit = options.recoveryLimit ?? 50;
  const recoveryBudgetMs = options.recoveryBudgetMs ?? DEFAULT_RECOVERY_BUDGET_MS;
  const invocationBudgetMs = options.invocationBudgetMs ?? DEFAULT_INVOCATION_BUDGET_MS;
  const schedules = options.schedules ?? [];

  validateSchedules(schedules);

  if (
    !Number.isInteger(invocationBudgetMs) ||
    invocationBudgetMs < 1 ||
    invocationBudgetMs > MAX_INVOCATION_BUDGET_MS
  ) {
    throw new Error(
      `createScheduledHandler: invocationBudgetMs must be an integer in [1, ${MAX_INVOCATION_BUDGET_MS}], got ${invocationBudgetMs}`,
    );
  }
  if (!Number.isInteger(recoveryBudgetMs) || recoveryBudgetMs < 1) {
    throw new Error(
      `createScheduledHandler: recoveryBudgetMs must be a positive integer, got ${recoveryBudgetMs}`,
    );
  }

  // Never more than half the tick: schedules keep a window even under a small invocation budget.
  const recoveryReserveMs = Math.min(recoveryBudgetMs, Math.floor(invocationBudgetMs / 2));

  const retention = {
    succeededRetentionMs: options.retention?.succeededRetentionMs ?? DEFAULT_SUCCEEDED_RETENTION_MS,
    terminalRetentionMs: options.retention?.terminalRetentionMs ?? DEFAULT_TERMINAL_RETENTION_MS,
    batch: options.retention?.batch ?? 1_000,
    maxBatches: options.retention?.maxBatches ?? 10,
  };

  return async (event: ScheduledEventLike, env: Env, ctx: Ctx) => {
    const outbox = outboxFor(env, ctx);
    const runtime = outbox[RUNTIME];
    const cron = event.cron ?? null;

    const phase = async <T>(
      name: 'recover' | 'reserved' | 'drain' | 'handoff' | 'cleanup' | 'backlog',
      work: () => Promise<T>,
    ) => {
      try {
        return await work();
      } catch (error) {
        runtime.emit({ type: 'phase_failed', phase: name, cron, error: serializeError(error) });

        return undefined;
      }
    };

    // One deadline for the whole tick: every phase, and everything each phase awaits, shares it.
    const startedAt = runtime.nowMs();
    const deadlineAt = startedAt + invocationBudgetMs;
    const within = { deadlineAt };

    if (schedules.length > 0) {
      // Never throws: each schedule reports its own failure, and the sweep runs regardless.
      // Recovery's slice is held back, so slow schedules can't strand expired leases tick after tick.
      await runSchedules(
        outbox,
        schedules,
        cron,
        event.scheduledTime ?? startedAt,
        deadlineAt - recoveryReserveMs,
      );
    }

    // Measured from when recovery starts, not the tick: schedule time is not recovery's.
    const recovery = await phase('recover', () =>
      runtime.recover(recoveryLimit, {
        deadlineAt: Math.min(deadlineAt, runtime.nowMs() + recoveryBudgetMs),
      }),
    );

    for (const slice of options.reserved ?? []) {
      await phase('reserved', () =>
        runtime.drain(slice.budget, 'sweep', { kinds: slice.kinds }, within),
      );
    }

    const report = await phase('drain', () => runtime.drain(budget, 'sweep', {}, within));
    await phase('handoff', () => runtime.handoff(budget.deadlineMs, 100, within));
    const deleted = await phase('cleanup', () => runtime.cleanup(retention, within));

    let continued = false;

    if (report && report.claimed > report.errored) {
      continued =
        (await phase('drain', async () => {
          let any = false;

          for (const lane of runtime.lanes()) {
            if (
              (await runtime.hasBacklog({ lane }, within)) &&
              (await runtime.continueDrain(lane, within))
            ) {
              any = true;
            }
          }

          return any;
        })) ?? false;
    }

    if (options.backlog) {
      const backlogOptions = options.backlog;
      await phase('backlog', () => runtime.monitorBacklog({ ...backlogOptions, ...within }));
    }

    runtime.emit({
      type: 'sweep_finished',
      cron,
      recovered: recovery ? recovery.retrying + recovery.dead : 0,
      report: report ?? emptySweepReport(),
      deleted: deleted ?? 0,
      continued,
    });
  };
}

function emptySweepReport(): RunReport {
  return {
    claimed: 0,
    succeeded: 0,
    retrying: 0,
    dead: 0,
    cancelled: 0,
    leaseLost: 0,
    settleFailed: 0,
    byKind: {},
    errored: 0,
    erroredSamples: [],
    reachedLimit: false,
    reachedDeadline: false,
  };
}
