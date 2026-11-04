import type { Outbox } from '../runtime/create-outbox';
import type { Budget } from '../runtime/runtime';

import { MAX_LEASE_MS } from '../core/define-job';
import { RUNTIME } from '../runtime/create-outbox';
import { serializeError } from '../runtime/serialize-error';
import { DEFAULT_INVOCATION_BUDGET_MS, MAX_INVOCATION_BUDGET_MS } from './limits';
import { decodeMessage } from './message-codec';

export interface QueueMessage {
  readonly body: unknown;
  readonly attempts: number;
  ack(): void;
  retry(options?: { delaySeconds?: number }): void;
}

export interface QueueBatch {
  readonly queue: string;
  readonly messages: readonly QueueMessage[];
}

export interface QueueHandlerOptions {
  /**
   * Wall time shared by every message in one batch. Default 840 s (14 min); at most 870 s,
   * leaving headroom under Cloudflare's 15-minute consumer limit.
   */
  invocationBudgetMs?: number;
  /** Default: 100 jobs within the invocation budget. Its deadline must hold the longest legal lease (635 s). */
  nudgeBudget?: Budget;
  /** Default: 50 jobs within 45 s. */
  drainBudget?: Budget;
  /** Default `'default'`; only this lane's kinds are claimed. */
  lane?: string;
}

export { DEFAULT_INVOCATION_BUDGET_MS };

const DEFAULT_DRAIN_BUDGET: Budget = { maxJobs: 50, deadlineMs: 45_000 };

export const messageRetryDelaySeconds = (attempts: number) =>
  Math.min(300, 5 * 2 ** Math.max(0, attempts - 1));

/** A handler failure is a job outcome; only an infrastructure fault retries the message. */
export function createQueueHandler<Env, Ctx = unknown>(
  outboxFor: (env: Env, ctx: Ctx) => Outbox,
  options: QueueHandlerOptions = {},
) {
  const invocationBudgetMs = options.invocationBudgetMs ?? DEFAULT_INVOCATION_BUDGET_MS;

  if (
    !Number.isInteger(invocationBudgetMs) ||
    invocationBudgetMs < MAX_LEASE_MS ||
    invocationBudgetMs > MAX_INVOCATION_BUDGET_MS
  ) {
    throw new Error(
      `createQueueHandler: invocationBudgetMs must be an integer in [${MAX_LEASE_MS}, ${MAX_INVOCATION_BUDGET_MS}], got ${invocationBudgetMs}`,
    );
  }

  const nudgeBudget = options.nudgeBudget ?? { maxJobs: 100, deadlineMs: invocationBudgetMs };

  const drainBudget = options.drainBudget ?? DEFAULT_DRAIN_BUDGET;
  const lane = options.lane ?? 'default';

  // A nudge whose kind can never fit would re-nudge forever (only time-sliced misses are transient).
  if (nudgeBudget.deadlineMs < MAX_LEASE_MS || nudgeBudget.deadlineMs > invocationBudgetMs) {
    throw new Error(
      `createQueueHandler: nudgeBudget.deadlineMs must be in [${MAX_LEASE_MS}, ${invocationBudgetMs}] (the longest legal lease, the invocation budget), got ${nudgeBudget.deadlineMs}`,
    );
  }

  if (drainBudget.deadlineMs > invocationBudgetMs) {
    throw new Error(
      `createQueueHandler: drainBudget.deadlineMs must not exceed invocationBudgetMs (${invocationBudgetMs}), got ${drainBudget.deadlineMs}`,
    );
  }

  return async (batch: QueueBatch, env: Env, ctx: Ctx) => {
    const runtime = outboxFor(env, ctx)[RUNTIME];

    // One deadline for the whole batch: the platform's wall-time limit is per invocation.
    const started = runtime.nowMs();
    const deadlineAt = started + invocationBudgetMs;

    const within = (budget: Budget): Budget => ({
      ...budget,
      deadlineMs: Math.max(
        0,
        Math.min(budget.deadlineMs, invocationBudgetMs - (runtime.nowMs() - started)),
      ),
    });

    for (const message of batch.messages) {
      const decoded = decodeMessage(message.body);

      if (!decoded.ok) {
        runtime.emit({ type: 'message_rejected', reason: decoded.reason, body: decoded.body });
        message.ack();

        continue;
      }

      const { message: body } = decoded;

      runtime.emit({
        type: 'message_received',
        messageType: body.type,
        ids: body.type === 'nudge' ? body.ids : [],
        attempts: message.attempts,
      });

      try {
        if (body.type === 'nudge') {
          // Out of time, its jobs come back as over_budget and are re-nudged to a fresh invocation.
          await runtime.processNudge(body.ids, within(nudgeBudget), { lane }, { deadlineAt });
        } else {
          const budget = within(drainBudget);
          const report = await runtime.drain(budget, 'drain', { lane }, { deadlineAt });
          // A drain that only hit errors must not chain itself forever.
          const progressed = report.claimed > report.errored;
          // Cut short by earlier messages: hand the rest to a fresh invocation.
          const starved = budget.deadlineMs < drainBudget.deadlineMs && report.reachedDeadline;

          if ((progressed || starved) && (await runtime.hasBacklog({ lane }, { deadlineAt }))) {
            await runtime.continueDrain(lane, { deadlineAt });
          }
        }

        message.ack();
      } catch (error) {
        const delaySeconds = messageRetryDelaySeconds(message.attempts);

        runtime.emit({
          type: 'message_retried',
          messageType: body.type,
          delaySeconds,
          attempts: message.attempts,
          error: serializeError(error),
        });

        message.retry({ delaySeconds });
      }
    }
  };
}
