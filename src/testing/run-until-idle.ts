import { sql } from 'drizzle-orm';

import type { OutcomeTally } from '../runtime/runtime';
import type { TestClock } from './test-clock';

import { queryRows, type SqlRunner } from '../drizzle/catalog';
import { RUNTIME, type Outbox } from '../runtime/create-outbox';

export interface RunUntilIdleOptions {
  /**
   * With a test clock (the one the outbox was created with), time skips ahead to the next retry,
   * delayed job or expired lease until nothing is left. Without one, only jobs due now run.
   */
  clock?: TestClock;
  /** Reads the next due instant; required with `clock`. */
  db?: SqlRunner;
  /** Throws past this many rounds, so an always-retrying job can't hang a test. Default 100. */
  maxRounds?: number;
}

export interface IdleReport extends OutcomeTally {
  rounds: number;
}

const TALLY_KEYS = [
  'claimed',
  'succeeded',
  'retrying',
  'dead',
  'cancelled',
  'leaseLost',
  'settleFailed',
] as const satisfies readonly (keyof OutcomeTally)[];

// Generous: a test drain is bounded by the jobs it finds, not by time.
const BUDGET = { maxJobs: 10_000, deadlineMs: 600_000 };

/** Runs every job this outbox can run, then returns the totals. */
export async function runUntilIdle(
  outbox: Outbox,
  options: RunUntilIdleOptions = {},
): Promise<IdleReport> {
  const { clock, db, maxRounds = 100 } = options;

  if (clock && !db) {
    throw new Error('runUntilIdle: pass `db` with `clock`, to find the next due job');
  }

  const runtime = outbox[RUNTIME];
  const totals: IdleReport = {
    rounds: 0,
    ...Object.fromEntries(TALLY_KEYS.map((k) => [k, 0])),
  } as IdleReport;
  const add = (report: OutcomeTally) => {
    for (const key of TALLY_KEYS) {
      totals[key] += report[key];
    }
  };

  for (;;) {
    if (totals.rounds >= maxRounds) {
      throw new Error(
        `runUntilIdle: still busy after ${maxRounds} rounds (${JSON.stringify(totals)})`,
      );
    }

    totals.rounds++;
    const drained = await runtime.drain(BUDGET, 'drain');
    add(drained);

    if (drained.claimed > 0) {
      continue;
    }

    if (!clock || !db) {
      return totals;
    }

    const recovered = await runtime.recover(BUDGET.maxJobs);
    add(recovered);

    // Recovery settles expired leases without claiming: any outcome is progress.
    if (TALLY_KEYS.some((key) => recovered[key] > 0)) {
      continue;
    }

    const next = await nextWakeAt(db);

    // Nothing left, or due already but not ours to run (another kind, a newer deploy's job).
    if (next === null || next <= clock.nowMs()) {
      return totals;
    }

    clock.advance(next - clock.nowMs());
  }
}

async function nextWakeAt(db: SqlRunner): Promise<number | null> {
  const [row] = await queryRows<{ at: string | Date | null }>(
    db,
    sql`SELECT LEAST(
          (SELECT min(COALESCE(next_attempt_at, available_at)) FROM "outbox"."job"
            WHERE status IN ('pending', 'retrying')),
          (SELECT min(leased_until) FROM "outbox"."job" WHERE status = 'processing')
        ) AS at, now() AS read_at`,
  );

  return row?.at == null ? null : new Date(row.at).getTime();
}
