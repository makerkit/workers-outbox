import type { AnyJob } from './define-job';

export interface ClaimableKind {
  readonly kind: string;
  readonly leaseMs: number;
}

export function createJobRegistry(jobs: readonly AnyJob[]) {
  if (jobs.length === 0) {
    throw new Error('createJobRegistry: at least one job is required');
  }

  const byKind = new Map<string, AnyJob>();

  for (const job of jobs) {
    if (byKind.has(job.kind)) {
      throw new Error(`createJobRegistry: duplicate kind ${JSON.stringify(job.kind)}`);
    }

    byKind.set(job.kind, job);
  }

  return {
    get: (kind: string) => byKind.get(kind),

    laneOf: (kind: string) => byKind.get(kind)?.lane,

    lanes: () => [...new Set([...byKind.values()].map((job) => job.lane))],

    // A job claimed without time to finish is killed mid-run, and each kill counts toward crash_loop.
    claimable: (
      budgetMs = Number.POSITIVE_INFINITY,
      options: { lane?: string } = {},
    ): ClaimableKind[] =>
      [...byKind.values()]
        .filter((job) => job.leaseMs <= budgetMs)
        .filter((job) => options.lane === undefined || job.lane === options.lane)
        .map((job) => ({ kind: job.kind, leaseMs: job.leaseMs })),
  };
}

export type JobRegistry = ReturnType<typeof createJobRegistry>;
