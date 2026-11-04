export interface KindBacklog {
  readonly kind: string;
  readonly due: number;
  readonly aged: number;
  readonly scheduled: number;
  readonly processing: number;
  readonly stuck: number;
  readonly oldestDueAgeMs: number | null;
}

export interface BacklogThresholds {
  readonly maxDue: number;
  readonly maxAged: number;
  readonly maxStuck: number;
  /** Critical at `criticalFactor ×` the threshold (any value over a 0 threshold). */
  readonly criticalFactor: number;
  /** `'warning'` for lanes that must never page. */
  readonly maxSeverity: 'warning' | 'critical';
}

export type BacklogSeverity = 'ok' | 'warning' | 'critical';

export type BacklogMetric = 'due' | 'aged' | 'stuck';

export interface BacklogBreach {
  readonly metric: BacklogMetric;
  readonly value: number;
  readonly threshold: number;
  readonly severity: Exclude<BacklogSeverity, 'ok'>;
}

export interface BacklogTotals {
  readonly due: number;
  readonly aged: number;
  readonly scheduled: number;
  readonly processing: number;
  readonly stuck: number;
}

export interface LaneBacklog extends BacklogTotals {
  readonly lane: string;
  readonly kinds: readonly KindBacklog[];
  readonly oldestDueAgeMs: number | null;
  readonly severity: BacklogSeverity;
  readonly breaches: readonly BacklogBreach[];
  readonly thresholds: BacklogThresholds;
}

export interface BacklogAlert {
  readonly severity: Exclude<BacklogSeverity, 'ok'>;
  readonly message: string;
  readonly lanes: readonly LaneBacklog[];
  readonly totals: BacklogTotals;
}

export interface BacklogReport {
  readonly severity: BacklogSeverity;
  readonly totals: BacklogTotals;
  readonly lanes: readonly LaneBacklog[];
  readonly alert: BacklogAlert | null;
}

export const DEFAULT_BACKLOG_THRESHOLDS: BacklogThresholds = {
  maxDue: 1_000,
  maxAged: 100,
  maxStuck: 0,
  criticalFactor: 2,
  maxSeverity: 'critical',
};

/** Milliseconds a due job may wait before it counts as aged. */
export const DEFAULT_BACKLOG_AGED_MS = 900_000;

/** Kinds no job handles never drain; the name fails the lane pattern, so it can't collide. */
export const UNREGISTERED_LANE = '(unregistered)';

const UNREGISTERED_THRESHOLDS: Partial<BacklogThresholds> = {
  maxDue: 0,
  maxAged: 0,
  maxSeverity: 'warning',
};

const SEVERITY_RANK: Record<BacklogSeverity, number> = { ok: 0, warning: 1, critical: 2 };

export function evaluateBacklog(input: {
  counts: readonly KindBacklog[];
  laneOf: (kind: string) => string | undefined;
  thresholds?: Partial<BacklogThresholds>;
  lanes?: Readonly<Record<string, Partial<BacklogThresholds>>>;
}): BacklogReport {
  const byLane = new Map<string, KindBacklog[]>();

  for (const count of input.counts) {
    const lane = input.laneOf(count.kind) ?? UNREGISTERED_LANE;
    byLane.set(lane, [...(byLane.get(lane) ?? []), count]);
  }

  const lanes = [...byLane.entries()].map(([lane, kinds]) =>
    gradeLane(lane, kinds, resolveThresholds(lane, input)),
  );
  const totals = sumTotals(lanes);
  const severity = lanes.reduce<BacklogSeverity>((worst, l) => worse(worst, l.severity), 'ok');

  if (severity === 'ok') {
    return { severity, totals, lanes, alert: null };
  }

  const breaching = lanes.filter((lane) => lane.severity !== 'ok');

  return {
    severity,
    totals,
    lanes,
    alert: { severity, message: alertMessage(severity, breaching), lanes: breaching, totals },
  };
}

function resolveThresholds(
  lane: string,
  input: {
    thresholds?: Partial<BacklogThresholds>;
    lanes?: Readonly<Record<string, Partial<BacklogThresholds>>>;
  },
): BacklogThresholds {
  return {
    ...DEFAULT_BACKLOG_THRESHOLDS,
    ...(lane === UNREGISTERED_LANE ? UNREGISTERED_THRESHOLDS : input.thresholds),
    ...input.lanes?.[lane],
  };
}

function gradeLane(
  lane: string,
  kinds: readonly KindBacklog[],
  thresholds: BacklogThresholds,
): LaneBacklog {
  const totals = sumTotals(kinds);
  const breaches = (
    [
      ['due', totals.due, thresholds.maxDue],
      ['aged', totals.aged, thresholds.maxAged],
      ['stuck', totals.stuck, thresholds.maxStuck],
    ] as const
  ).flatMap(([metric, value, threshold]): BacklogBreach[] => {
    if (value <= threshold) {
      return [];
    }

    const critical = value >= threshold * thresholds.criticalFactor;
    const severity = critical && thresholds.maxSeverity === 'critical' ? 'critical' : 'warning';

    return [{ metric, value, threshold, severity }];
  });

  const ages = kinds.flatMap((k) => (k.oldestDueAgeMs === null ? [] : [k.oldestDueAgeMs]));

  return {
    lane,
    kinds,
    ...totals,
    oldestDueAgeMs: ages.length === 0 ? null : Math.max(...ages),
    severity: breaches.reduce<BacklogSeverity>((worst, b) => worse(worst, b.severity), 'ok'),
    breaches,
    thresholds,
  };
}

function sumTotals(rows: readonly BacklogTotals[]): BacklogTotals {
  return rows.reduce(
    (sum, row) => ({
      due: sum.due + row.due,
      aged: sum.aged + row.aged,
      scheduled: sum.scheduled + row.scheduled,
      processing: sum.processing + row.processing,
      stuck: sum.stuck + row.stuck,
    }),
    { due: 0, aged: 0, scheduled: 0, processing: 0, stuck: 0 },
  );
}

function worse(a: BacklogSeverity, b: BacklogSeverity) {
  return SEVERITY_RANK[b] > SEVERITY_RANK[a] ? b : a;
}

function alertMessage(severity: BacklogSeverity, lanes: readonly LaneBacklog[]) {
  const parts = lanes.flatMap((lane) =>
    lane.breaches.map((b) => `${lane.lane} ${b.metric} ${b.value} > ${b.threshold}`),
  );

  return `outbox backlog ${severity}: ${parts.join('; ')}`;
}
