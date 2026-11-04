import { describe, expect, it } from 'vitest';

import {
  DEFAULT_BACKLOG_THRESHOLDS,
  evaluateBacklog,
  UNREGISTERED_LANE,
  type KindBacklog,
} from './backlog';

const kind = (name: string, counts: Partial<KindBacklog> = {}): KindBacklog => ({
  kind: name,
  due: 0,
  aged: 0,
  scheduled: 0,
  processing: 0,
  stuck: 0,
  oldestDueAgeMs: null,
  ...counts,
});

const LANES: Record<string, string> = { email: 'default', crm: 'default', backfill: 'bulk' };
const laneOf = (k: string) => LANES[k];

const thresholds = { maxDue: 100, maxAged: 10 };

const evaluate = (counts: KindBacklog[], lanes = {}) =>
  evaluateBacklog({ counts, laneOf, thresholds, lanes });

describe('evaluateBacklog', () => {
  // port: outbox-depth.monitor:72
  // port: outbox-depth.monitor:255
  it('reports zero depth and no alert for an empty backlog', () => {
    expect(evaluate([])).toEqual({
      severity: 'ok',
      totals: { due: 0, aged: 0, scheduled: 0, processing: 0, stuck: 0 },
      lanes: [],
      alert: null,
    });
  });

  // port: outbox-depth.monitor:86
  // port: outbox-depth.monitor:97
  // port: outbox-depth.monitor:155
  it('keeps per-kind counts, due separate from scheduled and processing', () => {
    const report = evaluate([
      kind('email', { due: 3, scheduled: 7, processing: 2 }),
      kind('crm', { due: 1 }),
    ]);

    expect(report.lanes).toHaveLength(1);
    expect(report.lanes[0]).toMatchObject({
      lane: 'default',
      due: 4,
      scheduled: 7,
      processing: 2,
      kinds: [
        expect.objectContaining({ kind: 'email', due: 3 }),
        expect.objectContaining({ kind: 'crm', due: 1 }),
      ],
    });
    expect(report.alert).toBeNull();
  });

  // port: outbox-depth.monitor:449
  it('raises nothing for a backlog at or under its thresholds', () => {
    expect(evaluate([kind('email', { due: 100, aged: 10 })]).severity).toBe('ok');
  });

  // port: outbox-depth.monitor:174
  // port: outbox-depth.monitor:462
  it('warns when due jobs exceed the threshold', () => {
    const report = evaluate([kind('email', { due: 150 })]);

    expect(report.severity).toBe('warning');
    expect(report.lanes[0]?.breaches).toEqual([
      { metric: 'due', value: 150, threshold: 100, severity: 'warning' },
    ]);
  });

  // port: outbox-depth.monitor:108
  // port: outbox-depth.monitor:186
  // port: outbox-depth.monitor:359
  it('grades aging: over the threshold is a warning, at twice it critical', () => {
    expect(evaluate([kind('email', { due: 15, aged: 15 })]).severity).toBe('warning');
    expect(evaluate([kind('email', { due: 19, aged: 19 })]).severity).toBe('warning');
    expect(evaluate([kind('email', { due: 20, aged: 20 })]).severity).toBe('critical');
  });

  // port: outbox-depth.monitor:144
  // port: outbox-depth.monitor:215
  // port: outbox-depth.monitor:227
  // port: outbox-depth.monitor:274
  it('treats any stuck job as critical, with a message and the numbers', () => {
    const report = evaluate([kind('email', { processing: 1, stuck: 1 })]);

    expect(report.severity).toBe('critical');
    expect(report.alert).toMatchObject({
      severity: 'critical',
      message: 'outbox backlog critical: default stuck 1 > 0',
      totals: { stuck: 1, processing: 1 },
    });
  });

  // port: outbox-depth.monitor:199
  it('does not call a long run inside its lease stuck', () => {
    expect(evaluate([kind('email', { processing: 5, stuck: 0 })]).alert).toBeNull();
  });

  // port: outbox-depth.monitor:380
  it('puts the thresholds in the alert payload', () => {
    const report = evaluate([kind('email', { due: 150 })]);

    expect(report.alert?.lanes[0]?.thresholds).toEqual({
      ...DEFAULT_BACKLOG_THRESHOLDS,
      ...thresholds,
    });
  });

  // port: outbox-depth.monitor:312
  it('escalates aging on an urgent lane to critical', () => {
    const report = evaluate([kind('email', { due: 30, aged: 30 })]);

    expect(report.lanes[0]).toMatchObject({ lane: 'default', severity: 'critical' });
  });

  // port: outbox-depth.monitor:335
  // port: outbox-depth.monitor:426
  it('never pages critical for a lane capped at warning, however deep', () => {
    const report = evaluate([kind('backfill', { due: 10_000, aged: 5_000, stuck: 3 })], {
      bulk: { maxSeverity: 'warning' },
    });

    expect(report.severity).toBe('warning');
    expect(report.lanes[0]?.breaches.map((b) => b.severity)).toEqual([
      'warning',
      'warning',
      'warning',
    ]);
  });

  // port: outbox-depth.monitor:410
  it('splits counts per lane and sums them globally', () => {
    const report = evaluate([
      kind('email', { due: 2, aged: 1 }),
      kind('backfill', { due: 40, scheduled: 5 }),
      kind('crm', { due: 3, stuck: 0 }),
    ]);

    expect(report.lanes.map((l) => [l.lane, l.due])).toEqual([
      ['default', 5],
      ['bulk', 40],
    ]);
    expect(report.totals).toEqual({ due: 45, aged: 1, scheduled: 5, processing: 0, stuck: 0 });
  });

  it('applies per-lane thresholds over the shared ones', () => {
    const report = evaluate([kind('email', { due: 50 }), kind('backfill', { due: 50 })], {
      default: { maxDue: 10 },
    });

    expect(report.lanes.map((l) => [l.lane, l.severity])).toEqual([
      ['default', 'critical'],
      ['bulk', 'ok'],
    ]);
  });

  it('surfaces a due backlog of a kind no job handles, which would never drain', () => {
    const report = evaluate([kind('orphan', { due: 1, oldestDueAgeMs: 5_000 })]);

    expect(report.lanes[0]).toMatchObject({
      lane: UNREGISTERED_LANE,
      severity: 'warning',
      oldestDueAgeMs: 5_000,
    });
    expect(report.alert?.message).toContain('(unregistered) due 1 > 0');
  });
});
