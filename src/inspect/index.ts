import type { SqlRunner } from '../drizzle/catalog';

import * as store from '../store/outbox-store';
import { boundedRead, type InspectBound } from './bounded';

/** Each read runs under a transaction-local `statement_timeout` (default 30 s; see `InspectBound`). */
const bounded =
  <I extends object, R>(read: (db: SqlRunner, input: I) => Promise<R>) =>
  (db: SqlRunner, input: I & InspectBound): Promise<R> => {
    const { statementTimeoutMs, ...rest } = input;

    return boundedRead(db, { statementTimeoutMs }, (tx) => read(tx, rest as unknown as I));
  };

export const aggregateAttemptOutcomes = bounded(store.aggregateAttemptOutcomes);
export const countBacklog = bounded(store.countBacklog);
export const countJobs = bounded(store.countJobs);
export const findJobsByCorrelationId = bounded(store.findJobsByCorrelationId);
export const getJob = bounded(store.getJob);
export const listJobs = bounded(store.listJobs);

export { DEFAULT_INSPECT_STATEMENT_TIMEOUT_MS, type InspectBound } from './bounded';
export {
  DEFAULT_BACKLOG_AGED_MS,
  DEFAULT_BACKLOG_THRESHOLDS,
  evaluateBacklog,
  type BacklogReport,
  type BacklogSeverity,
  type BacklogThresholds,
  type KindBacklog,
} from '../core/backlog';
