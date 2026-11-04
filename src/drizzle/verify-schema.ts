import { sql } from 'drizzle-orm';
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core';

import { MIN_SERVER_VERSION_NUM } from '../sql/migrations';
import { queryRows, readOutboxCatalog, type OutboxCatalog, type SqlRunner } from './catalog';
import { outboxAttempt, outboxJob } from './schema';

export type SchemaDrift =
  | { type: 'missing_table'; table: string }
  | { type: 'missing_column'; table: string; column: string }
  | { type: 'column_mismatch'; table: string; column: string; expected: string; actual: string }
  | { type: 'missing_constraint'; table: string; name: string }
  | { type: 'missing_index'; table: string; name: string }
  /** Left INVALID by an interrupted concurrent build: drop it and rebuild. */
  | { type: 'invalid_index'; table: string; name: string }
  | { type: 'unsupported_version'; minimum: number; actual: number };

const OUTBOX_TABLES = [outboxJob, outboxAttempt];

/** Empty = matches. Compares columns and names, not index/CHECK bodies; extra objects aren't drift. */
export async function verifyOutboxSchema(db: SqlRunner): Promise<SchemaDrift[]> {
  const [version] = await queryRows<{ server_version_num: string | number }>(
    db,
    sql`SELECT current_setting('server_version_num') AS server_version_num, now() AS read_at`,
  );
  const actual = Number(version?.server_version_num);
  const drift: SchemaDrift[] =
    actual < MIN_SERVER_VERSION_NUM
      ? [{ type: 'unsupported_version', minimum: MIN_SERVER_VERSION_NUM, actual }]
      : [];
  const catalog = await readOutboxCatalog(db);

  return [...drift, ...OUTBOX_TABLES.flatMap((table) => diffTable(table, catalog))];
}

function diffTable(table: PgTable, catalog: OutboxCatalog): SchemaDrift[] {
  const config = getTableConfig(table);
  const tableName = config.name;
  const liveColumns = catalog.columns.filter((column) => column.table === tableName);

  if (liveColumns.length === 0) {
    return [{ type: 'missing_table', table: tableName }];
  }

  const drift: SchemaDrift[] = [];

  for (const column of config.columns) {
    const live = liveColumns.find((candidate) => candidate.column === column.name);

    if (!live) {
      drift.push({ type: 'missing_column', table: tableName, column: column.name });
      continue;
    }

    const expected = describeColumn(column.getSQLType(), column.notNull);
    const actual = describeColumn(live.type, live.notNull);

    if (expected !== actual) {
      drift.push({
        type: 'column_mismatch',
        table: tableName,
        column: column.name,
        expected,
        actual,
      });
    }
  }

  const liveConstraints = new Set(
    catalog.constraints.filter((c) => c.table === tableName).map((c) => c.name),
  );

  for (const name of expectedConstraintNames(table)) {
    if (!liveConstraints.has(name)) {
      drift.push({ type: 'missing_constraint', table: tableName, name });
    }
  }

  const liveIndexes = new Map(
    catalog.indexes.filter((i) => i.table === tableName).map((i) => [i.name, i]),
  );

  for (const name of config.indexes.flatMap((index) => index.config.name ?? [])) {
    const live = liveIndexes.get(name);

    if (!live) {
      drift.push({ type: 'missing_index', table: tableName, name });
    } else if (!live.valid) {
      // pg_indexes lists it, but the planner never uses it: claims silently degrade.
      drift.push({ type: 'invalid_index', table: tableName, name });
    }
  }

  return drift;
}

function expectedConstraintNames(table: PgTable) {
  const config = getTableConfig(table);

  const columnPrimaryKey = config.columns.some((column) => column.primary)
    ? [`${config.name}_pkey`]
    : [];

  return [
    ...columnPrimaryKey,
    ...config.primaryKeys.map((pk) => pk.getName()),
    ...config.uniqueConstraints.flatMap((u) => u.getName() ?? []),
    ...config.foreignKeys.map((fk) => fk.getName()),
    ...config.checks.map((c) => c.name),
  ];
}

function describeColumn(type: string, notNull: boolean) {
  const normalized = type.replace(/\s+\(/g, '(').toLowerCase();

  return `${normalized}${notNull ? ' not null' : ''}`;
}
