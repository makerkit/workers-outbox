import type { SQL } from 'drizzle-orm';

import { PgDialect } from 'drizzle-orm/pg-core';

import type { SqlRunner } from '../drizzle/catalog';

export interface RecordedStatement {
  sql: string;
  params: unknown[];
}

export function recordingRunner(inner: SqlRunner) {
  const dialect = new PgDialect();
  const recorded: RecordedStatement[] = [];

  const runner: SqlRunner = {
    execute: (query: SQL) => {
      const { sql, params } = dialect.sqlToQuery(query);
      recorded.push({ sql, params });

      return inner.execute(query);
    },
  };

  return { runner, recorded, statements: () => recorded.map((r) => r.sql) };
}
