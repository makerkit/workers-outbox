import { sql, type SQL } from 'drizzle-orm';

/** Drivers disagree on the result shape, so read through `queryRows`. */
export interface SqlRunner {
  execute(query: SQL): PromiseLike<unknown>;
}

export async function queryRows<Row>(db: SqlRunner, query: SQL) {
  const result = await db.execute(query);

  if (Array.isArray(result)) {
    return result as Row[];
  }

  return (result as { rows: Row[] }).rows;
}

export interface CatalogColumn {
  table: string;
  column: string;
  type: string;
  notNull: boolean;
  default: string | null;
}

export interface CatalogConstraint {
  table: string;
  name: string;
  type: 'p' | 'u' | 'f' | 'c';
  definition: string;
}

export interface CatalogIndex {
  table: string;
  name: string;
  definition: string;
  /** False for an index an interrupted `CREATE INDEX CONCURRENTLY` left behind: the planner skips it. */
  valid: boolean;
}

export interface OutboxCatalog {
  columns: CatalogColumn[];
  constraints: CatalogConstraint[];
  indexes: CatalogIndex[];
}

// Only p/u/f/c constraints: Postgres 18 also records NOT NULL ones, which would vary by version.
export async function readOutboxCatalog(db: SqlRunner): Promise<OutboxCatalog> {
  const columns = await queryRows<{
    table: string;
    column: string;
    type: string;
    not_null: boolean;
    default: string | null;
  }>(
    db,
    sql`SELECT c.relname AS "table", a.attname AS "column",
               format_type(a.atttypid, a.atttypmod) AS "type",
               a.attnotnull AS "not_null",
               pg_get_expr(d.adbin, d.adrelid) AS "default"
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE n.nspname = 'outbox' AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY 1, 2`,
  );

  const constraints = await queryRows<CatalogConstraint>(
    db,
    sql`SELECT c.relname AS "table", con.conname AS "name", con.contype AS "type",
               pg_get_constraintdef(con.oid) AS "definition"
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'outbox' AND con.contype IN ('p', 'u', 'f', 'c')
        ORDER BY 1, 2`,
  );

  const indexes = await queryRows<CatalogIndex>(
    db,
    sql`SELECT tc.relname AS "table", ic.relname AS "name",
               pg_get_indexdef(ic.oid) AS "definition", i.indisvalid AS "valid"
        FROM pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_class tc ON tc.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = tc.relnamespace
        WHERE n.nspname = 'outbox'
        ORDER BY 1, 2`,
  );

  return {
    columns: columns.map((row) => ({
      table: row.table,
      column: row.column,
      type: row.type,
      notNull: row.not_null,
      default: row.default,
    })),
    constraints,
    indexes,
  };
}
