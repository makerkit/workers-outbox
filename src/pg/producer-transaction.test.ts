import { entityKind } from 'drizzle-orm';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

import { hasStockTransaction } from './producer-transaction';

// stand-ins for Drizzle's base database on each line: 1.0 (PgAsyncDatabase) and 0.x (PgDatabase)
const stockDatabase = (kind: string) => {
  class Base {
    static readonly [entityKind]: string = kind;
    transaction() {}
  }
  class NodePgDatabase extends Base {
    static override readonly [entityKind]: string = 'NodePgDatabase';
  }
  return NodePgDatabase;
};

describe('hasStockTransaction', () => {
  it.each(['PgAsyncDatabase', 'PgDatabase'])('recognises %s as Drizzle own method', (kind) => {
    const Database = stockDatabase(kind);
    expect(hasStockTransaction(new Database())).toBe(true);
  });

  it('refuses an overridden, bound or proxied transaction', () => {
    const Database = stockDatabase('PgDatabase');
    class Router extends Database {
      override transaction() {}
    }
    const bound = new Database();
    bound.transaction = bound.transaction.bind(bound);
    const proxied = new Proxy(new Database(), {
      get: (target, key, receiver) =>
        key === 'transaction' ? () => undefined : Reflect.get(target, key, receiver),
    });

    expect(hasStockTransaction(new Router())).toBe(false);
    expect(hasStockTransaction(bound)).toBe(false);
    expect(hasStockTransaction(proxied)).toBe(false);
  });

  it('refuses a database that is not Drizzle', () => {
    expect(hasStockTransaction({ transaction() {} })).toBe(false);
  });

  it('names no export that only one Drizzle line has', () => {
    const source = readFileSync(new URL('./producer-transaction.ts', import.meta.url), 'utf8');
    expect(source).not.toMatch(/import \{[^}]*\bPg(Async)?Database\b/);
  });
});
