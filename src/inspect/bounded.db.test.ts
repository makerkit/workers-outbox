import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { queryRows, type SqlRunner } from '../drizzle/catalog';
import { pgSuiteDrivers, pgUrls, testDrivers, type TestDriver } from '../test-support/drivers';
import { boundedRead } from './bounded';
import { countJobs } from './index';

const timeoutOf = async (db: SqlRunner) =>
  (await queryRows<{ t: string }>(db, sql`SELECT current_setting('statement_timeout') AS t`))[0]?.t;

describe.each(testDrivers())('bounded inspection reads on $name', ({ create }) => {
  let driver: TestDriver;

  beforeAll(async () => {
    driver = await create();
  });

  afterAll(() => driver.close());

  it('applies the bound inside the read only', async () => {
    const seen = await boundedRead(driver.db, { statementTimeoutMs: 1_234 }, timeoutOf);

    expect(seen).toBe('1234ms');
    expect(await timeoutOf(driver.db)).toBe('0');
  });

  it("restores the caller's setting when called inside the caller's transaction", async () => {
    const after = await driver.db.transaction(async (tx) => {
      await tx.execute(sql`SELECT set_config('statement_timeout', '7000', true)`);
      await boundedRead(tx, { statementTimeoutMs: 50 }, timeoutOf);

      return timeoutOf(tx);
    });

    expect(after).toBe('7s');
  });

  it('keeps the public signature: the bound rides in the input', async () => {
    expect(await countJobs(driver.db, { status: 'pending', statementTimeoutMs: 5_000 })).toBe(0);
  });

  it('rejects an out-of-range bound', async () => {
    await expect(boundedRead(driver.db, { statementTimeoutMs: -1 }, timeoutOf)).rejects.toThrow(
      /statementTimeoutMs/,
    );
  });
});

describe.skipIf(pgUrls().length === 0).each(pgSuiteDrivers())(
  'bounded inspection reads on $name',
  ({ create }) => {
    let driver: TestDriver;

    beforeAll(async () => {
      driver = await create();
    });

    afterAll(() => driver.close());

    it('cancels a read that runs past the bound (57014)', async () => {
      await expect(
        boundedRead(driver.db, { statementTimeoutMs: 100 }, (tx) =>
          queryRows(tx, sql`SELECT pg_sleep(5)`),
        ),
      ).rejects.toMatchObject({ cause: { code: '57014' } });
    });
  },
);
