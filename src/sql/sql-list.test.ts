import { describe, expect, it } from 'vitest';

import { sqlLiteralList } from './sql-list';

describe('sqlLiteralList', () => {
  it('renders constants as a quoted SQL list', () => {
    expect(sqlLiteralList(['pending', 'lease_expired'])).toBe(`'pending', 'lease_expired'`);
  });

  it('refuses anything that is not a lowercase constant', () => {
    expect(() => sqlLiteralList(["x'); DROP TABLE job; --"])).toThrow(/non-constant/);
    expect(() => sqlLiteralList(['Pending'])).toThrow(/non-constant/);
  });
});
