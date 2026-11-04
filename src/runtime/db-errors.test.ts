import { describe, expect, it } from 'vitest';

import { isConnectionError, isTransientDbError, sqlstateOf } from './db-errors';

const pgError = (code: string) => Object.assign(new Error(`pg ${code}`), { code });

describe('database error classification', () => {
  it('reads the SQLSTATE through a wrapper with no code of its own', () => {
    expect(sqlstateOf(new Error('Failed query', { cause: pgError('40P01') }))).toBe('40P01');
    expect(sqlstateOf(new Error('no code'))).toBeNull();
    expect(sqlstateOf('not an error')).toBeNull();
  });

  it.each(['40001', '40P01', '55P03', '57014', '53300'])('treats %s as transient', (code) => {
    expect(isTransientDbError(new Error('wrapped', { cause: pgError(code) }))).toBe(true);
  });

  it.each(['23505', '23514', '42P01'])('treats %s as permanent', (code) => {
    expect(isTransientDbError(pgError(code))).toBe(false);
  });

  it('never classifies by message text', () => {
    expect(isTransientDbError(new Error('deadlock detected'))).toBe(false);
  });

  it.each([
    ['a socket reset', Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' })],
    ['a refused connection', Object.assign(new Error('connect'), { code: 'ECONNREFUSED' })],
    ['an SQLSTATE connection exception', Object.assign(new Error('x'), { code: '08006' })],
    ['an admin shutdown', Object.assign(new Error('x'), { code: '57P01' })],
    ['node-pg termination', new Error('Connection terminated unexpectedly')],
    [
      'node-pg refusing a dead client',
      new Error('Client has encountered a connection error and is not queryable'),
    ],
    ['node-pg refusing a closed client', new Error('Client was closed and is not queryable')],
  ])('treats %s, wrapped, as a connection error that is retryable', (_, cause) => {
    const error = new Error('Failed query', { cause });

    expect(isConnectionError(error)).toBe(true);
    expect(isTransientDbError(error)).toBe(true);
  });

  it('does not treat a constraint violation as a connection error', () => {
    expect(
      isConnectionError(
        new Error('x', { cause: Object.assign(new Error('dup'), { code: '23505' }) }),
      ),
    ).toBe(false);
  });

  it.each(['40003', '53000', '53100', '53200', '53400'])('treats %s as transient', (code) => {
    expect(isTransientDbError(new Error('wrapped', { cause: pgError(code) }))).toBe(true);
  });

  it.each([
    'ENETUNREACH',
    'ECONNABORTED',
    'EAI_AGAIN',
    'CONNECTION_CLOSED',
    'CONNECTION_ENDED',
    'CONNECTION_DESTROYED',
    'CONNECT_TIMEOUT',
  ])('treats %s as a connection error', (code) => {
    expect(
      isConnectionError(new Error('x', { cause: Object.assign(new Error(code), { code }) })),
    ).toBe(true);
  });

  it('looks inside an AggregateError (connect retried several addresses)', () => {
    const aggregate = new AggregateError(
      [new Error('first'), Object.assign(new Error('reset'), { code: 'ECONNRESET' })],
      'all failed',
    );

    expect(isConnectionError(new Error('Failed query', { cause: aggregate }))).toBe(true);
  });
});
