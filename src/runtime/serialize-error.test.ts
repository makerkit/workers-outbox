import { describe, expect, it } from 'vitest';

import { serializeError } from './serialize-error';

describe('serializeError', () => {
  // port: process-submission-outbox.service:3341
  it('keeps name, message, stack and code of a thrown error', () => {
    const error = Object.assign(new TypeError('bad input'), { code: 'E_BAD' });

    expect(serializeError(error)).toMatchObject({
      name: 'TypeError',
      message: 'bad input',
      code: 'E_BAD',
      stack: expect.stringContaining('bad input'),
    });
  });

  // port: process-submission-outbox.service:3350
  it('follows the cause chain', () => {
    const root = Object.assign(new Error('deadlock detected'), { code: '40P01' });
    const wrapped = new Error('Failed query', { cause: root });

    expect(serializeError(wrapped)).toMatchObject({
      message: 'Failed query',
      cause: { message: 'deadlock detected', code: '40P01' },
    });
  });

  // port: process-submission-outbox.service:3360
  it('bounds a cyclic cause chain', () => {
    const a = new Error('a');
    const b = new Error('b', { cause: a });
    a.cause = b;

    let depth = 0;
    let node: { cause?: unknown } | undefined = serializeError(a);

    while (node) {
      depth++;
      node = node.cause as typeof node;
    }

    expect(depth).toBe(5);
  });

  // port: process-submission-outbox.service:3375
  it('stringifies non-Error throws', () => {
    expect(serializeError('plain string')).toEqual({ name: 'NonError', message: 'plain string' });
    expect(serializeError({ toString: () => 'object thrown' })).toEqual({
      name: 'NonError',
      message: 'object thrown',
    });
  });
});
