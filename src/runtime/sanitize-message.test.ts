import { describe, expect, it } from 'vitest';

import { MAX_PERSISTED_MESSAGE_LENGTH, sanitizeErrorMessage } from './sanitize-message';

const redactTokens = (message: string) => message.replace(/sk_test_[a-z0-9]+/g, '[redacted]');

describe('sanitizeErrorMessage', () => {
  // port: process-submission-outbox.service:3279
  it('redacts every match, not just the first', () => {
    expect(sanitizeErrorMessage('a sk_test_abc b sk_test_def', redactTokens)).toBe(
      'a [redacted] b [redacted]',
    );
  });

  // port: process-submission-outbox.service:3290
  it('redacts before truncating, so a secret straddling the length cap leaves no prefix behind', () => {
    const secret = 'sk_test_0123456789abcdef0123456789';
    const padding = 'x'.repeat(MAX_PERSISTED_MESSAGE_LENGTH - 10);
    const sanitized = sanitizeErrorMessage(`${padding} ${secret} tail`, redactTokens);

    expect(sanitized).not.toContain('sk_test');
    expect(sanitized.length).toBeLessThanOrEqual(MAX_PERSISTED_MESSAGE_LENGTH);
  });

  // port: process-submission-outbox.service:3299
  it('strips CR, LF and other control characters so a message cannot forge log lines', () => {
    const sanitized = sanitizeErrorMessage('boom\r\nlevel=error msg=forged\u0000\u001b[31m');

    expect(sanitized).toBe('boom level=error msg=forged [31m');
    // oxlint-disable-next-line no-control-regex -- asserting their absence
    expect(sanitized).not.toMatch(/[\u0000-\u001f\u007f]/);
  });

  // port: process-submission-outbox.service:3314
  it('flattens a multi-line message into one line, keeping every line', () => {
    expect(sanitizeErrorMessage('first\nsecond\n\nthird')).toBe('first second third');
  });

  it('truncates long messages to the cap with an ellipsis', () => {
    const sanitized = sanitizeErrorMessage('y'.repeat(MAX_PERSISTED_MESSAGE_LENGTH + 50));

    expect(sanitized).toHaveLength(MAX_PERSISTED_MESSAGE_LENGTH);
    expect(sanitized.endsWith('…')).toBe(true);
  });
});
