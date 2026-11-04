export const MAX_PERSISTED_MESSAGE_LENGTH = 2_000;

// oxlint-disable-next-line no-control-regex -- stripping control characters is the point
const CONTROL_CHARACTERS = /[\u0000-\u001f\u007f]+/g;

export type MessageRedactor = (message: string) => string;

// Redact before truncating, so a secret can't survive by being cut in half.
export function sanitizeErrorMessage(message: string, redact?: MessageRedactor) {
  const redacted = redact ? redact(message) : message;
  const flattened = redacted.replace(CONTROL_CHARACTERS, ' ').replace(/ {2,}/g, ' ').trim();

  if (flattened.length <= MAX_PERSISTED_MESSAGE_LENGTH) {
    return flattened;
  }

  return `${flattened.slice(0, MAX_PERSISTED_MESSAGE_LENGTH - 1)}…`;
}
