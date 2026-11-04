export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
  code?: string;
  cause?: SerializedError;
}

const MAX_CAUSE_DEPTH = 5;

// Not redacted: events go to operators; only the persisted copy is sanitized.
export function serializeError(error: unknown, depth = 0): SerializedError {
  if (!(error instanceof Error)) {
    return { name: 'NonError', message: safeString(error) };
  }

  const serialized: SerializedError = { name: error.name, message: error.message };

  if (error.stack !== undefined) {
    serialized.stack = error.stack;
  }

  const code = (error as { code?: unknown }).code;

  if (typeof code === 'string') {
    serialized.code = code;
  }

  if (error.cause !== undefined && depth + 1 < MAX_CAUSE_DEPTH) {
    serialized.cause = serializeError(error.cause, depth + 1);
  }

  return serialized;
}

function safeString(value: unknown) {
  try {
    return String(value);
  } catch {
    return '[unprintable]';
  }
}
