// Matched on codes only, never on message text (except node-pg's code-less termination).
// Class 53 (insufficient resources) is retryable as a whole.
const TRANSIENT_SQLSTATES = new Set(['40001', '40003', '40P01', '55P03', '57014']);

// 25P03/25P04: idle-in-transaction / transaction_timeout terminate the session (FATAL).
const CONNECTION_SQLSTATES = new Set(['57P01', '57P02', '57P03', '25P03', '25P04']);

// A timeout aborts the statement, but a client can't tell whether an in-flight COMMIT landed.
const TIMEOUT_SQLSTATES = new Set(['57014', '55P03', '25P03', '25P04']);

const SOCKET_CODES = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'EPIPE',
  'ETIMEDOUT',
  'EHOSTUNREACH',
  'ENOTCONN',
  'ENETUNREACH',
  'ECONNABORTED',
  'EAI_AGAIN',
  // postgres.js
  'CONNECTION_CLOSED',
  'CONNECTION_ENDED',
  'CONNECTION_DESTROYED',
  'CONNECT_TIMEOUT',
]);

const NODE_PG_TERMINATED = /^Connection terminated/;

// node-pg refuses a query on a client whose socket already died (e.g. while a handler ran):
// a bare Error, no code. The query never left the client, so a fresh lane can retry it.
const NODE_PG_NOT_QUERYABLE =
  /^Client (?:has encountered a connection error|was closed) and is not queryable$/;

const MAX_NODES = 16;

type ErrorNode = { code?: unknown; message?: unknown };

/** Searches the `cause` chain and `AggregateError.errors`: Drizzle's wrapper has no `code`. */
function findInChain<T>(error: unknown, pick: (node: ErrorNode) => T | null) {
  const seen = new Set<unknown>();
  const queue: unknown[] = [error];

  while (queue.length > 0 && seen.size < MAX_NODES) {
    const node = queue.shift();

    if (!node || typeof node !== 'object' || seen.has(node)) {
      continue;
    }

    seen.add(node);
    const picked = pick(node as ErrorNode);

    if (picked !== null) {
      return picked;
    }

    queue.push((node as { cause?: unknown }).cause);

    const nested = (node as { errors?: unknown }).errors;

    if (Array.isArray(nested)) {
      queue.push(...nested);
    }
  }

  return null;
}

export function sqlstateOf(error: unknown) {
  return findInChain(error, (node) =>
    typeof node.code === 'string' && /^[0-9A-Z]{5}$/.test(node.code) ? node.code : null,
  );
}

/**
 * The connection dropped mid-statement: whether it committed is unknown, so a retry
 * must use a fresh connection and read back what landed.
 */
export function isConnectionError(error: unknown) {
  const hit = findInChain(error, (node) => {
    const code = typeof node.code === 'string' ? node.code : '';
    const message = typeof node.message === 'string' ? node.message : '';
    const terminated = NODE_PG_TERMINATED.test(message) || NODE_PG_NOT_QUERYABLE.test(message);

    return code.startsWith('08') ||
      CONNECTION_SQLSTATES.has(code) ||
      SOCKET_CODES.has(code) ||
      terminated
      ? true
      : null;
  });

  return hit === true;
}

export function isTimeoutError(error: unknown) {
  const code = sqlstateOf(error);

  return code !== null && TIMEOUT_SQLSTATES.has(code);
}

export function isTransientDbError(error: unknown) {
  const code = sqlstateOf(error);

  const transientCode = code !== null && (TRANSIENT_SQLSTATES.has(code) || code.startsWith('53'));

  return transientCode || isConnectionError(error);
}
