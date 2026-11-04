import type { OutboxMessage } from '../runtime/types';

const MAX_REJECTED_BODY = 500;

export type DecodedMessage =
  | { ok: true; message: OutboxMessage }
  | { ok: false; reason: 'malformed' | 'unknown_version'; body: string };

// Unreadable bodies are rejected and acked: retrying them only burns attempts into the DLQ.
export function decodeMessage(body: unknown): DecodedMessage {
  if (typeof body !== 'object' || body === null) {
    return reject('malformed', body);
  }

  const candidate = body as Record<string, unknown>;

  if (candidate['v'] !== 1) {
    return reject('unknown_version', body);
  }

  if (candidate['type'] === 'nudge') {
    const ids = candidate['ids'];

    if (Array.isArray(ids) && ids.length > 0 && ids.every((id) => typeof id === 'string')) {
      return { ok: true, message: { v: 1, type: 'nudge', ids: ids as string[] } };
    }

    return reject('malformed', body);
  }

  if (candidate['type'] === 'drain' && typeof candidate['lane'] === 'string') {
    return { ok: true, message: { v: 1, type: 'drain', lane: candidate['lane'] } };
  }

  return reject('malformed', body);
}

function reject(reason: 'malformed' | 'unknown_version', body: unknown): DecodedMessage {
  let text: string;

  try {
    text = JSON.stringify(body) ?? String(body);
  } catch {
    text = String(body);
  }

  return { ok: false, reason, body: text.slice(0, MAX_REJECTED_BODY) };
}
