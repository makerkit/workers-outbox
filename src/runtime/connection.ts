import type { Clock, LaneConnection } from './types';

import { settledWithin, unwrap, untilDeadline } from './deadline';

// Destroying may wait on a runaway query; past this the session is abandoned (unusable either way).
const DESTROY_WAIT_MS = 5_000;

// A healthy close() that never returns is abandoned, and the session destroyed instead.
const CLOSE_WAIT_MS = 2_000;

/**
 * `connect()` bounded by `deadlineAt`: throws `DeadlineExceededError` past it, and destroys the
 * connection if it arrives later anyway.
 */
export async function acquireConnection(
  connect: () => Promise<LaneConnection>,
  clock: Clock,
  deadlineAt: number,
) {
  const connecting = connect();
  const got = await untilDeadline(clock, connecting, deadlineAt);

  if (!got.done) {
    void connecting.then(discardConnection, () => undefined);
  }

  return unwrap(got, 'connection acquisition');
}

/** Destroys a session nobody will wait on. */
export function discardConnection(connection: LaneConnection) {
  void connection.destroy().catch(() => undefined);
}

/** Destroys a session, waiting a bounded time for it. */
export const destroyConnection = (connection: LaneConnection) =>
  settledWithin(connection.destroy(), DESTROY_WAIT_MS);

/** Returns a healthy session; one whose `close()` hangs is destroyed instead. */
export async function closeConnection(connection: LaneConnection) {
  const closed = await settledWithin(connection.close(), CLOSE_WAIT_MS);

  if (!closed.done) {
    discardConnection(connection);
  }
}
