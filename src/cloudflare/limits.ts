/** Cloudflare's wall-time limit for one queue-consumer or scheduled invocation. */
export const WORKER_INVOCATION_LIMIT_MS = 900_000;

/** Headroom kept under the platform kill for acks, the last settle's tail and close. */
const PLATFORM_HEADROOM_MS = 30_000;

/** The most an invocation budget may claim of the platform window. */
export const MAX_INVOCATION_BUDGET_MS = WORKER_INVOCATION_LIMIT_MS - PLATFORM_HEADROOM_MS;

/** Default budget shared by every phase of one queue batch or cron tick. */
export const DEFAULT_INVOCATION_BUDGET_MS = 840_000;
