import { defineJobFor, fail, ok, retry } from '@makerkit/workers-outbox';
import { z } from 'zod';

export interface Mailer {
  send(
    message: { to: string; subject: string; text: string },
    options: { signal: AbortSignal; idempotencyKey: string },
  ): Promise<{ status: number; retryAfterMs?: number }>;
}

/** What every handler gets as `ctx.deps`; `createOutbox` refuses to compile without it. */
export interface Deps {
  mailer: Mailer;
}

const defineJob = defineJobFor<Deps>();

export const welcomeEmail = defineJob({
  kind: 'email.welcome',
  payload: z.object({ email: z.email() }),
  timeoutMs: 30_000,
  retry: { maxAttempts: 10, backoff: { baseMs: 30_000, maxMs: 3_600_000 } },
  async handle({ email }, ctx) {
    const res = await ctx.deps.mailer.send(
      { to: email, subject: 'Welcome!', text: 'Thanks for signing up.' },
      // Delivery is at-least-once: the provider dedupes on the key, and the signal fires at timeoutMs.
      { signal: ctx.signal, idempotencyKey: ctx.idempotencyKey },
    );

    if (res.status === 429) {
      return retry({ code: 'rate_limited', after: res.retryAfterMs });
    }

    if (res.status >= 500) {
      return retry({ code: `http_${res.status}` });
    }

    if (res.status >= 400) {
      return fail({ code: `http_${res.status}` });
    }

    return ok();
  },
});

/** A fetch-based mailer (Resend's API shape). `baseUrl` points it at a stub in tests. */
export function createMailer(apiKey: string, baseUrl = 'https://api.resend.com'): Mailer {
  return {
    async send(message, { signal, idempotencyKey }) {
      const res = await fetch(`${baseUrl}/emails`, {
        method: 'POST',
        signal,
        headers: {
          authorization: `Bearer ${apiKey}`,
          'content-type': 'application/json',
          'idempotency-key': idempotencyKey,
        },
        body: JSON.stringify({ from: 'hello@example.com', ...message }),
      });
      const retryAfter = Number(res.headers.get('retry-after'));

      return {
        status: res.status,
        ...(Number.isFinite(retryAfter) && retryAfter > 0
          ? { retryAfterMs: retryAfter * 1_000 }
          : {}),
      };
    },
  };
}
