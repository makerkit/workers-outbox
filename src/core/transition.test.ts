import { describe, expect, it } from 'vitest';

import { cancel, fail, ok, retry, type SettleOutcome } from './outcome';
import { resolveRetryPolicy } from './retry-policy';
import { decideTransition, type JobBudgetState } from './transition';

const policy = resolveRetryPolicy({
  maxAttempts: 5,
  backoff: { baseMs: 1_000, maxMs: 60_000, factor: 2 },
  perCode: { timeout: 2 },
  maxCrashes: 3,
});
const timing = { graceMs: 5_000 };

const job = (overrides: Partial<JobBudgetState> = {}): JobBudgetState => ({
  id: 'job-1',
  spentAttempts: 0,
  codeStreak: 0,
  lastErrorCode: null,
  ...overrides,
});

const decide = (outcome: SettleOutcome, state: Partial<JobBudgetState> = {}) =>
  decideTransition({ outcome, job: job(state), policy, timing });

describe('decideTransition', () => {
  it('succeeds on ok', () => {
    expect(decide(ok())).toEqual({ to: 'succeeded', attemptOutcome: 'succeeded' });
  });

  it('cancels with the handler reason', () => {
    expect(decide(cancel({ reason: 'user_deleted' }))).toEqual({
      to: 'cancelled',
      reason: 'user_deleted',
      attemptOutcome: 'cancelled',
    });
  });

  it('dies immediately on fail, whatever budget is left', () => {
    expect(decide(fail({ code: 'http_404', message: 'gone' }))).toEqual({
      to: 'dead',
      reason: 'failed',
      code: 'http_404',
      message: 'gone',
      codeStreak: 1,
      attemptOutcome: 'failed',
    });
  });

  it('retries with backoff while budget remains', () => {
    const t = decide(retry({ code: 'http_503' }), { spentAttempts: 1 });

    expect(t).toMatchObject({
      to: 'retrying',
      code: 'http_503',
      codeStreak: 1,
      attemptOutcome: 'retry',
    });
    expect(t.to === 'retrying' && t.delayMs).toBeGreaterThanOrEqual(1_000);
  });

  // port: process-submission-outbox.service:752
  it('honours retry({ after }) but clamps it to a day', () => {
    expect(decide(retry({ code: 'rate_limited', after: 42_000 }))).toMatchObject({
      delayMs: 42_000,
    });
    expect(decide(retry({ code: 'rate_limited', after: 1e12 }))).toMatchObject({
      delayMs: 86_400_000,
    });
    expect(decide(retry({ code: 'rate_limited', after: -5 }))).toMatchObject({ delayMs: 0 });
  });

  // port: process-submission-outbox.service:2051
  it('is exhausted when the attempt budget is spent', () => {
    expect(decide(retry({ code: 'http_503' }), { spentAttempts: 4 })).toMatchObject({
      to: 'dead',
      reason: 'exhausted',
      code: 'http_503',
    });
  });

  it('never exhausts a job with maxAttempts: Infinity', () => {
    const forever = resolveRetryPolicy({ maxAttempts: Number.POSITIVE_INFINITY });

    expect(
      decideTransition({
        outcome: retry({ code: 'http_503' }),
        job: job({ spentAttempts: 1_000_000 }),
        policy: forever,
        timing,
      }),
    ).toMatchObject({ to: 'retrying' });
  });

  // port: process-submission-outbox.service:787
  it('falls back to the backoff when retry({ after }) is not a finite number', () => {
    const t = decide(retry({ code: 'rate_limited', after: Number.NaN }));

    expect(t).toMatchObject({ to: 'retrying' });
    expect(t.to === 'retrying' && t.delayMs).toBeGreaterThanOrEqual(500);
    expect(t.to === 'retrying' && t.delayMs).toBeLessThanOrEqual(1_000);
  });

  // port: submission-job.repository.db:666
  it('enforces per-code caps on consecutive failures only', () => {
    expect(decide({ type: 'timeout' }, { lastErrorCode: 'timeout', codeStreak: 1 })).toMatchObject({
      to: 'dead',
      reason: 'exhausted',
      code: 'timeout',
      codeStreak: 2,
    });
    expect(decide({ type: 'timeout' }, { lastErrorCode: 'http_503', codeStreak: 4 })).toMatchObject(
      {
        to: 'retrying',
        codeStreak: 1,
      },
    );
  });

  it('never retries a timeout sooner than the grace period', () => {
    const tight = resolveRetryPolicy({ backoff: { baseMs: 1, maxMs: 1 } });

    expect(
      decideTransition({ outcome: { type: 'timeout' }, job: job(), policy: tight, timing }),
    ).toMatchObject({ to: 'retrying', delayMs: 5_000, attemptOutcome: 'timeout' });
  });

  // port: stuck-job-recovery:196
  it('declares a crash loop after maxCrashes consecutive dead workers', () => {
    expect(
      decide({ type: 'lease_expired' }, { lastErrorCode: 'lease_expired', codeStreak: 1 }),
    ).toMatchObject({ to: 'retrying', attemptOutcome: 'lease_expired', codeStreak: 2 });

    expect(
      decide({ type: 'lease_expired' }, { lastErrorCode: 'lease_expired', codeStreak: 2 }),
    ).toMatchObject({ to: 'dead', reason: 'crash_loop', codeStreak: 3 });
  });

  // port: stuck-job-recovery:109
  it('retries a crashed attempt at once without charging the attempt budget', () => {
    expect(
      decide(
        { type: 'lease_expired' },
        { spentAttempts: 4, lastErrorCode: 'http_503', codeStreak: 3 },
      ),
    ).toMatchObject({ to: 'retrying', delayMs: 0, codeStreak: 1, attemptOutcome: 'lease_expired' });
  });

  it('spends the budget on hook failures like any retry, so they cannot loop forever', () => {
    expect(decide(retry({ code: 'hook_failed' }), { spentAttempts: 4 })).toMatchObject({
      to: 'dead',
      reason: 'exhausted',
      code: 'hook_failed',
    });
  });

  describe('code streaks', () => {
    // port: submission-job.repository.db:703
    it('counts the first failure of a code as a streak of 1', () => {
      expect(decide(retry({ code: 'http_503' }))).toMatchObject({ codeStreak: 1 });
      expect(
        decide(fail({ code: 'http_404' }), { lastErrorCode: 'http_503', codeStreak: 3 }),
      ).toMatchObject({
        codeStreak: 1,
      });
    });

    // port: submission-job.repository.db:684
    it('lets a crash between two timeouts break the timeout streak', () => {
      const crashed = decide(
        { type: 'lease_expired' },
        { lastErrorCode: 'timeout', codeStreak: 1 },
      );

      expect(crashed).toMatchObject({ code: 'lease_expired', codeStreak: 1 });
      expect(
        decide({ type: 'timeout' }, { lastErrorCode: 'lease_expired', codeStreak: 1 }),
      ).toMatchObject({ to: 'retrying', codeStreak: 1 });
    });

    // port: process-submission-outbox.service:1685
    // port: process-submission-outbox.service:1734
    // port: process-submission-outbox.service:2164
    // port: process-submission-outbox.service:2208
    it('decides from the claimed row alone, so no extra read can fail or skew the streak', () => {
      const state = Object.freeze(
        job({ spentAttempts: 3, lastErrorCode: 'http_503', codeStreak: 3 }),
      );
      const input = { outcome: retry({ code: 'rate_limited' }), job: state, policy, timing };

      expect(decideTransition(input)).toMatchObject({ to: 'retrying', codeStreak: 1 });
      expect(decideTransition(input)).toEqual(decideTransition(input));
    });
  });

  describe('per-code streak caps', () => {
    const wide = resolveRetryPolicy({ maxAttempts: 100, perCode: { flaky: 20 } });
    const decideWide = (outcome: SettleOutcome, state: Partial<JobBudgetState>) =>
      decideTransition({ outcome, job: job(state), policy: wide, timing });

    // port: process-submission-outbox.service:1265
    it('judges the cap on the consecutive streak, not on how many attempts the job made', () => {
      expect(
        decideWide(retry({ code: 'flaky' }), {
          spentAttempts: 25,
          lastErrorCode: 'other',
          codeStreak: 25,
        }),
      ).toMatchObject({ to: 'retrying', codeStreak: 1 });
    });

    // port: process-submission-outbox.service:1314
    // port: process-submission-outbox.service:1838
    it('is exhausted when the streak reaches the cap', () => {
      expect(
        decideWide(retry({ code: 'flaky' }), {
          spentAttempts: 19,
          lastErrorCode: 'flaky',
          codeStreak: 19,
        }),
      ).toMatchObject({ to: 'dead', reason: 'exhausted', code: 'flaky', codeStreak: 20 });
    });

    // port: process-submission-outbox.service:1350
    it('is exhausted at the cap even deep into the attempt budget', () => {
      expect(
        decideWide(retry({ code: 'flaky' }), {
          spentAttempts: 80,
          lastErrorCode: 'flaky',
          codeStreak: 19,
        }),
      ).toMatchObject({ to: 'dead', reason: 'exhausted', codeStreak: 20 });
    });

    // port: process-submission-outbox.service:1389
    it('keeps retrying below the cap even when attempts exceed it', () => {
      expect(
        decideWide(retry({ code: 'flaky' }), {
          spentAttempts: 60,
          lastErrorCode: 'flaky',
          codeStreak: 5,
        }),
      ).toMatchObject({ to: 'retrying', codeStreak: 6 });
    });

    // port: process-submission-outbox.service:3064
    it('dead-letters repeated timeouts at the timeout cap, below maxAttempts', () => {
      expect(
        decide({ type: 'timeout' }, { spentAttempts: 1, lastErrorCode: 'timeout', codeStreak: 1 }),
      ).toMatchObject({
        to: 'dead',
        reason: 'exhausted',
        code: 'timeout',
        attemptOutcome: 'timeout',
      });
    });

    // port: process-submission-outbox.service:3097
    it('retries a timeout below its cap, never sooner than the grace period', () => {
      const t = decide({ type: 'timeout' }, { lastErrorCode: 'http_503', codeStreak: 1 });

      expect(t).toMatchObject({ to: 'retrying', code: 'timeout', codeStreak: 1 });
      expect(t.to === 'retrying' && t.delayMs).toBeGreaterThanOrEqual(timing.graceMs);
    });
  });

  describe('attempt ceiling', () => {
    // port: process-submission-outbox.service:1177
    it('is exhausted at the default ceiling of 20 handled attempts', () => {
      const defaults = resolveRetryPolicy();
      const at = (spentAttempts: number) =>
        decideTransition({
          outcome: retry({ code: 'http_503' }),
          job: job({ spentAttempts }),
          policy: defaults,
          timing,
        });

      expect(at(18)).toMatchObject({ to: 'retrying' });
      expect(at(19)).toMatchObject({ to: 'dead', reason: 'exhausted' });
    });

    // port: process-submission-outbox.service:2084
    it('binds even when alternating codes keep resetting the streak', () => {
      const alternating = ['a', 'b', 'a', 'b', 'a'];
      let state = job();
      let last = decide(ok());

      for (const code of alternating) {
        last = decideTransition({ outcome: retry({ code }), job: state, policy, timing });

        if (last.to !== 'retrying') {
          break;
        }

        state = {
          ...state,
          spentAttempts: state.spentAttempts + 1,
          lastErrorCode: code,
          codeStreak: last.codeStreak,
        };
      }

      expect(state.spentAttempts).toBe(4);
      expect(last).toMatchObject({ to: 'dead', reason: 'exhausted', codeStreak: 1 });
    });

    // port: process-submission-outbox.service:2341
    it('is global: a fresh code past maxAttempts is exhausted, not a fresh streak', () => {
      expect(
        decide(retry({ code: 'brand_new' }), {
          spentAttempts: 99,
          lastErrorCode: 'old',
          codeStreak: 3,
        }),
      ).toMatchObject({ to: 'dead', reason: 'exhausted', code: 'brand_new', codeStreak: 1 });
    });
  });

  describe('crashes', () => {
    // port: stuck-job-recovery:156
    it('counts only consecutive crashes: a handled failure in between resets the crash streak', () => {
      expect(
        decide(
          { type: 'lease_expired' },
          { spentAttempts: 4, lastErrorCode: 'http_503', codeStreak: 2 },
        ),
      ).toMatchObject({ to: 'retrying', delayMs: 0, codeStreak: 1 });
    });

    const patient = resolveRetryPolicy({ maxCrashes: 10 });
    const crash = (codeStreak: number) =>
      decideTransition({
        outcome: { type: 'lease_expired' },
        job: job({ lastErrorCode: 'lease_expired', codeStreak }),
        policy: patient,
        timing,
      });

    // port: stuck-job-recovery:349
    it("honors a job's own maxCrashes: below it, a crash re-arms", () => {
      expect(crash(8)).toMatchObject({ to: 'retrying', codeStreak: 9 });
    });

    // port: stuck-job-recovery:381
    it("dead-letters at the job's own maxCrashes as a crash loop", () => {
      expect(crash(9)).toMatchObject({
        to: 'dead',
        reason: 'crash_loop',
        code: 'lease_expired',
        codeStreak: 10,
      });
    });
  });

  describe('per-code rules', () => {
    // port: process-submission-outbox.service:2018
    it('lets one code carry a budget above maxAttempts', () => {
      const p = resolveRetryPolicy({
        maxAttempts: 5,
        perCode: { rate_limited: { maxAttempts: 50 } },
      });
      const decideP = (code: string, spentAttempts: number) =>
        decideTransition({
          outcome: retry({ code }),
          job: job({ spentAttempts }),
          policy: p,
          timing,
        });

      expect(decideP('rate_limited', 30)).toMatchObject({ to: 'retrying' });
      expect(decideP('rate_limited', 49)).toMatchObject({ to: 'dead', reason: 'exhausted' });
      expect(decideP('http_503', 30)).toMatchObject({ to: 'dead', reason: 'exhausted' });
    });

    // port: process-submission-outbox.service:913
    it('lets one code retry forever', () => {
      const p = resolveRetryPolicy({
        perCode: { paused: { maxAttempts: Number.POSITIVE_INFINITY } },
      });

      expect(
        decideTransition({
          outcome: retry({ code: 'paused' }),
          job: job({ spentAttempts: 1_000_000 }),
          policy: p,
          timing,
        }),
      ).toMatchObject({ to: 'retrying' });
    });

    describe('sticky episodes', () => {
      const p = resolveRetryPolicy({
        maxAttempts: 5,
        perCode: { awaiting_provider: { maxAttempts: 40, sticky: true } },
      });
      const decideP = (code: string, state: Partial<JobBudgetState>) =>
        decideTransition({ outcome: retry({ code }), job: job(state), policy: p, timing });

      // port: process-submission-outbox.service:2127
      it('widens the budget for every later code once the sticky code was seen', () => {
        expect(decideP('awaiting_provider', { spentAttempts: 2 })).toMatchObject({
          to: 'retrying',
          stickyCode: 'awaiting_provider',
        });
        expect(
          decideP('http_503', { spentAttempts: 20, stickyCode: 'awaiting_provider' }),
        ).toMatchObject({
          to: 'retrying',
          stickyCode: 'awaiting_provider',
        });
        expect(decideP('http_503', { spentAttempts: 20 })).toMatchObject({ to: 'dead' });
      });

      it('never narrows the global budget: a code or episode budget below maxAttempts is ignored', () => {
        const narrow = resolveRetryPolicy({
          maxAttempts: 10,
          perCode: { awaiting_provider: { maxAttempts: 3, sticky: true } },
        });

        expect(
          decideTransition({
            outcome: retry({ code: 'awaiting_provider' }),
            job: job({ spentAttempts: 5, stickyCode: 'awaiting_provider', lastErrorCode: 'x' }),
            policy: narrow,
            timing,
          }),
        ).toMatchObject({ to: 'retrying' });
      });

      // port: process-submission-outbox.service:2300
      it('counts the episode budget across code changes, then exhausts', () => {
        expect(
          decideP('http_503', { spentAttempts: 39, stickyCode: 'awaiting_provider' }),
        ).toMatchObject({
          to: 'dead',
          reason: 'exhausted',
          stickyCode: 'awaiting_provider',
        });
      });

      it('keeps the episode through a crash', () => {
        expect(
          decideTransition({
            outcome: { type: 'lease_expired' },
            job: job({ stickyCode: 'awaiting_provider' }),
            policy: p,
            timing,
          }),
        ).toMatchObject({ to: 'retrying', stickyCode: 'awaiting_provider' });
      });

      it('only ever widens: a code with a smaller budget of its own still gets the episode budget', () => {
        const mixed = resolveRetryPolicy({
          maxAttempts: 5,
          perCode: {
            awaiting_provider: { maxAttempts: 40, sticky: true },
            quick: { maxAttempts: 2 },
          },
        });

        expect(
          decideTransition({
            outcome: retry({ code: 'quick' }),
            job: job({ spentAttempts: 10, stickyCode: 'awaiting_provider' }),
            policy: mixed,
            timing,
          }),
        ).toMatchObject({ to: 'retrying' });
      });

      it('omits stickyCode outside an episode, so the store keeps whatever the row records', () => {
        expect(decideP('http_503', {})).not.toHaveProperty('stickyCode');
      });
    });

    describe('delay ceiling', () => {
      const p = resolveRetryPolicy({
        backoff: { baseMs: 60_000, maxMs: 3_600_000 },
        perCode: { token_replay: { maxDelayMs: 900_000 }, timeout: { maxDelayMs: 1_000 } },
      });
      const delayOf = (outcome: SettleOutcome, spentAttempts: number) => {
        const t = decideTransition({ outcome, job: job({ spentAttempts }), policy: p, timing });

        return t.to === 'retrying' ? t.delayMs : Number.NaN;
      };

      // port: token-replay-window.dispatcher:108
      it('caps a saturated backoff at the code ceiling', () => {
        expect(delayOf(retry({ code: 'token_replay' }), 9)).toBe(900_000);
        expect(delayOf(retry({ code: 'other' }), 9)).toBeGreaterThan(900_000);
      });

      // port: token-replay-window.dispatcher:122
      it('is a ceiling, not a fixed delay: a shorter backoff stands', () => {
        const first = delayOf(retry({ code: 'token_replay' }), 0);

        expect(first).toBeGreaterThanOrEqual(30_000);
        expect(first).toBeLessThanOrEqual(60_000);
      });

      it('never undercuts the timeout grace floor', () => {
        expect(delayOf({ type: 'timeout' }, 0)).toBe(timing.graceMs);
      });

      it('keeps the default timeout streak cap when timeout is configured as an object', () => {
        expect(p.perCode['timeout']).toBe(5);
      });
    });

    describe("onExhaustion: 'cancel'", () => {
      const p = resolveRetryPolicy({
        maxAttempts: 3,
        perCode: {
          deferred: { streak: 10, onExhaustion: 'cancel' },
          capped: { streak: 2, onExhaustion: 'cancel' },
        },
      });
      const decideP = (code: string, state: Partial<JobBudgetState>) =>
        decideTransition({
          outcome: retry({ code, message: 'still waiting' }),
          job: job(state),
          policy: p,
          timing,
        });

      // port: process-submission-outbox.service:2557
      it('cancels instead of dead-lettering when the attempt ceiling is reached, keeping the error', () => {
        expect(decideP('deferred', { spentAttempts: 2 })).toEqual({
          to: 'cancelled',
          reason: 'exhausted',
          code: 'deferred',
          message: 'still waiting',
          codeStreak: 1,
          attemptOutcome: 'retry',
        });
      });

      it('cancels when the streak cap is reached too', () => {
        expect(decideP('capped', { lastErrorCode: 'capped', codeStreak: 1 })).toMatchObject({
          to: 'cancelled',
          reason: 'exhausted',
          codeStreak: 2,
        });
      });

      // port: process-submission-outbox.service:2791
      it('always reaches a terminal state on exhaustion, whichever option the code carries', () => {
        expect(decideP('deferred', { spentAttempts: 2 }).to).toBe('cancelled');
        expect(decideP('plain', { spentAttempts: 2 }).to).toBe('dead');
      });
    });
  });
});
