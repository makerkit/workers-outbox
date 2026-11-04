import { drizzle } from 'drizzle-orm/node-postgres';
// The example Worker under `wrangler dev`: workerd, nodejs_compat + node-postgres, local Queues
// and the cron trigger, against a real PostgreSQL and a stub mail API. Each test drives the
// Worker over HTTP (or the cron endpoint) and checks the database, the mail stub and the
// Worker's own event log. Hyperdrive is a local passthrough here, so its pooling isn't exercised.
// Needs wrangler (`WRANGLER=/path/to/wrangler` or on PATH) and Postgres (`OUTBOX_PG_URLS`, see
// `pnpm db:up`); skipped otherwise, unless OUTBOX_REQUIRE_E2E=1.
import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { createOutbox } from '../../../src/index';
import { createPgConnector } from '../../../src/pg/index';
import { OUTBOX_MIGRATIONS } from '../../../src/sql/migrations';
import { claimDue } from '../../../src/store/outbox-store';
import { welcomeEmail, type Mailer } from '../src/jobs';

const ROOT = resolve(import.meta.dirname, '../../..');
const WORKER_DIR = resolve(import.meta.dirname, '..');
const CONFIG = join(WORKER_DIR, '.wrangler.e2e.json');
const CRON = '*/5 * * * *';

const pgUrl = (process.env['OUTBOX_PG_URLS'] ?? '').split(',')[0]?.trim() ?? '';

function findWrangler() {
  const candidates = [
    process.env['WRANGLER'],
    join(ROOT, 'node_modules/.bin/wrangler'),
    'wrangler',
  ];

  return candidates.find(
    (bin) => bin && spawnSync(bin, ['--version'], { encoding: 'utf8' }).status === 0,
  );
}

const wrangler = pgUrl ? findWrangler() : undefined;

// CI sets this, so a missing wrangler or database fails the job instead of skipping it.
if (process.env['OUTBOX_REQUIRE_E2E'] === '1' && !wrangler) {
  throw new Error(
    `e2e required, but ${pgUrl ? 'wrangler was not found (set WRANGLER)' : 'OUTBOX_PG_URLS is empty'}`,
  );
}

interface MailRequest {
  idempotencyKey: string | undefined;
  to: string;
  subject: string;
}

type StubResponse = { status: number; headers?: Record<string, string> } | { drop: true };

/** A local stand-in for the mail API: records requests, answers from a per-recipient script. */
function mailStub() {
  const requests: MailRequest[] = [];
  const scripts = new Map<string, StubResponse[]>();

  const server: Server = createServer((req, res) => {
    let body = '';
    req.on('data', (chunk: Buffer) => (body += chunk.toString()));
    req.on('end', () => {
      const { to, subject } = JSON.parse(body) as { to: string; subject: string };
      requests.push({
        idempotencyKey: req.headers['idempotency-key'] as string | undefined,
        to,
        subject,
      });
      const next = scripts.get(to)?.shift() ?? { status: 200 };

      if ('drop' in next) {
        req.socket.destroy(); // the connection dies mid-request: fetch throws

        return;
      }

      res.writeHead(next.status, { 'content-type': 'application/json', ...next.headers });
      res.end(JSON.stringify({ id: `mail-${requests.length}` }));
    });
  });

  return {
    to: (email: string) => requests.filter((r) => r.to === email),
    script: (email: string, ...responses: StubResponse[]) => scripts.set(email, responses),
    listen: () =>
      new Promise<number>((done) => {
        server.listen(0, '127.0.0.1', () => {
          const address = server.address();
          done(typeof address === 'object' && address ? address.port : 0);
        });
      }),
    close: () =>
      new Promise<void>((done) => {
        server.closeAllConnections();
        server.close(() => done());
      }),
  };
}

/** wrangler.jsonc, with the library's imports aliased to this repo's sources. */
function writeE2eConfig() {
  const jsonc = readFileSync(join(WORKER_DIR, 'wrangler.jsonc'), 'utf8');
  const json = jsonc.replace(/^\s*\/\/.*$/gm, '').replace(/,(\s*[}\]])/g, '$1');
  const config = JSON.parse(json) as Record<string, unknown>;
  const lib = '@makerkit/workers-outbox';

  config['alias'] = {
    [lib]: join(ROOT, 'src/index.ts'),
    ...Object.fromEntries(
      ['cloudflare', 'pg', 'drizzle'].map((entry) => [
        `${lib}/${entry}`,
        join(ROOT, `src/${entry}/index.ts`),
      ]),
    ),
  };
  writeFileSync(CONFIG, JSON.stringify(config, null, 2));
}

const sleep = (ms: number) => new Promise((done) => setTimeout(done, ms));

async function eventually<T>(read: () => Promise<T>, ok: (value: T) => boolean, ms = 30_000) {
  const until = Date.now() + ms;
  let value = await read();

  while (!ok(value) && Date.now() < until) {
    await sleep(200);
    value = await read();
  }

  return value;
}

interface JobRow {
  id: string;
  status: string;
  last_error_code: string | null;
  terminal_reason: string | null;
  due_in_ms: number | null;
  attempts: { outcome: string | null; code: string | null; trigger: string }[];
}

describe.skipIf(!wrangler)('example Worker under wrangler dev', () => {
  const mail = mailStub();
  const dbName = `outbox_e2e_${Date.now()}`;
  let admin: Pool;
  let pool: Pool;
  let dbUrl: string;
  let worker: ChildProcess;
  let base: string;
  let persistDir: string;
  const output: string[] = [];

  beforeAll(async () => {
    admin = new Pool({ connectionString: pgUrl, max: 1 });
    await admin.query(`CREATE DATABASE "${dbName}"`);
    const url = new URL(pgUrl);
    url.pathname = `/${dbName}`;
    dbUrl = url.toString();
    pool = new Pool({ connectionString: dbUrl, max: 4 });

    for (const migration of OUTBOX_MIGRATIONS) {
      for (const statement of migration.statements) {
        await pool.query(statement);
      }
    }

    await pool.query(
      `CREATE TABLE users (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), email text NOT NULL UNIQUE)`,
    );

    const mailPort = await mail.listen();
    writeE2eConfig();
    persistDir = mkdtempSync(join(tmpdir(), 'outbox-e2e-'));
    const port = 18_000 + Math.floor(Math.random() * 2_000);
    base = `http://127.0.0.1:${port}`;

    worker = spawn(
      wrangler!,
      [
        'dev',
        '--config',
        CONFIG,
        '--ip',
        '127.0.0.1',
        '--port',
        String(port),
        '--test-scheduled',
        '--persist-to',
        persistDir,
        '--var',
        `MAILER_URL:http://127.0.0.1:${mailPort}`,
        '--var',
        'RESEND_API_KEY:test',
      ],
      {
        cwd: WORKER_DIR,
        detached: true,
        env: {
          ...process.env,
          CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_DB: dbUrl,
          CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_OUTBOX_DB: dbUrl,
          WRANGLER_SEND_METRICS: 'false',
          CI: '1',
        },
      },
    );
    worker.stdout?.on('data', (chunk: Buffer) => output.push(chunk.toString()));
    worker.stderr?.on('data', (chunk: Buffer) => output.push(chunk.toString()));

    const ready = await eventually(
      () =>
        fetch(`${base}/`)
          .then((res) => res.status)
          .catch(() => 0),
      (status) => status === 404,
      60_000,
    );

    if (ready !== 404) {
      throw new Error(`wrangler dev never came up:\n${output.join('')}`);
    }
  });

  afterAll(async () => {
    if (worker?.pid) {
      try {
        process.kill(-worker.pid, 'SIGTERM');
      } catch {
        // already gone
      }
    }

    await mail.close();
    await pool?.end();
    await sleep(500);
    await admin?.query(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`);
    await admin?.end();
    rmSync(CONFIG, { force: true });
    rmSync(persistDir, { recursive: true, force: true });
  });

  const signup = (email: string) =>
    fetch(`${base}/signup`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ email }),
    });

  const signedUp = async (email: string) => {
    const res = await signup(email);
    expect(res.status, fail(`signup ${email}`)).toBe(201);

    return `welcome:${((await res.json()) as { id: string }).id}`;
  };

  const cron = async () => {
    const res = await fetch(`${base}/__scheduled?cron=${encodeURIComponent(CRON)}`);
    expect(res.status, fail('scheduled')).toBe(200);
  };

  const job = async (dedupeKey: string): Promise<JobRow | undefined> => {
    const { rows } = await pool.query<JobRow>(
      `SELECT j.id, j.status, j.last_error_code, j.terminal_reason,
              (extract(epoch FROM (j.next_attempt_at - now())) * 1000)::int AS due_in_ms,
              COALESCE((SELECT json_agg(json_build_object('outcome', a.outcome, 'code', a.code,
                                                         'trigger', a.trigger) ORDER BY a.attempt_no)
                          FROM "outbox"."attempt" a WHERE a.job_id = j.id), '[]') AS attempts
         FROM "outbox"."job" j WHERE j.dedupe_key = $1`,
      [dedupeKey],
    );

    return rows[0];
  };

  const until = (dedupeKey: string, ...statuses: string[]) =>
    eventually(
      () => job(dedupeKey),
      (row) => row !== undefined && statuses.includes(row.status),
    );

  /** Pulls a retry's due time to now, as waiting out its backoff would. */
  const makeDue = (id: string) =>
    pool.query(
      `UPDATE "outbox"."job" SET next_attempt_at = now() WHERE id = $1 AND status = 'retrying'`,
      [id],
    );

  /** The Worker's structured events, as its `events` sink logged them. */
  const events = (jobId: string) =>
    output
      .join('')
      .split('\n')
      .flatMap((line) => {
        const start = line.indexOf('{"type":');

        if (start < 0) return [];

        try {
          return [
            JSON.parse(line.slice(start)) as { type: string; jobId?: string } & Record<
              string,
              unknown
            >,
          ];
        } catch {
          return [];
        }
      })
      .filter((event) => event.jobId === jobId);

  const fail = (what: string) => `${what}\n--- wrangler dev output ---\n${output.join('')}`;

  describe('enqueue', () => {
    it('signup commits the user and its job; the queue runs it once', async () => {
      const key = await signedUp('ada@example.com');
      const row = await until(key, 'succeeded', 'dead');

      expect(row?.status, fail('job did not succeed')).toBe('succeeded');
      expect(row?.attempts).toEqual([{ outcome: 'succeeded', code: null, trigger: 'nudge' }]);
      expect(mail.to('ada@example.com')).toHaveLength(1);
      expect(mail.to('ada@example.com')[0]).toMatchObject({ subject: 'Welcome!' });
      expect(mail.to('ada@example.com')[0]?.idempotencyKey).toBeTruthy();
    });

    it('the Worker logs the job’s lifecycle through its events sink', async () => {
      const key = await signedUp('events@example.com');
      const row = await until(key, 'succeeded');

      const seen = await eventually(
        async () => events(row!.id).map((e) => e.type),
        (types) => types.includes('transition'),
        10_000,
      );

      expect(seen).toEqual(expect.arrayContaining(['enqueued', 'transition']));
      expect(events(row!.id).find((e) => e.type === 'transition')).toMatchObject({
        to: 'succeeded',
        trigger: 'nudge',
        severity: 'info',
      });
    });

    it('an invalid email is rejected before any write', async () => {
      const res = await signup('not-an-email');

      expect(res.status).toBe(400);
      const { rows } = await pool.query(`SELECT 1 FROM users WHERE email = 'not-an-email'`);
      expect(rows).toHaveLength(0);
    });

    it('a duplicate signup rolls back: 409, still one user, one job, one email', async () => {
      const key = await signedUp('twice@example.com');
      await until(key, 'succeeded');

      const again = await signup('twice@example.com');
      expect(again.status).toBe(409);

      const { rows } = await pool.query<{ users: number; jobs: number }>(
        `SELECT (SELECT count(*)::int FROM users WHERE email = 'twice@example.com') AS users,
                (SELECT count(*)::int FROM "outbox"."job"
                  WHERE payload->>'email' = 'twice@example.com') AS jobs`,
      );
      expect(rows[0]).toEqual({ users: 1, jobs: 1 });
      await sleep(1_000);
      expect(mail.to('twice@example.com')).toHaveLength(1);
    });

    it('concurrent signups each get exactly one email', async () => {
      const emails = Array.from({ length: 20 }, (_, i) => `burst-${i}@example.com`);
      const keys = await Promise.all(emails.map((email) => signedUp(email)));

      const rows = await Promise.all(keys.map((key) => until(key, 'succeeded', 'dead')));

      expect(rows.map((r) => r?.status)).toEqual(emails.map(() => 'succeeded'));
      for (const email of emails) {
        expect(mail.to(email), email).toHaveLength(1);
      }
    });

    it('unknown routes are 404', async () => {
      expect((await fetch(`${base}/nope`)).status).toBe(404);
      expect((await fetch(`${base}/signup`)).status).toBe(404); // GET
    });
  });

  describe('handler outcomes', () => {
    it('429 with Retry-After: retries after the provider’s delay, with the same idempotency key', async () => {
      mail.script('grace@example.com', { status: 429, headers: { 'retry-after': '1' } });

      const key = await signedUp('grace@example.com');
      const row = await until(key, 'succeeded', 'dead');
      const sends = mail.to('grace@example.com');

      expect(row?.status, fail('job did not succeed after the retry')).toBe('succeeded');
      expect(row?.attempts).toEqual([
        { outcome: 'retry', code: 'rate_limited', trigger: 'nudge' },
        { outcome: 'succeeded', code: null, trigger: 'nudge' },
      ]);
      expect(sends).toHaveLength(2);
      expect(sends[0]?.idempotencyKey).toBe(sends[1]?.idempotencyKey);
    });

    it('5xx: backs off, isn’t re-sent before it is due, then the cron runs it', async () => {
      mail.script('flaky@example.com', { status: 503 });

      const key = await signedUp('flaky@example.com');
      const retrying = await until(key, 'retrying');

      expect(retrying?.last_error_code).toBe('http_503');
      // The policy's first backoff: equal jitter over a 30 s base, so 15–30 s out.
      expect(retrying?.due_in_ms).toBeGreaterThan(10_000);
      expect(retrying?.due_in_ms).toBeLessThanOrEqual(30_000);

      await cron(); // not due yet: the sweep leaves it alone
      expect((await job(key))?.status).toBe('retrying');
      expect(mail.to('flaky@example.com')).toHaveLength(1);

      await makeDue(retrying!.id);
      await cron();
      const done = await until(key, 'succeeded', 'dead');

      expect(done?.status, fail('the sweep did not retry the job')).toBe('succeeded');
      expect(done?.attempts.map((a) => a.trigger)).toEqual(['nudge', 'sweep']);
      const sends = mail.to('flaky@example.com');
      expect(sends).toHaveLength(2);
      expect(sends[0]?.idempotencyKey).toBe(sends[1]?.idempotencyKey);
    });

    it('4xx: dead-letters at once, with an error-severity event', async () => {
      mail.script('rejected@example.com', { status: 422 });

      const key = await signedUp('rejected@example.com');
      const row = await until(key, 'dead', 'succeeded');

      expect(row).toMatchObject({
        status: 'dead',
        terminal_reason: 'failed',
        last_error_code: 'http_422',
      });
      expect(row?.attempts).toEqual([{ outcome: 'failed', code: 'http_422', trigger: 'nudge' }]);

      await cron(); // dead is terminal: no sweep runs it again
      await sleep(500);
      expect(mail.to('rejected@example.com')).toHaveLength(1);

      const transition = await eventually(
        async () => events(row!.id).find((e) => e.type === 'transition'),
        (e) => e !== undefined,
        10_000,
      );
      expect(transition).toMatchObject({ to: 'dead', severity: 'error', code: 'http_422' });
    });

    it('a dropped connection to the provider throws; the default classify retries it', async () => {
      mail.script('dropped@example.com', { drop: true });

      const key = await signedUp('dropped@example.com');
      const row = await until(key, 'retrying', 'succeeded', 'dead');

      expect(row).toMatchObject({ status: 'retrying', last_error_code: 'unexpected' });
      expect(row?.attempts).toEqual([{ outcome: 'retry', code: 'unexpected', trigger: 'nudge' }]);

      await makeDue(row!.id);
      await cron();
      expect((await until(key, 'succeeded', 'dead'))?.status).toBe('succeeded');
    });
  });

  describe('cron', () => {
    /** An outbox outside the Worker with no queue: only the Worker's sweep can run its jobs. */
    const producer = (version?: { timestamp: number }) => {
      const noop: Mailer = { send: async () => ({ status: 200 }) };

      return createOutbox({
        jobs: [welcomeEmail],
        deps: { mailer: noop },
        connect: createPgConnector(dbUrl),
        ...(version ? { version } : {}),
      });
    };

    const enqueueDirectly = async (
      email: string,
      dedupeKey: string,
      version?: { timestamp: number },
    ) => {
      const connection = await createPgConnector(dbUrl)();

      try {
        const result = await producer(version).enqueue(connection.db, welcomeEmail, {
          payload: { email },
          dedupeKey,
        });
        expect(result.status).toBe('created');
      } finally {
        await connection.close();
      }
    };

    it('the sweep runs a job nobody nudged', async () => {
      await enqueueDirectly('cron@example.com', 'cron-only');
      expect((await job('cron-only'))?.status).toBe('pending');

      await cron();
      const row = await until('cron-only', 'succeeded', 'dead');

      expect(row?.status, fail('the sweep did not run the job')).toBe('succeeded');
      expect(row?.attempts).toEqual([{ outcome: 'succeeded', code: null, trigger: 'sweep' }]);
      expect(mail.to('cron@example.com')).toHaveLength(1);
    });

    it('recovers a job whose worker died mid-attempt, then runs it', async () => {
      await enqueueDirectly('crashed@example.com', 'crashed');
      // A worker claims it and dies: the lease is never settled. Age it past its end.
      await claimDue(drizzle({ client: pool }), {
        kinds: [{ kind: welcomeEmail.kind, leaseMs: welcomeEmail.leaseMs, fits: true }],
        limit: 1,
        trigger: 'sweep',
        leaseToken: '00000000-0000-4000-8000-00000000dead',
        now: null,
      });
      await pool.query(
        `UPDATE "outbox"."job" SET leased_until = now() - interval '1 second' WHERE dedupe_key = 'crashed'`,
      );

      await cron();
      const recovered = await until('crashed', 'retrying', 'succeeded');

      expect(recovered?.attempts[0]).toMatchObject({ outcome: 'lease_expired' });

      // A crash's first retry may be due at once, in which case the same tick already ran it.
      if (recovered?.status === 'retrying') {
        await makeDue(recovered.id);
        await cron();
      }

      const done = await until('crashed', 'succeeded', 'dead');
      expect(done?.status, fail('the recovered job did not run')).toBe('succeeded');
      expect(done?.attempts.map((a) => a.outcome)).toEqual(['lease_expired', 'succeeded']);
      // The dead worker never sent it; the recovered attempt sent it once.
      expect(mail.to('crashed@example.com')).toHaveLength(1);
    });

    it('an older deploy leaves a newer deploy’s job alone', async () => {
      // Enqueued by a deploy an hour newer than the running Worker.
      await enqueueDirectly('newer@example.com', 'newer-deploy', {
        timestamp: Date.now() + 3_600_000,
      });

      await cron();
      await sleep(1_000);

      expect((await job('newer-deploy'))?.status).toBe('pending');
      expect(mail.to('newer@example.com')).toHaveLength(0);
    });

    it('cleans up succeeded jobs past their retention, and keeps recent ones', async () => {
      const old = await signedUp('old@example.com');
      const recent = await signedUp('recent@example.com');
      await until(old, 'succeeded');
      await until(recent, 'succeeded');
      await pool.query(
        `UPDATE "outbox"."job"
            SET created_at = now() - interval '9 days', finished_at = now() - interval '8 days'
          WHERE dedupe_key = $1`,
        [old],
      );

      await cron();
      const gone = await eventually(
        () => job(old),
        (row) => row === undefined,
        10_000,
      );

      expect(gone, fail('cleanup did not delete the expired job')).toBeUndefined();
      expect((await job(recent))?.status).toBe('succeeded');
    });
  });
});
