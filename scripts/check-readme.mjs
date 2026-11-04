#!/usr/bin/env node
// Typechecks every ```ts block in README.md against src/, so the examples can't drift from the
// API. Imports from relative paths (the app's own modules) are shown to readers but resolved
// from PRELUDE. Each `## ` section is one module: its blocks run in order and may build on each other, and
// a later section may redefine a name (the README grows its examples). The app code the examples
// assume (a mailer, `env`, `db`, …) comes from PRELUDE, minus whatever the section defines itself.
// Errors are reported at their README line.
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';

const OUT = '.readme-check';

// module → type-only names; merged with a section's own imports, so neither duplicates the other.
const PRELUDE_IMPORTS = {
  'drizzle-orm/node-postgres': ['NodePgDatabase'],
  '@makerkit/workers-outbox': [
    'JobDefinition',
    'Outbox',
    'OutboxDb',
    'OutboxOptions',
    'QueueSender',
  ],
  zod: ['z'],
};

// name → declaration; a section that defines `name` itself doesn't get it.
const PRELUDE = {
  Mailer: `interface Mailer {
  send(
    userId: string,
    options: { signal: AbortSignal; idempotencyKey: string },
  ): Promise<{ rateLimited: boolean; retryAfterMs?: number }>;
}`,
  Env: `interface Env {
  OUTBOX_DB: { connectionString: string };
  OUTBOX_QUEUE: QueueSender;
  OUTBOX_BULK_QUEUE: QueueSender;
  CF_VERSION_METADATA: { timestamp: string };
}`,
  ExecutionContext: 'interface ExecutionContext { waitUntil(promise: Promise<unknown>): void }',
  isHttp4xx: 'declare function isHttp4xx(error: unknown): boolean;',
  sendWelcomeEmail:
    'declare function sendWelcomeEmail(userId: string, options: { signal: AbortSignal; idempotencyKey: string }): Promise<void>;',
  createMailer: 'declare function createMailer(env: Env): Mailer;',
  fakeMailer: 'declare function fakeMailer(sent: string[]): Mailer;',
  flakyMailer: 'declare function flakyMailer(options: { failures: number }): Mailer;',
  flagUser: 'declare function flagUser(tx: OutboxDb, jobId: string): Promise<void>;',
  markWelcomed: 'declare function markWelcomed(tx: OutboxDb, userId: string): Promise<void>;',
  isoWeek: 'declare function isoWeek(date: Date): string;',
  sendNewsletter:
    'declare function sendNewsletter(week: string, options: { signal: AbortSignal; idempotencyKey: string }): Promise<void>;',
  dbFor: 'declare function dbFor(env: Env): NodePgDatabase;',
  listShops: 'declare function listShops(tx: OutboxDb): Promise<{ id: string }[]>;',
  syncInsights:
    'declare function syncInsights(shopId: string, day: string, options: { signal: AbortSignal }): Promise<void>;',
  buildExport:
    'declare function buildExport(orgId: string, options: { signal: AbortSignal }): Promise<void>;',
  insertUser:
    'declare function insertUser(tx: unknown, input: unknown): Promise<{ id: string; orgId: string }>;',
  env: 'declare const env: Env;',
  ctx: 'declare const ctx: ExecutionContext;',
  db: 'declare const db: NodePgDatabase;',
  app: 'declare const app: { fetch(request: Request): Promise<Response> };',
  input: 'declare const input: unknown;',
  requestId: 'declare const requestId: string;',
  user: 'declare const user: { id: string; orgId: string };',
  jobId: 'declare const jobId: string;',
  jobIds: 'declare const jobIds: string[];',
  welcomeEmail:
    "declare const welcomeEmail: JobDefinition<'email.welcome', z.ZodObject<{ userId: z.ZodString }>, { mailer: Mailer }>;",
  outboxFor: 'declare const outboxFor: (env: Env) => Outbox<[typeof welcomeEmail]>;',
  outbox: 'declare const outbox: Outbox<[typeof welcomeEmail]>;',
  options: 'declare const options: OutboxOptions<[typeof welcomeEmail]>;',
};

const readme = readFileSync('README.md', 'utf8').split('\n');
const sections = []; // { title, blocks: { start, lines }[] }
let section = { title: '(top)', blocks: [] };

for (let i = 0; i < readme.length; i++) {
  if (readme[i].startsWith('## ')) {
    sections.push(section);
    section = { title: readme[i].slice(3), blocks: [] };
    continue;
  }

  if (readme[i].trim() !== '```ts') continue;

  const start = i + 1;
  let end = start;

  while (readme[end].trim() !== '```') end++;

  section.blocks.push({ start, lines: readme.slice(start, end) });
  i = end;
}

sections.push(section);

const IMPORT = /^import\s+(type\s+)?\{([^}]*)\}\s*from\s*'([^']+)';?/;
const DEFINES =
  /^(?:export\s+)?(?:declare\s+)?(?:async\s+)?(?:const|let|function|interface|type|class)\s+(\w+)/gm;

rmSync(OUT, { recursive: true, force: true });
mkdirSync(OUT, { recursive: true });

const files = [];
let blockCount = 0;

for (const [index, { title, blocks }] of sections.entries()) {
  if (blocks.length === 0) continue;

  blockCount += blocks.length;
  const imports = new Map(); // module → Set of specifiers
  const body = []; // { text, readmeLine }
  const appImports = []; // { name, line }: from relative paths, resolved from PRELUDE

  for (const block of blocks) {
    let offset = 0;

    for (const chunk of block.lines.join('\n').split(/(?=^import\s)/m)) {
      const match = IMPORT.exec(chunk);
      let rest = chunk;

      if (match && match[3].startsWith('.')) {
        // App code (`./jobs`, `./mailer`, …): the README shows the import, PRELUDE declares it.
        const [whole, , names] = match;

        for (const name of names
          .split(',')
          .map((n) => n.trim().replace(/^type\s+/, ''))
          .filter(Boolean)) {
          appImports.push({ name, line: block.start + 1 + offset });
        }

        rest = '\n'.repeat(whole.split('\n').length - 1) + chunk.slice(whole.length);
      } else if (match) {
        const [whole, typeOnly, names, from] = match;
        const set = imports.get(from) ?? new Set();

        for (const name of names
          .split(',')
          .map((n) => n.trim())
          .filter(Boolean)) {
          set.add(typeOnly && !name.startsWith('type ') ? `type ${name}` : name);
        }

        imports.set(from, set);
        // Keep line numbers aligned: an import's lines become blank lines.
        rest = '\n'.repeat(whole.split('\n').length - 1) + chunk.slice(whole.length);
      } else if (/^import\s/.test(chunk)) {
        throw new Error(`README.md:${block.start + 1 + offset}: unsupported import form`);
      }

      rest.split('\n').forEach((line, k) => {
        body.push({ text: line, readmeLine: block.start + 1 + offset + k });
      });
      offset += chunk.split('\n').length - 1;
    }
  }

  const text = body.map((b) => b.text).join('\n');
  const defined = new Set([...text.matchAll(DEFINES)].map((m) => m[1]));

  for (const { name, line } of appImports) {
    if (!(name in PRELUDE) && !defined.has(name)) {
      throw new Error(`README.md:${line}: '${name}' is imported from app code but not in PRELUDE`);
    }
  }

  for (const names of imports.values()) {
    for (const name of names)
      defined.add(
        name
          .replace(/^type\s+/, '')
          .split(/\s+as\s+/)
          .pop(),
      );
  }

  const bare = (name) =>
    name
      .replace(/^type\s+/, '')
      .split(/\s+as\s+/)
      .pop();

  for (const [from, names] of Object.entries(PRELUDE_IMPORTS)) {
    const set = imports.get(from) ?? new Set();
    const have = new Set([...set].map(bare));

    for (const name of names) if (!have.has(name)) set.add(`type ${name}`);

    imports.set(from, set);
  }

  const header = [
    `// README.md § ${title}`,
    ...[...imports].map(([from, names]) => `import { ${[...names].join(', ')} } from '${from}';`),
    ...Object.entries(PRELUDE)
      .filter(([name]) => !defined.has(name))
      .flatMap(([, decl]) => decl.split('\n')),
    'export {};',
  ];
  const file = `section-${index}.ts`;

  writeFileSync(`${OUT}/${file}`, [...header, ...body.map((b) => b.text)].join('\n'));
  files.push({ file, map: (line) => body[line - header.length - 1]?.readmeLine });
}

writeFileSync(
  `${OUT}/tsconfig.json`,
  JSON.stringify({ extends: '../tsconfig.json', include: ['*.ts'] }, null, 2),
);

try {
  execFileSync('tsc', ['-p', `${OUT}/tsconfig.json`, '--noEmit', '--pretty', 'false'], {
    encoding: 'utf8',
    stdio: 'pipe',
  });
  console.log(`readme: ${blockCount} ts blocks in ${files.length} sections typecheck`);
} catch (error) {
  const out = `${error.stdout ?? ''}${error.stderr ?? ''}`;
  console.error(
    out.replace(/\.readme-check\/(section-\d+\.ts)\((\d+),(\d+)\)/g, (whole, file, line, col) => {
      const mapped = files.find((f) => f.file === file)?.map(Number(line));

      return mapped ? `README.md:${mapped}:${col}` : whole;
    }),
  );
  process.exit(1);
}
