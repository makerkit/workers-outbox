import { resolve } from 'node:path';
import { defineConfig } from 'vitest/config';

const src = (path: string) => resolve(import.meta.dirname, '../../src', path);

export default defineConfig({
  resolve: {
    // Resolve the library to this repo's sources (like the wrangler alias in the e2e test),
    // so the example runs without a prior `pnpm build`.
    alias: [
      {
        find: /^@makerkit\/workers-outbox\/(.+)$/,
        replacement: src('$1/index.ts'),
      },
      { find: /^@makerkit\/workers-outbox$/, replacement: src('index.ts') },
    ],
  },
  test: {
    include: ['examples/worker/e2e/**/*.test.ts'],
    testTimeout: 60_000,
    hookTimeout: 120_000,
  },
});
