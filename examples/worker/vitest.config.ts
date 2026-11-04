import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['examples/worker/e2e/**/*.test.ts'],
    testTimeout: 60_000,
    hookTimeout: 120_000,
  },
});
