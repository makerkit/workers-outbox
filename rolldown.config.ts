import { readFileSync, rmSync } from 'node:fs';
import { builtinModules } from 'node:module';
import { defineConfig } from 'rolldown';
import { dts } from 'rolldown-plugin-dts';

const pkg = JSON.parse(readFileSync(new URL('./package.json', import.meta.url), 'utf8')) as {
  dependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
};

// Runtime deps and peers stay external so the app's single drizzle-orm / pglite / pg
// instance is used and deps are resolved by the consumer's package manager.
const externalPackages = [
  ...Object.keys(pkg.dependencies ?? {}),
  ...Object.keys(pkg.peerDependencies ?? {}),
];

const isExternal = (id: string) =>
  id.startsWith('node:') ||
  builtinModules.includes(id) ||
  externalPackages.some((name) => id === name || id.startsWith(`${name}/`));

rmSync(new URL('./dist', import.meta.url), { recursive: true, force: true });

export default defineConfig({
  input: {
    index: 'src/index.ts',
    cloudflare: 'src/cloudflare/index.ts',
    inspect: 'src/inspect/index.ts',
    drizzle: 'src/drizzle/index.ts',
    sql: 'src/sql/index.ts',
    pg: 'src/pg/index.ts',
    testing: 'src/testing/index.ts',
  },
  platform: 'node',
  external: isExternal,
  plugins: [dts()],
  output: {
    dir: 'dist',
    format: 'esm',
    entryFileNames: '[name].mjs',
    chunkFileNames: '[name]-[hash].mjs',
  },
});
