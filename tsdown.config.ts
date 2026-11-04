import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    cloudflare: 'src/cloudflare/index.ts',
    inspect: 'src/inspect/index.ts',
    drizzle: 'src/drizzle/index.ts',
    sql: 'src/sql/index.ts',
    pg: 'src/pg/index.ts',
    testing: 'src/testing/index.ts',
  },
  format: 'esm',
  dts: true,
  clean: true,
  // Peers stay external so the app's single drizzle-orm / pglite / pg instance is used.
  external: [/^drizzle-orm/, /^@electric-sql\/pglite/, /^pg$/],
});
