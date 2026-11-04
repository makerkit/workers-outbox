import { pgTable, text, uuid } from 'drizzle-orm/pg-core';

// drizzle-kit generates the outbox migration from these re-exports, alongside your own tables.
export { outboxAttempt, outboxJob, outboxSchema } from '@makerkit/workers-outbox/drizzle';

export const users = pgTable('users', {
  id: uuid('id').primaryKey().defaultRandom(),
  email: text('email').notNull().unique(),
});
