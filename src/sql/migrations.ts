import {
  ATTEMPT_OUTCOMES,
  ATTEMPT_TRIGGERS,
  CLAIMABLE_STATUSES,
  JOB_STATUSES,
  REASONED_TERMINAL_STATUSES,
  TERMINAL_STATUSES,
} from '../core/status';
import { sqlLiteralList } from './sql-list';

/** Append-only: never edit a shipped migration. */
export interface OutboxMigration {
  readonly id: string;
  readonly statements: readonly string[];
}

const jobStatuses = sqlLiteralList(JOB_STATUSES);
const claimable = sqlLiteralList(CLAIMABLE_STATUSES);
const terminal = sqlLiteralList(TERMINAL_STATUSES);
const reasonedTerminal = sqlLiteralList(REASONED_TERMINAL_STATUSES);

/** PostgreSQL 17: settlement relies on `transaction_timeout`. */
export const MIN_SERVER_VERSION_NUM = 170_000;

/** Fails setup on an older server, before any object is created. */
export function serverVersionGuard(minimum: number) {
  const major = Math.floor(minimum / 10_000);

  return `DO $guard$
BEGIN
  IF current_setting('server_version_num')::int < ${minimum} THEN
    RAISE EXCEPTION 'workers-outbox requires PostgreSQL ${major} or newer; this server is %', current_setting('server_version')
      USING ERRCODE = 'feature_not_supported';
  END IF;
END
$guard$`;
}

const init: OutboxMigration = {
  id: '0001_init',
  statements: [
    serverVersionGuard(MIN_SERVER_VERSION_NUM),
    `CREATE SCHEMA "outbox"`,
    `CREATE TABLE "outbox"."job" (
  "id" uuid PRIMARY KEY,
  "kind" text NOT NULL,
  "dedupe_key" text NOT NULL,
  "payload" jsonb NOT NULL,
  "status" text DEFAULT 'pending' NOT NULL,
  "tenant_key" text,
  "correlation_id" text,
  "metadata" jsonb DEFAULT '{}' NOT NULL,
  "available_at" timestamp(3) with time zone DEFAULT now() NOT NULL,
  "next_attempt_at" timestamp(3) with time zone,
  "lease_token" uuid,
  "leased_until" timestamp(3) with time zone,
  "started_attempts" integer DEFAULT 0 NOT NULL,
  "finished_attempts" integer DEFAULT 0 NOT NULL,
  "spent_attempts" integer DEFAULT 0 NOT NULL,
  "code_streak" integer DEFAULT 0 NOT NULL,
  "sticky_code" text,
  "min_version_ms" bigint,
  "last_error_code" text,
  "last_error_message" text,
  "terminal_reason" text,
  "created_at" timestamp(3) with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp(3) with time zone DEFAULT now() NOT NULL,
  "finished_at" timestamp(3) with time zone,
  "nudge_sent_at" timestamp(3) with time zone,
  "nudge_attempts" integer DEFAULT 0 NOT NULL,
  "last_nudge_error" text,
  "ordering_key" text,
  "seq" bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  CONSTRAINT "job_status_check" CHECK ("status" IN (${jobStatuses})),
  CONSTRAINT "job_retrying_requires_next_attempt_at" CHECK ("status" <> 'retrying' OR "next_attempt_at" IS NOT NULL),
  CONSTRAINT "job_processing_iff_leased" CHECK (("status" = 'processing') = ("lease_token" IS NOT NULL AND "leased_until" IS NOT NULL)),
  CONSTRAINT "job_attempt_counters" CHECK ("finished_attempts" >= 0 AND "finished_attempts" <= "started_attempts" AND "spent_attempts" >= 0 AND "spent_attempts" <= "finished_attempts" AND "code_streak" >= 0),
  CONSTRAINT "job_terminal_iff_finished_at" CHECK (("status" IN (${terminal})) = ("finished_at" IS NOT NULL)),
  CONSTRAINT "job_terminal_reason" CHECK (("status" IN (${reasonedTerminal})) = ("terminal_reason" IS NOT NULL))
)`,
    `CREATE TABLE "outbox"."attempt" (
  "job_id" uuid NOT NULL,
  "attempt_no" integer NOT NULL,
  "kind" text NOT NULL,
  "lease_token" uuid NOT NULL,
  "trigger" text NOT NULL,
  "started_at" timestamp(3) with time zone NOT NULL,
  "finished_at" timestamp(3) with time zone,
  "outcome" text,
  "code" text,
  "message" text,
  "duration_ms" integer,
  CONSTRAINT "attempt_pkey" PRIMARY KEY ("job_id", "attempt_no"),
  CONSTRAINT "attempt_job_id_fkey" FOREIGN KEY ("job_id") REFERENCES "outbox"."job" ("id") ON DELETE CASCADE,
  CONSTRAINT "attempt_trigger_check" CHECK ("trigger" IN (${sqlLiteralList(ATTEMPT_TRIGGERS)})),
  CONSTRAINT "attempt_outcome_check" CHECK ("outcome" IS NULL OR "outcome" IN (${sqlLiteralList(ATTEMPT_OUTCOMES)})),
  CONSTRAINT "attempt_finished_iff_outcome" CHECK (("outcome" IS NULL) = ("finished_at" IS NULL))
)`,
    `CREATE UNIQUE INDEX "job_tenant_dedupe_key_unique" ON "outbox"."job" (COALESCE("tenant_key", ''), "dedupe_key")`,
    `CREATE INDEX "job_claim_due_idx" ON "outbox"."job" (COALESCE("next_attempt_at", "available_at"), "created_at", "id") WHERE "status" IN (${claimable})`,
    `CREATE INDEX "job_pending_kind_idx" ON "outbox"."job" ("kind", "available_at", "created_at") WHERE "status" = 'pending'`,
    `CREATE INDEX "job_retrying_kind_idx" ON "outbox"."job" ("kind", "next_attempt_at", "created_at") WHERE "status" = 'retrying'`,
    `CREATE INDEX "job_lease_expiry_idx" ON "outbox"."job" ("leased_until") WHERE "status" = 'processing'`,
    `CREATE INDEX "job_retention_idx" ON "outbox"."job" ("status", "finished_at") WHERE "status" IN (${terminal})`,
    `CREATE INDEX "job_tenant_key_idx" ON "outbox"."job" ("tenant_key") WHERE "tenant_key" IS NOT NULL`,
    `CREATE INDEX "job_correlation_id_idx" ON "outbox"."job" ("correlation_id") WHERE "correlation_id" IS NOT NULL`,
    `CREATE INDEX "job_status_created_idx" ON "outbox"."job" ("status", "created_at", "id")`,
    `CREATE INDEX "attempt_finished_at_idx" ON "outbox"."attempt" ("finished_at") WHERE "finished_at" IS NOT NULL`,
  ],
};

export const OUTBOX_MIGRATIONS: readonly OutboxMigration[] = [init];
