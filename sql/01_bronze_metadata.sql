-- Bronze control-plane tables (Postgres) for auditability and replay.
-- Raw payloads live in S3 Bronze; Postgres stores run/object metadata + hashes.

create schema if not exists audit;

create type audit.run_status as enum ('STARTED', 'SUCCEEDED', 'FAILED', 'PARTIAL');

create table if not exists audit.ingestion_run (
  run_id           uuid primary key,
  source           text not null,           -- aecb|fraud|aml|internal_profile|...
  started_at       timestamptz not null default now(),
  ended_at         timestamptz,
  status           audit.run_status not null default 'STARTED',
  trigger_type     text,                    -- schedule|event|manual|decisioning_service
  parser_version   text,                    -- git sha / build id for parsing code
  notes            text
);

create table if not exists audit.ingestion_object (
  object_id        bigserial primary key,
  run_id           uuid not null references audit.ingestion_run(run_id),
  s3_bucket        text not null,
  s3_key           text not null,
  content_type     text,
  bytes            bigint,
  record_count     bigint,
  sha256_hex       text,                    -- hash of raw object bytes
  etag             text,
  s3_version_id    text,
  created_at       timestamptz not null default now(),
  unique (s3_bucket, s3_key, coalesce(s3_version_id, ''))
);

create table if not exists audit.ingestion_error (
  error_id         bigserial primary key,
  run_id           uuid references audit.ingestion_run(run_id),
  stage            text not null,           -- ingest|parse|load|dq|identity|snapshot|mart
  error_type       text not null,
  message          text not null,
  details          jsonb,
  created_at       timestamptz not null default now()
);

-- Generic hash log for decision-time writes (fraud write-through etc.)
create table if not exists audit.payload_hash_log (
  payload_id       bigserial primary key,
  source           text not null,           -- fraud_req|fraud_res|aml_payload|aecb_file|...
  external_id      text,                    -- vendor id / webhook event id / etc.
  decision_id      text,                    -- if associated with a decision
  s3_bucket        text,
  s3_key           text,
  sha256_hex       text not null,
  created_at       timestamptz not null default now()
);

