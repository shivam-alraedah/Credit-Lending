-- Postgres native RANGE partitioning for the hottest tables.
--
-- NOTE: This file is a *drop-in replacement* for the non-partitioned definitions
-- in 01_bronze_metadata.sql and 03_gold_dq_marts.sql. In a real migration we'd
-- use a dual-write + swap strategy (or pg_partman's "_template" + MOVE) — here
-- we show the target shape the migration aims for.
--
-- Requirements:
--   - pg_partman (installed in its own schema), or a light custom maintenance job.
--
-- Conventions:
--   - One partition per calendar month, named _pYYYYMM.
--   - Pre-create 3 months in advance; retain 84 months (7 years) to satisfy
--     UAE Central Bank audit retention before archival to S3.

create extension if not exists pg_partman;
create schema if not exists partman_ctl;

-- ============================================================================
-- audit.ingestion_object (BIGGEST writer): partition by created_at
-- ============================================================================

drop table if exists audit.ingestion_object_partitioned cascade;
create table if not exists audit.ingestion_object_partitioned (
  object_id        bigserial,
  run_id           uuid not null references audit.ingestion_run(run_id),
  s3_bucket        text not null,
  s3_key           text not null,
  content_type     text,
  bytes            bigint,
  record_count     bigint,
  sha256_hex       text,
  etag             text,
  s3_version_id    text,
  created_at       timestamptz not null default now(),
  primary key (object_id, created_at),
  unique (s3_bucket, s3_key, coalesce(s3_version_id, ''), created_at)
) partition by range (created_at);

select partman.create_parent(
  p_parent_table  => 'audit.ingestion_object_partitioned',
  p_control       => 'created_at',
  p_type          => 'range',
  p_interval      => '1 month',
  p_premake       => 3
);

update partman.part_config
   set retention = '84 months',
       retention_keep_table = true,  -- we archive before dropping
       retention_keep_index = false,
       infinite_time_partitions = true
 where parent_table = 'audit.ingestion_object_partitioned';

create index if not exists idx_iop_sha on audit.ingestion_object_partitioned (sha256_hex);
create index if not exists idx_iop_run on audit.ingestion_object_partitioned (run_id);


-- ============================================================================
-- gold.decision: partition by requested_at
-- ============================================================================

drop table if exists gold.decision_partitioned cascade;
create table if not exists gold.decision_partitioned (
  decision_id          text not null,
  product_type         text not null,
  requested_amount     numeric,
  currency             text default 'AED',
  requested_at         timestamptz not null default now(),
  decisioned_at        timestamptz,
  decision_status      text not null,
  decision_policy      text,
  customer_key         uuid,
  product_id           text,
  sharia_contract      gold.sharia_contract_type,
  profit_rate_bps      integer,
  disclosures_version  text,
  policy_code          gold.decision_policy_code,
  created_at           timestamptz not null default now(),
  primary key (decision_id, requested_at)  -- partition key must be in PK
) partition by range (requested_at);

select partman.create_parent(
  p_parent_table  => 'gold.decision_partitioned',
  p_control       => 'requested_at',
  p_type          => 'range',
  p_interval      => '1 month',
  p_premake       => 3
);

update partman.part_config
   set retention = '84 months',
       retention_keep_table = true,
       infinite_time_partitions = true
 where parent_table = 'gold.decision_partitioned';

create index if not exists idx_dec_customer on gold.decision_partitioned (customer_key);
create index if not exists idx_dec_product_ts on gold.decision_partitioned (product_type, requested_at desc);


-- ============================================================================
-- gold.decision_input_snapshot: partition by snapshot_ts
-- ============================================================================

drop table if exists gold.decision_input_snapshot_partitioned cascade;
create table if not exists gold.decision_input_snapshot_partitioned (
  snapshot_id            uuid not null,
  decision_id            text not null,
  snapshot_ts            timestamptz not null default now(),
  customer_key           uuid,
  identity_summary       jsonb not null,
  inputs                 jsonb not null,
  dq_summary             jsonb not null,
  model_rule_version     text,
  supersedes_snapshot_id uuid,
  sha256_hex             text not null,
  primary key (snapshot_id, snapshot_ts),
  unique (decision_id, sha256_hex, snapshot_ts)
) partition by range (snapshot_ts);

select partman.create_parent(
  p_parent_table  => 'gold.decision_input_snapshot_partitioned',
  p_control       => 'snapshot_ts',
  p_type          => 'range',
  p_interval      => '1 month',
  p_premake       => 3
);

update partman.part_config
   set retention = '84 months',
       retention_keep_table = true,
       infinite_time_partitions = true
 where parent_table = 'gold.decision_input_snapshot_partitioned';

create index if not exists idx_snap_decision on gold.decision_input_snapshot_partitioned (decision_id);
create index if not exists idx_snap_customer on gold.decision_input_snapshot_partitioned (customer_key);


-- ============================================================================
-- Maintenance job: call this nightly via MWAA.
-- ============================================================================
-- select partman.run_maintenance();
--
-- For a read-only replica, stop autovacuum aggressive settings on older
-- partitions:
--   alter table gold.decision_partitioned_p202501 set (autovacuum_enabled = false);

-- ============================================================================
-- Retention & archival workflow (sketch):
--   1) Nightly, pg_partman detaches partitions older than 84 months (kept as tables).
--   2) A separate MWAA task pg_dump's the detached partition to S3 (Gold-archive
--      bucket with Object Lock) and verifies row counts + sha256 of the dump.
--   3) Only after verification do we DROP the detached table in Postgres.
-- ============================================================================
