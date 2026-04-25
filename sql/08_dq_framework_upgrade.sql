-- Upgrade the DQ framework in Postgres:
--   1. Add dimension / metric_type / threshold metadata to dq.rule.
--   2. Add dq.reconciliation for cross-database parity results.
--   3. Add partial unique indexes on silver.customer to guard survivorship.
--   4. Extend mart.dq_scorecard_daily with a reconciliation_failed column.

-- ============================================================================
-- 1. dq.rule upgrade
-- ============================================================================

do $$ begin
  create type dq.dimension as enum (
    'completeness',
    'uniqueness',
    'validity',
    'consistency',
    'timeliness',
    'referential'
  );
exception when duplicate_object then null; end $$;

do $$ begin
  create type dq.metric_type as enum ('violation_count', 'ratio', 'delta');
exception when duplicate_object then null; end $$;

do $$ begin
  create type dq.threshold_op as enum ('eq', 'ne', 'lt', 'lte', 'gt', 'gte');
exception when duplicate_object then null; end $$;

alter table dq.rule
  add column if not exists dimension dq.dimension,
  add column if not exists metric_type dq.metric_type not null default 'violation_count',
  add column if not exists threshold_op dq.threshold_op not null default 'lte',
  add column if not exists threshold_value numeric not null default 0,
  add column if not exists scope jsonb not null default '{}'::jsonb,
  add column if not exists owner text;

-- dq.result carries the measured metric explicitly.
alter table dq.result
  add column if not exists metric_value numeric,
  add column if not exists threshold_op dq.threshold_op,
  add column if not exists threshold_value numeric;

-- ============================================================================
-- 2. dq.reconciliation
-- ============================================================================

create table if not exists dq.reconciliation (
  reconciliation_id  bigserial primary key,
  dq_run_id          uuid references dq.run(dq_run_id),
  name               text not null,
  left_source        text not null,
  right_source       text not null,
  metric_type        text not null,            -- row_count | fingerprint | hash
  left_metric        numeric,
  right_metric       numeric,
  left_fingerprint   text,
  right_fingerprint  text,
  discrepancy        numeric,
  tolerance          numeric,
  passed             boolean not null,
  window_start       timestamptz,
  window_end         timestamptz,
  details            jsonb,
  created_at         timestamptz not null default now()
);

create index if not exists idx_reconciliation_name_created_at
  on dq.reconciliation (name, created_at desc);

-- ============================================================================
-- 3. Survivorship guards on silver.customer
-- ============================================================================

create unique index if not exists ux_customer_emirates_id
  on silver.customer (emirates_id)
  where emirates_id is not null;

create unique index if not exists ux_customer_internal_uuid
  on silver.customer (internal_uuid)
  where internal_uuid is not null;

create unique index if not exists ux_customer_phone_email
  on silver.customer (phone_e164, email_norm)
  where phone_e164 is not null and email_norm is not null;

-- A sanity constraint on customer_link.confidence in addition to DQ.
do $$ begin
  alter table silver.customer_link
    add constraint ck_customer_link_confidence
    check (confidence >= 0 and confidence <= 1) not valid;
exception when duplicate_object then null; end $$;

-- Validate off-peak:
--   alter table silver.customer_link validate constraint ck_customer_link_confidence;

-- ============================================================================
-- 4. Mart extension for reconciliation rollup
-- ============================================================================

alter table mart.dq_scorecard_daily
  add column if not exists reconciliation_failed bigint not null default 0;
