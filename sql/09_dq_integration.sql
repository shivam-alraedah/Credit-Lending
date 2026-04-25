-- DQ integration: rule-to-target map, publication marker (cross-consumer
-- "latest green" pointer), anomaly log, and dq_run_id stamping on the
-- tables where it belongs.
--
-- This file assumes 01..08 have been applied.

-- ============================================================================
-- 1. Rule -> target registry
-- ============================================================================

create table if not exists dq.rule_target (
  rule_id       text not null references dq.rule(rule_id),
  schema_name   text not null,
  table_name    text not null,
  column_name   text,
  kind          text not null,            -- 'column' | 'composite_key' | 'table' | 'cross_table'
  primary key (rule_id, schema_name, table_name, coalesce(column_name, ''))
);

create index if not exists idx_rule_target_table
  on dq.rule_target (schema_name, table_name);

-- ============================================================================
-- 2. Publication marker (one row per layer)
-- ============================================================================

create table if not exists dq.publication_marker (
  layer             text primary key,      -- bronze | silver | gold | mart | cross_db
  last_green_run_id uuid not null references dq.run(dq_run_id),
  last_green_at     timestamptz not null,
  updated_by        text,
  notes             text
);

create or replace view dq.v_latest_green as
  select layer, last_green_run_id, last_green_at
  from dq.publication_marker;

-- ============================================================================
-- 3. Anomaly log
-- ============================================================================

do $$ begin
  create type dq.anomaly_severity as enum ('info', 'warn', 'page');
exception when duplicate_object then null; end $$;

create table if not exists dq.anomaly (
  anomaly_id       bigserial primary key,
  dq_run_id        uuid references dq.run(dq_run_id),
  detector_id      text not null,
  method           text not null,          -- z_score | mad | schema_drift | enum_drift | freshness
  severity         dq.anomaly_severity not null default 'info',
  metric_name      text not null,
  observed_value   numeric,
  baseline_value   numeric,
  deviation        numeric,
  z_score          numeric,
  window_start     timestamptz,
  window_end       timestamptz,
  details          jsonb,
  created_at       timestamptz not null default now()
);

create index if not exists idx_anomaly_detector_created
  on dq.anomaly (detector_id, created_at desc);

alter table mart.dq_scorecard_daily
  add column if not exists anomaly_count bigint not null default 0;

-- ============================================================================
-- 4. Stamp dq_run_id on the tables whose per-row lineage matters
-- ============================================================================

alter table gold.decision
  add column if not exists dq_run_id uuid references dq.run(dq_run_id);
create index if not exists idx_gold_decision_dq_run
  on gold.decision (dq_run_id);

alter table gold.decision_input_snapshot
  add column if not exists dq_run_id uuid references dq.run(dq_run_id);
create index if not exists idx_gold_snapshot_dq_run
  on gold.decision_input_snapshot (dq_run_id);

alter table mart.risk_portfolio_daily
  add column if not exists dq_run_id uuid references dq.run(dq_run_id);
create index if not exists idx_mart_rpd_dq_run
  on mart.risk_portfolio_daily (dq_run_id);

alter table mart.customer_risk_features_latest
  add column if not exists dq_run_id uuid references dq.run(dq_run_id);

-- ============================================================================
-- 5. Consumer-safe views (only expose rows tied to the current green run)
-- ============================================================================

create or replace view mart.v_publishable_risk_portfolio_daily as
  with g as (select last_green_run_id from dq.publication_marker where layer = 'mart')
  select m.*
  from mart.risk_portfolio_daily m, g
  where m.dq_run_id is not distinct from g.last_green_run_id;

create or replace view gold.v_publishable_decisions as
  with g as (select last_green_run_id from dq.publication_marker where layer = 'gold')
  select d.*
  from gold.decision d, g
  where d.dq_run_id is not distinct from g.last_green_run_id;

-- ============================================================================
-- 6. Example: DB-level enforcement for a multi-column invariant (trigger)
-- ============================================================================
-- Ensures silver.identity_observation always has at least one identifier.

create or replace function silver.enforce_observation_has_identifier()
returns trigger language plpgsql as $$
begin
  if new.emirates_id is null
     and new.internal_uuid is null
     and new.phone_e164 is null
     and new.full_name_norm is null
  then
    raise exception 'silver.identity_observation has no identifiers (observation_id=%)', new.observation_id;
  end if;
  return new;
end $$;

drop trigger if exists trg_enforce_observation_valid on silver.identity_observation;
create trigger trg_enforce_observation_valid
before insert or update on silver.identity_observation
for each row execute function silver.enforce_observation_has_identifier();

-- ============================================================================
-- 7. Example: generated column for deterministic name normalization
-- ============================================================================
-- If the application layer sometimes forgets to normalize a name, this
-- generated column guarantees we always have `full_name_norm` on the row.
-- Kept optional to avoid forcing consumer changes; commented for review.
--
-- alter table silver.customer_profile
--   add column if not exists full_name_norm_generated text generated always as (
--     regexp_replace(lower(coalesce(full_name,'')), '\\s+', ' ', 'g')
--   ) stored;
