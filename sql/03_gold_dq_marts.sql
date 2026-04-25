-- Gold layer: immutable decision snapshots + DQ scorecard + portfolio marts.

create schema if not exists gold;
create schema if not exists dq;
create schema if not exists mart;

-- Decision record
create table if not exists gold.decision (
  decision_id          text primary key,
  product_type         text not null,      -- personal_finance|bnpl|card_alt
  requested_amount     numeric,
  currency             text default 'AED',
  requested_at         timestamptz not null default now(),
  decisioned_at        timestamptz,
  decision_status      text not null,      -- approved|declined|manual_review|blocked
  decision_policy      text,               -- full_inputs|required_bureau|degraded_no_bureau|...
  customer_key         uuid,               -- resolved at decision time (may be null initially)
  created_at           timestamptz not null default now()
);

-- Immutable input snapshot (append-only)
create table if not exists gold.decision_input_snapshot (
  snapshot_id          uuid primary key,
  decision_id          text not null references gold.decision(decision_id),
  snapshot_ts          timestamptz not null default now(),
  customer_key         uuid,
  identity_summary     jsonb not null,     -- identifiers used + match rule summary
  inputs              jsonb not null,      -- conformed input values used by rules/model
  dq_summary          jsonb not null,      -- dq pass/fail + key metrics at decision time
  model_rule_version   text,               -- ruleset version / model version
  supersedes_snapshot_id uuid references gold.decision_input_snapshot(snapshot_id),
  sha256_hex           text not null,      -- hash of canonical JSON of the snapshot payload
  unique (decision_id, sha256_hex)
);

-- Pointers to raw artifacts in Bronze (S3) + hashes
create table if not exists gold.decision_input_artifact (
  artifact_id          bigserial primary key,
  snapshot_id          uuid not null references gold.decision_input_snapshot(snapshot_id),
  source               text not null,      -- aecb|fraud_req|fraud_res|aml|internal_profile
  s3_bucket            text not null,
  s3_key               text not null,
  sha256_hex           text not null,
  created_at           timestamptz not null default now()
);

-- ============ Data Quality ============
create type dq.severity as enum ('MUST_PASS', 'WARN');

create table if not exists dq.rule (
  rule_id              text primary key,
  description          text not null,
  severity             dq.severity not null,
  layer                text not null,      -- bronze|silver|gold|mart|decision
  sql_check            text not null,      -- SQL returning rows that violate the rule
  alert_threshold      numeric,            -- optional: percent/ratio threshold
  enabled              boolean not null default true
);

create table if not exists dq.run (
  dq_run_id            uuid primary key,
  executed_at          timestamptz not null default now(),
  scope                jsonb,              -- e.g., {source:"aecb", batch_id:"..."}
  status               text not null        -- succeeded|failed
);

create table if not exists dq.result (
  dq_result_id         bigserial primary key,
  dq_run_id            uuid not null references dq.run(dq_run_id),
  rule_id              text not null references dq.rule(rule_id),
  passed               boolean not null,
  violation_count      bigint,
  sample_violations    jsonb,              -- limited sample for debugging
  created_at           timestamptz not null default now()
);

-- ============ Portfolio monitoring marts ============
-- Keep minimal MVP marts; expand post-launch.

create table if not exists mart.risk_portfolio_daily (
  as_of_date           date not null,
  product_type         text not null,
  decisions_total      bigint not null,
  approvals_total      bigint not null,
  declines_total       bigint not null,
  degraded_total       bigint not null,
  avg_requested_amount numeric,
  dq_must_pass_failures bigint not null,
  created_at           timestamptz not null default now(),
  primary key (as_of_date, product_type)
);

create table if not exists mart.customer_risk_features_latest (
  customer_key         uuid primary key,
  updated_at           timestamptz not null default now(),
  bureau_score         integer,
  fraud_score          numeric,
  aml_status           text,
  feature_payload      jsonb
);

create table if not exists mart.dq_scorecard_daily (
  as_of_date           date not null,
  layer                text not null,
  must_pass_failed     bigint not null,
  warn_failed          bigint not null,
  created_at           timestamptz not null default now(),
  primary key (as_of_date, layer)
);

