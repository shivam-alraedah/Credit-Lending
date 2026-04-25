-- IFRS 9 provisioning data model (extension; separate from core decision-input pipeline)
--
-- Scope: Support Expected Credit Loss (ECL) calculation with:
--   - Credit exposure staging (Stage 1 / 2 / 3)
--   - PD, LGD, EAD as versioned parameters (Point-in-Time vs Through-the-Cycle)
--   - Audit trail for stage transitions and provision calculations
--   - Linkage to Mal's decision pipeline (gold.decision / customer_key)
--
-- Design pillars:
-- 1) Append-only fact tables for all risk-parameter versions (no updates).
-- 2) Effective-dated windows via (effective_from, effective_to).
-- 3) Separate PIT vs TTC parameter sets so we can explain provisioning movements.
-- 4) All stage changes recorded as events; current stage is the latest event.

create schema if not exists ifrs9;

-- Accounts = credit accounts opened after a decision is approved.
create table if not exists ifrs9.account (
  account_id           text primary key,
  customer_key         uuid not null,                -- from silver.customer
  decision_id          text,                          -- from gold.decision
  product_id           text,                          -- from gold.product
  origination_date     date not null,
  original_amount_aed  numeric not null,
  currency             text not null default 'AED',
  term_months          integer,
  status               text not null,                 -- active|closed|written_off
  created_at           timestamptz not null default now()
);

-- Exposure snapshots (append-only), typically at month-end.
create table if not exists ifrs9.exposure_snapshot (
  snapshot_id          bigserial primary key,
  account_id           text not null references ifrs9.account(account_id),
  as_of_date           date not null,
  outstanding_principal numeric not null,
  accrued_profit        numeric not null default 0,   -- Sharia: profit not interest
  undrawn_commitment    numeric not null default 0,
  ccf                   numeric not null default 1.0, -- credit conversion factor for EAD
  days_past_due         integer not null default 0,
  created_at            timestamptz not null default now(),
  unique (account_id, as_of_date)
);

-- Stage transitions are events; never mutate, only append.
do $$ begin
  create type ifrs9.stage_code as enum ('S1', 'S2', 'S3');
exception when duplicate_object then null; end $$;

create table if not exists ifrs9.stage_event (
  stage_event_id       bigserial primary key,
  account_id           text not null references ifrs9.account(account_id),
  as_of_date           date not null,
  prev_stage           ifrs9.stage_code,
  new_stage            ifrs9.stage_code not null,
  reason_code          text not null,                 -- dpd30|dpd90|sicr|cure|forbearance|...
  reason_detail        jsonb,
  decided_by           text not null,                 -- rule|manual|model
  created_at           timestamptz not null default now()
);

create index if not exists idx_stage_event_account_date
  on ifrs9.stage_event (account_id, as_of_date);

-- Risk parameter versions (PD / LGD / EAD-CCF) with PIT vs TTC flag.
-- Parameters are looked up by (segment, stage, horizon_months, basis) with effective date.
do $$ begin
  create type ifrs9.param_basis as enum ('PIT', 'TTC');
exception when duplicate_object then null; end $$;

create table if not exists ifrs9.risk_segment (
  segment_id           text primary key,              -- e.g., bnpl_3m_emirati_salaried
  description          text
);

create table if not exists ifrs9.pd_parameter (
  pd_param_id          bigserial primary key,
  segment_id           text not null references ifrs9.risk_segment(segment_id),
  stage                ifrs9.stage_code not null,
  horizon_months       integer not null,              -- 12 for S1; lifetime for S2/S3
  basis                ifrs9.param_basis not null,
  pd_value             numeric not null check (pd_value >= 0 and pd_value <= 1),
  effective_from       date not null,
  effective_to         date,
  version_note         text,
  created_at           timestamptz not null default now(),
  unique (segment_id, stage, horizon_months, basis, effective_from)
);

create table if not exists ifrs9.lgd_parameter (
  lgd_param_id         bigserial primary key,
  segment_id           text not null references ifrs9.risk_segment(segment_id),
  basis                ifrs9.param_basis not null,
  lgd_value            numeric not null check (lgd_value >= 0 and lgd_value <= 1),
  effective_from       date not null,
  effective_to         date,
  version_note         text,
  created_at           timestamptz not null default now(),
  unique (segment_id, basis, effective_from)
);

-- Macro overlays (e.g., scenario-weighted multipliers applied to PD/LGD).
create table if not exists ifrs9.macro_scenario (
  scenario_id          text primary key,              -- base|upside|downside
  weight               numeric not null check (weight >= 0 and weight <= 1),
  effective_from       date not null,
  effective_to         date,
  description          text
);

create table if not exists ifrs9.macro_overlay (
  overlay_id           bigserial primary key,
  scenario_id          text not null references ifrs9.macro_scenario(scenario_id),
  segment_id           text not null references ifrs9.risk_segment(segment_id),
  pd_multiplier        numeric not null default 1.0,
  lgd_multiplier       numeric not null default 1.0,
  effective_from       date not null,
  effective_to         date,
  unique (scenario_id, segment_id, effective_from)
);

-- Account → risk segment mapping (may change over time).
create table if not exists ifrs9.account_segment_assignment (
  account_id           text not null references ifrs9.account(account_id),
  segment_id           text not null references ifrs9.risk_segment(segment_id),
  effective_from       date not null,
  effective_to         date,
  primary key (account_id, effective_from)
);

-- Provision calculation audit: one row per (account, as_of_date, run).
create table if not exists ifrs9.provision_calculation (
  provision_calc_id    bigserial primary key,
  account_id           text not null references ifrs9.account(account_id),
  as_of_date           date not null,
  stage                ifrs9.stage_code not null,
  segment_id           text not null references ifrs9.risk_segment(segment_id),
  pd_param_id          bigint references ifrs9.pd_parameter(pd_param_id),
  lgd_param_id         bigint references ifrs9.lgd_parameter(lgd_param_id),
  pd_used              numeric not null,
  lgd_used             numeric not null,
  ead_used             numeric not null,
  scenario_weights     jsonb not null,                -- explainability for macro overlays
  ecl_amount           numeric not null,              -- computed provision
  run_id               uuid not null,                 -- batch identifier
  created_at           timestamptz not null default now(),
  unique (account_id, as_of_date, run_id)
);
