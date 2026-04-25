-- Silver layer: conformed canonical tables + identity resolution.

create schema if not exists silver;

-- Canonical customer profile (internal)
create table if not exists silver.customer_profile (
  internal_uuid        uuid primary key,
  full_name            text,
  dob                  date,
  phone_e164           text,
  email_norm           text,
  emirates_id          text,
  kyc_status           text,
  created_at           timestamptz not null default now(),
  updated_at           timestamptz not null default now()
);

-- AECB reports parsed from XML (one row per report per Emirates ID per batch)
create table if not exists silver.aecb_report (
  aecb_report_id       bigserial primary key,
  emirates_id          text not null,
  bureau_batch_id      text not null,
  report_ts            timestamptz,
  bureau_score         integer,
  delinquency_flags    jsonb,
  raw_object_id        bigint references audit.ingestion_object(object_id),
  created_at           timestamptz not null default now(),
  unique (emirates_id, bureau_batch_id)
);

-- Fraud scores (write-through at decision time). Keyed by decision_id for snapshotting.
create table if not exists silver.fraud_score (
  decision_id          text primary key,
  phone_e164           text,
  email_norm           text,
  provider_name        text not null,
  provider_request_ts  timestamptz not null,
  provider_response_ts timestamptz,
  score                numeric,
  risk_band            text,
  reason_codes         jsonb,
  raw_req_object_id    bigint references audit.ingestion_object(object_id),
  raw_res_object_id    bigint references audit.ingestion_object(object_id),
  created_at           timestamptz not null default now()
);

-- AML/PEP screening events received via webhook (may arrive after decision)
create table if not exists silver.aml_screen (
  aml_event_id         text primary key,   -- vendor event id
  full_name            text,
  dob                  date,
  screening_ts         timestamptz not null,
  status               text not null,      -- clear|potential_match|match|error|...
  match_details        jsonb,
  raw_object_id        bigint references audit.ingestion_object(object_id),
  created_at           timestamptz not null default now()
);

-- Identity observations unify source-specific keys in a common record.
create table if not exists silver.identity_observation (
  observation_id       bigserial primary key,
  source               text not null,      -- aecb|fraud|aml|internal
  source_record_id     text not null,      -- e.g., aecb_report_id, decision_id, aml_event_id, internal_uuid
  emirates_id          text,
  internal_uuid        uuid,
  phone_e164           text,
  email_norm           text,
  full_name_norm       text,
  dob                  date,
  observed_at          timestamptz not null default now(),
  unique (source, source_record_id)
);

-- Resolved customer entity key (stable).
create table if not exists silver.customer (
  customer_key         uuid primary key,
  created_at           timestamptz not null default now(),
  -- Optional "best known" attributes (survivorship output)
  emirates_id          text,
  internal_uuid        uuid,
  phone_e164           text,
  email_norm           text,
  full_name_norm       text,
  dob                  date,
  attributes           jsonb
);

-- Link evidence: many observations can map to one customer_key.
create table if not exists silver.customer_link (
  link_id              bigserial primary key,
  observation_id       bigint not null references silver.identity_observation(observation_id),
  customer_key         uuid not null references silver.customer(customer_key),
  match_rule           text not null,      -- emirates_id_exact|uuid_exact|phone_email_exact|name_dob_fuzzy|...
  confidence           numeric not null,   -- 0..1
  matched_at           timestamptz not null default now(),
  evidence             jsonb,
  unique (observation_id, customer_key)
);

-- Merge events: when two customer_keys are later proven the same (keep history).
create table if not exists silver.customer_merge_event (
  merge_event_id       bigserial primary key,
  from_customer_key    uuid not null references silver.customer(customer_key),
  to_customer_key      uuid not null references silver.customer(customer_key),
  reason               text not null,
  decided_by           text not null,      -- rule|manual|backfill
  decided_at           timestamptz not null default now(),
  unique (from_customer_key, to_customer_key)
);

create index if not exists idx_identity_obs_emirates_id on silver.identity_observation (emirates_id);
create index if not exists idx_identity_obs_phone_email on silver.identity_observation (phone_e164, email_norm);
create index if not exists idx_identity_obs_name_dob on silver.identity_observation (full_name_norm, dob);

