-- Schema registry: tracks which payload schema version is active per source
-- (and per logical sub-channel, e.g., fraud_request vs fraud_response).
--
-- Used by:
--   * ingestion code (records observed version per object)
--   * DQ ("payload conforms to registered schema")
--   * audit (regulators can see which contract was in force on any date)

create schema if not exists audit;

do $$ begin
  create type audit.schema_status as enum ('active', 'deprecated', 'retired', 'staged');
exception when duplicate_object then null; end $$;

create table if not exists audit.source_schema_version (
  source              text not null,          -- e.g., fraud_request, fraud_response, aml_webhook, aecb, internal_profile, decision_input_snapshot
  version             text not null,          -- e.g., "1.0", "1.1", "2.0"
  schema_ref          text not null,          -- path or URL to the machine-readable schema file
  status              audit.schema_status not null default 'active',
  effective_from      timestamptz not null default now(),
  effective_to        timestamptz,
  contract_ref        text,                   -- path to docs/data_contracts/<source>.md
  notes               text,
  primary key (source, version, effective_from)
);

create index if not exists idx_src_schema_active
  on audit.source_schema_version (source, status)
  where status = 'active';

-- Observed-version fact table: what versions actually appeared in live traffic.
-- Useful for "we think v1.0 is retired, but are we sure?"
create table if not exists audit.source_schema_observed (
  source              text not null,
  version             text not null,
  observed_on         date not null,
  count               bigint not null,
  primary key (source, version, observed_on)
);

-- Seed the initial v1.0 entries for every registered source.
insert into audit.source_schema_version (source, version, schema_ref, status, contract_ref, notes)
values
  ('aecb',                     '1.0', 'python/mal_pipeline/schemas/aecb.v1.json',                     'active', 'docs/data_contracts/aecb.md',                     'Launch baseline.'),
  ('fraud_request',            '1.0', 'python/mal_pipeline/schemas/fraud_request.v1.json',            'active', 'docs/data_contracts/fraud.md',                    'Launch baseline.'),
  ('fraud_response',           '1.0', 'python/mal_pipeline/schemas/fraud_response.v1.json',           'active', 'docs/data_contracts/fraud.md',                    'Launch baseline.'),
  ('aml_webhook',              '1.0', 'python/mal_pipeline/schemas/aml_webhook.v1.json',              'active', 'docs/data_contracts/aml.md',                      'Launch baseline.'),
  ('internal_profile',         '1.0', 'python/mal_pipeline/schemas/internal_profile.v1.json',         'active', 'docs/data_contracts/internal_profile.md',         'Launch baseline.'),
  ('decision_input_snapshot',  '1.0', 'python/mal_pipeline/schemas/decision_input_snapshot.v1.json',  'active', 'docs/data_contracts/decision_input_snapshot.md',  'Launch baseline.')
on conflict do nothing;

-- Example: deprecate v1.0 and stage v1.1 for a source (run at contract-update time)
--
-- update audit.source_schema_version
--    set status = 'deprecated', effective_to = now()
--  where source = 'fraud_response' and version = '1.0' and status = 'active';
--
-- insert into audit.source_schema_version (source, version, schema_ref, status, contract_ref, notes)
-- values (
--   'fraud_response', '1.1',
--   'python/mal_pipeline/schemas/fraud_response.v1.json',
--   'active',
--   'docs/data_contracts/fraud.md',
--   'Added optional explainability fields; minor non-breaking bump.'
-- );

-- Helper view: currently-active schemas (used by the DQ runner).
create or replace view audit.v_source_schema_active as
  select distinct on (source)
    source, version, schema_ref, effective_from, contract_ref
  from audit.source_schema_version
  where status = 'active'
  order by source, effective_from desc;
