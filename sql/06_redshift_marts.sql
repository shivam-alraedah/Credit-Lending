-- Redshift DDL for the analytical marts (post-launch).
--
-- Apply in a Redshift cluster / Serverless workgroup. Keep the logical mart
-- schema identical to Postgres so dashboards port without rewrites.

create schema if not exists mart;
create schema if not exists gold;

-- ============================================================================
-- mart.risk_portfolio_daily
--   Small daily aggregate -> broadcast to all nodes (DISTSTYLE ALL).
--   Dashboards always filter by as_of_date -> COMPOUND SORTKEY.
-- ============================================================================
drop table if exists mart.risk_portfolio_daily;
create table mart.risk_portfolio_daily (
  as_of_date            date         not null encode az64,
  product_type          varchar(32)  not null encode zstd,
  decisions_total       bigint       not null encode az64,
  approvals_total       bigint       not null encode az64,
  declines_total        bigint       not null encode az64,
  degraded_total        bigint       not null encode az64,
  avg_requested_amount  numeric(18,2) encode az64,
  dq_must_pass_failures bigint       not null encode az64,
  created_at            timestamp    not null default getdate() encode az64,
  primary key (as_of_date, product_type)
)
diststyle all
compound sortkey (as_of_date, product_type);

-- ============================================================================
-- mart.dq_scorecard_daily
--   Tiny dimension-style table; ALL + sort by date.
-- ============================================================================
drop table if exists mart.dq_scorecard_daily;
create table mart.dq_scorecard_daily (
  as_of_date        date         not null encode az64,
  layer             varchar(32)  not null encode zstd,
  must_pass_failed  bigint       not null encode az64,
  warn_failed       bigint       not null encode az64,
  created_at        timestamp    not null default getdate() encode az64,
  primary key (as_of_date, layer)
)
diststyle all
compound sortkey (as_of_date, layer);

-- ============================================================================
-- mart.customer_risk_features_latest
--   Larger; joined with decision history on customer_key -> DISTKEY to co-locate.
--   Most dashboard queries filter on updated_at window -> sort on updated_at.
-- ============================================================================
drop table if exists mart.customer_risk_features_latest;
create table mart.customer_risk_features_latest (
  customer_key     varchar(36) not null encode zstd,
  updated_at       timestamp   not null encode az64,
  bureau_score     integer     encode az64,
  fraud_score      numeric(10,4) encode az64,
  aml_status       varchar(32) encode zstd,
  feature_payload  super       encode zstd,
  primary key (customer_key)
)
diststyle key distkey (customer_key)
compound sortkey (updated_at);

-- ============================================================================
-- gold.decision (analytical copy)
--   Partition-equivalent via SORTKEY(requested_at); co-locate with snapshots
--   via DISTKEY(customer_key).
-- ============================================================================
drop table if exists gold.decision;
create table gold.decision (
  decision_id        varchar(64)  not null encode zstd,
  product_type       varchar(32)  not null encode zstd,
  requested_amount   numeric(18,2) encode az64,
  currency           varchar(3)   default 'AED' encode zstd,
  requested_at       timestamp    not null encode az64,
  decisioned_at      timestamp    encode az64,
  decision_status    varchar(24)  not null encode zstd,
  decision_policy    varchar(48)  encode zstd,
  customer_key       varchar(36)  encode zstd,
  product_id         varchar(64)  encode zstd,
  sharia_contract    varchar(24)  encode zstd,
  profit_rate_bps    integer      encode az64,
  policy_code        varchar(32)  encode zstd,
  primary key (decision_id)
)
diststyle key distkey (customer_key)
compound sortkey (requested_at, product_type);

-- ============================================================================
-- gold.decision_input_snapshot (analytical copy)
--   Heavy JSON column -> SUPER with ZSTD.
--   DISTKEY on customer_key co-locates with gold.decision.
-- ============================================================================
drop table if exists gold.decision_input_snapshot;
create table gold.decision_input_snapshot (
  snapshot_id             varchar(36) not null encode zstd,
  decision_id             varchar(64) not null encode zstd,
  snapshot_ts             timestamp   not null encode az64,
  customer_key            varchar(36) encode zstd,
  identity_summary        super       encode zstd,
  inputs                  super       encode zstd,
  dq_summary              super       encode zstd,
  model_rule_version      varchar(48) encode zstd,
  supersedes_snapshot_id  varchar(36) encode zstd,
  sha256_hex              varchar(64) not null encode zstd,
  primary key (snapshot_id)
)
diststyle key distkey (customer_key)
compound sortkey (snapshot_ts);

-- ============================================================================
-- Staging schema for COPY-merge loads
-- ============================================================================
create schema if not exists stage;

drop table if exists stage.risk_portfolio_daily;
create table stage.risk_portfolio_daily (like mart.risk_portfolio_daily);

drop table if exists stage.decision;
create table stage.decision (like gold.decision);

-- ============================================================================
-- Example COPY from S3 parquet (uses a manifest for idempotent loads).
--
-- The ETL exports a month of rows from RDS into s3://mal-analytics/gold/decision/
--   requested_date=YYYY-MM/part-*.parquet
-- and writes a manifest with exact keys + expected byte counts.
-- ============================================================================
--
-- copy stage.decision
--   from 's3://mal-analytics-prod/gold/decision/manifests/2025-04.manifest'
--   iam_role 'arn:aws:iam::<acct>:role/RedshiftLoader'
--   manifest
--   format as parquet;
--
-- -- Merge into final:
-- begin;
--   delete from gold.decision d
--    using stage.decision s
--    where d.decision_id = s.decision_id;
--   insert into gold.decision select * from stage.decision;
--   truncate table stage.decision;
-- commit;
--
-- analyze gold.decision;
-- vacuum sort only gold.decision;
