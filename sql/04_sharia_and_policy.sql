-- Sharia-compliant product structures + degraded decisioning policy tables.
--
-- Rationale:
-- Mal's products must not accrue riba (interest). Islamic finance typically uses
-- profit-based structures (e.g., Murabaha markup, Ijara rentals, Tawarruq, Qard
-- Hasan benevolent loans). Late fees, where charged, are commonly routed to
-- charity rather than recognized as income. We encode these concepts at the
-- product and contract level so decision snapshots and risk marts can reason
-- about them without free-text "magic strings".

-- Enumerations ---------------------------------------------------------------

do $$ begin
  create type gold.sharia_contract_type as enum (
    'murabaha',   -- cost-plus sale (most common for BNPL/card-alt here)
    'ijara',      -- leasing with eventual transfer
    'tawarruq',   -- commodity-based cash financing
    'qard_hasan', -- benevolent loan, no markup
    'musharaka',  -- partnership
    'mudaraba'    -- profit-sharing
  );
exception when duplicate_object then null; end $$;

do $$ begin
  create type gold.late_fee_treatment as enum (
    'charity',             -- route late fees to a pre-approved charity account
    'cost_recovery_only',  -- only recover administrative cost
    'none'                 -- no late fees
  );
exception when duplicate_object then null; end $$;

do $$ begin
  create type gold.decision_policy_code as enum (
    'full_inputs',                -- all must-have sources present + DQ green
    'required_bureau',            -- bureau required; hard-block if missing
    'degraded_no_bureau',         -- bureau missing; allowed under caps
    'degraded_no_fraud',          -- fraud missing; restricted
    'manual_review',              -- routed to human ops
    'blocked'                     -- policy blocks decision
  );
exception when duplicate_object then null; end $$;

-- Product catalog ------------------------------------------------------------

create table if not exists gold.product (
  product_id           text primary key,            -- e.g., pf_murabaha_12m, bnpl_3m
  product_type         text not null,               -- personal_finance|bnpl|card_alt
  sharia_contract      gold.sharia_contract_type not null,
  profit_rate_bps      integer,                     -- disclosed markup in bps, if applicable
  max_tenor_months     integer,
  max_exposure_aed     numeric,
  late_fee_treatment   gold.late_fee_treatment not null default 'charity',
  charity_account_ref  text,                        -- opaque ref to treasury charity ledger
  shariah_board_approval_ref text,                  -- document/version ref for audit
  effective_from       timestamptz not null default now(),
  effective_to         timestamptz,
  notes                text
);

-- Enrich gold.decision with Sharia + policy fields ---------------------------

alter table gold.decision
  add column if not exists product_id     text references gold.product(product_id),
  add column if not exists sharia_contract gold.sharia_contract_type,
  add column if not exists profit_rate_bps integer,
  add column if not exists disclosures_version text,
  add column if not exists policy_code    gold.decision_policy_code;

-- Degraded decisioning: exposure caps & reconciliation -----------------------

-- Daily/configurable caps per product for degraded approvals.
create table if not exists gold.degraded_policy_cap (
  product_id           text references gold.product(product_id),
  policy_code          gold.decision_policy_code not null,
  daily_count_cap      integer not null,
  daily_exposure_cap_aed numeric not null,
  per_decision_max_aed  numeric not null,
  effective_from       timestamptz not null default now(),
  effective_to         timestamptz,
  primary key (product_id, policy_code, effective_from)
);

-- Running tally for enforcement; cheap to check at decision time.
create table if not exists gold.degraded_policy_daily_usage (
  as_of_date           date not null,
  product_id           text not null,
  policy_code          gold.decision_policy_code not null,
  count_used           integer not null default 0,
  exposure_used_aed    numeric not null default 0,
  primary key (as_of_date, product_id, policy_code)
);

-- Post-bureau reconciliation: when AECB eventually arrives, we revisit degraded
-- decisions. Append-only; each reassessment is a new row with a new snapshot_id.
create table if not exists gold.decision_reassessment (
  reassessment_id      uuid primary key,
  decision_id          text not null references gold.decision(decision_id),
  triggered_by         text not null,          -- aecb_arrived|manual|scheduled
  original_policy_code gold.decision_policy_code not null,
  new_policy_code      gold.decision_policy_code not null,
  outcome              text not null,          -- reaffirmed|limit_reduced|blocked|escalated
  new_snapshot_id      uuid references gold.decision_input_snapshot(snapshot_id),
  created_at           timestamptz not null default now()
);
