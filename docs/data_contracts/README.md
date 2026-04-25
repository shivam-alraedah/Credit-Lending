# Data Contracts

Every data source in the Mal decision-input pipeline is governed by a **data contract**: a single document that spells out *exactly* what the producer promises, what the consumer can assume, and what has to happen when something changes. If a question about a field comes up at 2 AM, the contract is the source of truth.

**Related:** How contracts support *domain judgment* and *DQ governance* is summarized in [`../design_criteria_map.md`](../design_criteria_map.md) (see the dimensions table and conflict scenarios).

## Contract file layout

Each contract is a Markdown file with these sections (in this order):

1. **Identity** — name, version, owner, consumer(s), classification (public / internal / PII / regulated).
2. **Delivery** — transport (SFTP / REST / webhook / DB), cadence, expected volume, failure semantics.
3. **Schema** — link to the machine-readable JSON Schema in `python/mal_pipeline/schemas/`, plus a human-readable field table.
4. **Identifiers** — which fields are keys, which are PII, how they join to `customer_key`.
5. **SLAs / SLOs** — freshness, availability, latency, error-rate budgets.
6. **Quality rules** — must-pass and warn-level checks and how they're enforced in this repo (link to `dq/rules.yml` entries).
7. **Evolution policy** — what counts as breaking here, who approves, cutover expectations.
8. **Operational contacts** — vendor contacts, Mal team owners, escalation paths.

## Contracts in this repo

- [`aecb.md`](aecb.md) — UAE Credit Bureau (AECB) batch XML via SFTP.
- [`fraud.md`](fraud.md) — Fraud detection provider, REST API, real-time JSON.
- [`aml.md`](aml.md) — AML/PEP screening vendor webhook callbacks.
- [`internal_profile.md`](internal_profile.md) — Internal customer profile from the core RDS Postgres.
- [`decision_input_snapshot.md`](decision_input_snapshot.md) — The Mal-owned Gold snapshot payload.

Contracts are versioned with the payload schemas. If `fraud.md` says `v1.2`, the matching machine-readable schema is `python/mal_pipeline/schemas/fraud_response.v1.json` (major-version-matched) and the contract tracks minor additions in-document.

## How a contract is used in practice

- Ingestion code (in `python/mal_pipeline/ingest/`) validates every payload against the registered contract's JSON Schema before writing Silver.
- DQ rules in `python/mal_pipeline/dq/rules.yml` enforce the contract's invariants at a set frequency.
- CI fails if a contract file is edited in a way that breaks back-compat without a corresponding new schema version.
- `audit.source_schema_version` records which contract version is currently active per source per day, giving regulators a full history.

## One-way rules (regardless of source)

- **Never** put PII in S3 keys or in log lines — contracts call out PII fields explicitly.
- **Never** silently widen or tighten field semantics — bump the contract version instead.
- **Never** remove a field without a migration sequence — see `docs/schema_evolution.md` §5.2 (expand → migrate → contract).
