# Contract: AECB (UAE Credit Bureau)

## 1. Identity

| Field | Value |
|---|---|
| **Name** | `aecb` |
| **Current version** | `v1.0` |
| **Owner (producer)** | AECB (Al Etihad Credit Bureau) |
| **Owner (Mal-side)** | Data Engineering (ingest) + Credit Risk (consumer) |
| **Consumers** | `silver.aecb_report`, `gold.decision_input_snapshot.inputs.aecb`, `mart.customer_risk_features_latest`, IFRS 9 segmentation |
| **Classification** | Regulated (UAE CB reporting) + PII (contains Emirates ID) |

## 2. Delivery

- **Transport**: SFTP pull, Mal-initiated. The SFTP connection is managed in Secrets Manager and accessed only by MWAA.
- **Cadence**: Daily drop (~02:00 GST). We pull at 03:00 GST with retry + backfill.
- **Expected volume**: ~15k subject records / day at launch; bursts up to 50k on catch-up days.
- **Failure semantics**: If no file by 06:00 GST, the dependent decisioning policy for PF and Card-alt products is `required_bureau` (hard-block for affected new applications). See `policy.py`.

## 3. Schema

Machine-readable: [`python/mal_pipeline/schemas/aecb.v1.json`](../../python/mal_pipeline/schemas/aecb.v1.json).

Human-readable (after our XML parser transforms into a canonical row):

| Field | Type | Required | Notes |
|---|---|---|---|
| `emirates_id` | string | yes | Regex: `^\d{3}-\d{4}-\d{7}-\d{1}$`. **PII**. |
| `bureau_batch_id` | string | yes | Vendor batch identifier; included in S3 prefix. |
| `report_ts` | timestamp (UTC) | yes | Report generation time. |
| `bureau_score` | integer | no | Typical range 300–900; treat out-of-range as DQ violation. |
| `delinquency_flags` | object | no | Free-form flags (e.g., `Dpd30Plus`, `Dpd60Plus`); keys preserved as-received. |

## 4. Identifiers

- **Match key**: `emirates_id`. Joined to `silver.customer.emirates_id` with deterministic match rule `emirates_id_exact` (confidence 1.0).
- **PII**: `emirates_id` is PII — never appears in S3 keys, log lines, or dashboards; protected by schema-level role grants.

## 5. SLAs / SLOs

| Metric | Target |
|---|---|
| File arrival time | ≤ 06:00 GST daily |
| Parse success rate per file | ≥ 99.9% of rows |
| Silver visible latency (from S3 write) | ≤ 10 minutes |

## 6. Quality rules

- **Must-pass** (blocks Gold publish for the batch):
  - `silver_aecb_emirates_id_format` — Emirates ID regex valid.
  - `silver_aecb_batch_id_present` — batch ID populated on every row.
- **Warn**:
  - Bureau score null rate spike (>5% delta vs 7-day mean).
  - Delinquency flags key-set drift (new keys from vendor trigger an alert but do not block).

Rules live in [`python/mal_pipeline/dq/rules.yml`](../../python/mal_pipeline/dq/rules.yml).

## 7. Evolution policy

- **Breaking**: regex change, field rename, field removal, type change. Require a new major version + the §5.2 pattern from `docs/schema_evolution.md`.
- **Non-breaking**: new optional XML elements; new `delinquency_flags` keys.
- **Version bumps**: coordinated with AECB; announced in `#data-contracts` channel ≥ 14 days before cutover.

## 8. Operational contacts

- Vendor support: `ops@aecb.ae` (placeholder).
- Mal ingest owner: Data Engineering (`@de-oncall` in Slack).
- Risk owner: Credit Risk Lead.
- Escalation path: `@de-oncall` → Data Engineering Manager → VP Data.
