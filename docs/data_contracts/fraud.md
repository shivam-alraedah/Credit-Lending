# Contract: Fraud detection provider (real-time REST)

## 1. Identity

| Field | Value |
|---|---|
| **Name** | `fraud` |
| **Current version** | `v1.0` |
| **Owner (producer)** | External fraud-scoring provider |
| **Owner (Mal-side)** | Decisioning (caller) + Data Engineering (persistence) |
| **Consumers** | `silver.fraud_score`, `gold.decision_input_snapshot.inputs.fraud`, policy engine |
| **Classification** | Internal + PII (phone, email) |

## 2. Delivery

- **Transport**: HTTPS REST, Mal-initiated, synchronous per decision request.
- **Cadence**: Real-time at decision time. 1 call per decision.
- **Expected volume**: 10k/day at launch → 100k/day within 12 months.
- **Latency budget (provider)**: p95 ≤ 400 ms, p99 ≤ 800 ms. Total budget including our Bronze write is in `docs/aws_infrastructure.md` §7.
- **Failure semantics**: On timeout / 5xx, the decisioning service applies the product-specific degraded policy (see `policy.py`). Fraud response MUST be present for BNPL degraded-no-bureau approvals.

## 3. Schema

Machine-readable:

- Outbound request: [`python/mal_pipeline/schemas/fraud_request.v1.json`](../../python/mal_pipeline/schemas/fraud_request.v1.json)
- Inbound response: [`python/mal_pipeline/schemas/fraud_response.v1.json`](../../python/mal_pipeline/schemas/fraud_response.v1.json)

Human-readable response:

| Field | Type | Required | Notes |
|---|---|---|---|
| `schema_version` | string | yes | Must match active registered version (e.g., `"1.0"`). |
| `decision_id` | string | yes | Echoed; must match the request. |
| `score` | number (0..1000) | yes | Higher = riskier. |
| `risk_band` | string enum | yes | `low` | `medium` | `high` | `block`. |
| `reason_codes` | array<string> | yes | Vendor-defined codes; may be empty. |
| `provider_response_ts` | RFC3339 timestamp | yes | Vendor-generated. |

## 4. Identifiers

- **Match key**: `decision_id` (Mal-generated). Uniquely ties request ↔ response ↔ Silver row ↔ snapshot.
- **Secondary identity join**: `phone_e164` + `email_norm` → `silver.customer` via `phone_email_exact` match rule (confidence 0.85).
- **PII**: `phone_e164`, `email_norm` are PII. They live in Bronze raw req/res and in `silver.fraud_score` only.

## 5. SLAs / SLOs

| Metric | Target |
|---|---|
| Provider availability | ≥ 99.9% monthly |
| Provider p99 latency | ≤ 800 ms |
| Mal write-through end-to-end (API send → Silver row) | ≤ 1000 ms p99 |
| Silver-row-per-decision coverage | 100% for approved decisions |

## 6. Quality rules

- **Must-pass**:
  - `silver_fraud_score_has_response_ts` (already defined in repo).
  - `silver_fraud_score_score_in_range` (0 ≤ score ≤ 1000).
  - `silver_fraud_score_schema_version_registered`.
- **Warn**:
  - Reason-code set drift (new codes appearing not in our enum).
  - Response-time SLO violation rate > 1%.

## 7. Evolution policy

- **Breaking**: score scale change, enum narrowing (e.g., removing `block`), removal of `reason_codes`, change in `decision_id` echo semantics.
- **Non-breaking**: additional optional response fields; adding new reason codes (producers should update our enum); widening `risk_band` with new values (we treat unknown values as `unknown` but alert).
- **Cutover window**: 14 days of parallel support before retiring previous major.

## 8. Operational contacts

- Vendor TAM + 24/7 ops line on file with Risk team.
- Mal owner: Decisioning engineering lead.
- Escalation: Decisioning → Data Engineering → VP Risk for degraded-policy activation.
