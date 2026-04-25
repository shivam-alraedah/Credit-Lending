# Contract: AML / PEP screening (webhook callbacks)

## 1. Identity

| Field | Value |
|---|---|
| **Name** | `aml` |
| **Current version** | `v1.0` |
| **Owner (producer)** | External AML/PEP screening vendor |
| **Owner (Mal-side)** | Compliance (consumer) + Data Engineering (ingest) |
| **Consumers** | `silver.aml_screen`, `gold.decision_input_snapshot.inputs.aml`, policy engine |
| **Classification** | Regulated + PII (name, DOB, match_details) |

## 2. Delivery

- **Transport**: HTTPS webhook POST to `/webhooks/aml`. Vendor signs the body via HMAC-SHA256 using a shared secret. See `mal_pipeline.ingest.aml_webhook_receiver.verify_signature`.
- **Cadence**: Event-driven. Typical 1–2 events per customer onboarding flow; ongoing monitoring events thereafter.
- **Expected volume**: ~5k events/day at launch, bursty; must tolerate 10x spikes.
- **Failure semantics**: Bronze write failure → 502 to vendor (they retry). All other DB-side failures return 200 + Bronze-already-written, and the reconcile DAG heals Silver on its own cadence.

## 3. Schema

Machine-readable: [`python/mal_pipeline/schemas/aml_webhook.v1.json`](../../python/mal_pipeline/schemas/aml_webhook.v1.json).

Human-readable payload:

| Field | Type | Required | Notes |
|---|---|---|---|
| `schema_version` | string | yes | e.g., `"1.0"`. |
| `event_id` | string | yes | Vendor-assigned unique event id; idempotency key. |
| `screening_ts` | RFC3339 timestamp | yes | Event time at vendor. |
| `subject.full_name` | string | yes | **PII**. |
| `subject.dob` | date (YYYY-MM-DD) | yes | **PII**. |
| `status` | enum | yes | `clear` | `potential_match` | `match` | `error` | `in_progress`. |
| `match_details` | object | no | Vendor-specific payload (sanctions list, score, etc.). |

## 4. Identifiers

- **Match key** for join to `customer_key`: `full_name_norm` + `dob` → `name_dob_exact_norm` rule (confidence 0.65).
- **Idempotency key**: `event_id`. Upsert in `silver.aml_screen(aml_event_id)`.
- **PII**: `full_name`, `dob`, and any match details are PII.

## 5. SLAs / SLOs

| Metric | Target |
|---|---|
| Receiver 2xx acknowledgement | within 1s p99 from request receipt |
| Reconcile Bronze → Silver | ≤ 5 minutes p95 |
| Event delivery (vendor-side) | 99.9% within 60s of screening decision |

## 6. Quality rules

- **Must-pass**:
  - Signature verification (`verify_signature`) — wrong signature → 401; no DB impact.
  - Payload conforms to registered schema.
  - `aml_event_id` non-null after reconcile.
- **Warn**:
  - Unknown `status` enum values (accept, alert).
  - Receiver latency p99 > 1s.

## 7. Evolution policy

- **Breaking**: status enum narrowing, rename of `event_id`, change in HMAC algorithm.
- **Non-breaking**: new optional `match_details` fields, additional statuses (we treat unknown as `unknown` + warn).
- **Cutover**: `X-AML-Signature` header is required; any signature-scheme change is a major bump and requires vendor-coordinated rollout.

## 8. Operational contacts

- Vendor 24/7 ops; SLA contract maintained by Compliance.
- Mal owner: Compliance Engineering.
- Escalation: Compliance → Data Engineering → CRO for AML hold decisions.
