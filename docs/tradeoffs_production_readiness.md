## Trade-offs & production readiness (summary)

> **Related:** This document is the primary deep dive for **trade-offs and production considerations**. For how that dimension ties to the rest of the repo, see [`design_criteria_map.md`](design_criteria_map.md).

### Conflict resolution trade-offs
- **Deterministic-first** matching (Emirates ID, internal UUID) minimizes false positives and is easiest to audit.
- **Phone+email** is medium strength (changeable; can be shared) → use lower confidence and track verification state if available.
- **Name+DOB** is weakest (ambiguity, transliteration) → keep low confidence and require additional evidence for merges.

**Failure modes**
- False merges (two people conflated): mitigated via confidence, evidence logging, and merge events.
- Missed links (same person split across keys): mitigated via periodic re-resolution and manual exception queue post-launch.

**At 10x scale**
- Move from rule-only to hybrid (rules + probabilistic model), add blocking rules for high-risk merges, and optimize identity graph storage/indexing.

### Data quality strategy
- **Must-pass** rules for compliance-critical fields and referential integrity (e.g., decision snapshot references, fraud response completeness).
- **Warn-only** for anomalies that should not halt decisioning but must alert (duplicate spikes, drift).

**Decision speed vs completeness**
- Product-specific policy: higher-risk products hard-block when bureau missing; BNPL can operate degraded below exposure thresholds with caps + re-review.

**When AECB is delayed**
- Enforce degraded decisioning only for low exposure, with daily caps and mandatory post-arrival reconciliation.

### Compliance & auditability
- **Full audit trail** via:
  - immutable Bronze raw objects + hashes
  - audit.ingestion_run/object/error
  - immutable Gold decision input snapshots with pointers back to Bronze

**Retention**
- Default: retain decision evidence for 7+ years (configurable); separate PII stores allow pseudonymization where required.

**Deletion requests**
- Prefer deleting/anonymizing PII in live profiles while retaining hashed decision evidence where legally mandated; document policy and legal basis.

### Sharia compliance considerations
- Data model should support **profit/fee-based structures** vs interest (riba):
  - product contract type, fee schedule, late-fee treatment (charity/donation routing if applicable)
  - asset-backed / murabaha details where relevant (purchase price, disclosed markup, ownership transfer)
  - disclosures and consent artifacts (timestamps + versions)

### What’s cut for a 48-hour scope
- Full CDC, full probabilistic identity matching, full lineage catalog, and automated backfills.
- Redshift marts (kept as a documented migration path).

