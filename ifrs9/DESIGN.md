# IFRS 9 Provisioning Data Model — Design (1 page)

## Scope
Support Expected Credit Loss (ECL) calculation for Mal.ai's credit portfolio while satisfying IFRS 9 requirements for staging, parameterization (PD/LGD/EAD), and auditability of every provision number.

## Core design choices

- **Append-only everywhere.** `stage_event`, `exposure_snapshot`, and `provision_calculation` are never updated. This is the same principle used for decision snapshots in `gold.decision_input_snapshot`, so regulators see consistent evidential behavior across the platform.
- **Explicit stage transitions (not current-stage columns).** `ifrs9.stage_event` records every S1/S2/S3 change with a reason code and `decided_by` (rule/manual/model). Current stage is the latest event — we never overwrite history.
- **Versioned, effective-dated risk parameters.** `pd_parameter` and `lgd_parameter` carry `(effective_from, effective_to)` windows with a `basis` flag (`PIT` vs `TTC`). New versions are inserted; old ones remain to reproduce any historical ECL.
- **Segmented parameters.** `risk_segment` + `account_segment_assignment` separate “who the account is” from “what parameters apply”. Assignments are themselves effective-dated so segment re-classifications don’t rewrite history.
- **Macro overlays as scenarios.** `macro_scenario` (weights) + `macro_overlay` (per-segment PD/LGD multipliers) let us compute scenario-weighted ECLs without baking multipliers into base parameters.
- **EAD formula in the query, not the data.** EAD = `outstanding_principal + accrued_profit + ccf * undrawn_commitment`. Keeping `ccf` on the exposure snapshot makes the formula auditable per as_of_date without hidden coefficients.
- **One row per (account, as_of_date, run_id) in `provision_calculation`.** We store `pd_used`, `lgd_used`, `ead_used`, `scenario_weights`, and `ecl_amount`, plus the parameter IDs used. This is what makes every ECL number **replayable and explainable**.

## Sharia alignment

- We separate `accrued_profit` from `outstanding_principal` on exposure snapshots; there is no `interest_accrued` field — aligning with Murabaha/Ijara structures in `gold.product`.
- Late fees, when routed to charity in the product catalog, are excluded from `ead` here (they are not an asset to the firm). This keeps ECL numbers consistent with Sharia-compliant income recognition.

## Linkage to the decision pipeline

- `ifrs9.account.customer_key` links to `silver.customer.customer_key` and `decision_id` links to `gold.decision`. This gives auditors and model-risk teams a full thread from **application → decision snapshot → originated account → monthly stage/ECL history**.
- The model-monitoring narrative (post-launch) joins `provision_calculation` back to the **fraud / bureau / AML inputs used at decision time** via `gold.decision_input_snapshot`, closing the loop on whether pre-approval signals predicted post-origination outcomes.

## Launch vs post-launch

Launch: schema + baseline stage rules (DPD-driven) + rule-based PD/LGD parameter insertion + monthly ECL run.
Post-launch: SICR model flags feeding `stage_event.reason_code = sicr`, probabilistic PD model with versioning, macro scenarios calibrated quarterly, model-monitoring dashboards pulling from `provision_calculation` joined to `gold.decision_input_snapshot`.
