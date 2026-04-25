from __future__ import annotations

"""
Degraded decisioning policy engine (launch-grade).

Responsibilities:
- Determine the applicable `decision_policy_code` based on which inputs are
  available + fresh at decision time (bureau, fraud, AML, internal profile).
- Enforce product + policy caps (daily count, daily exposure, per-decision max)
  using gold.degraded_policy_cap + gold.degraded_policy_daily_usage.
- Stamp `gold.decision.policy_code` for audit.

This module is idempotent on (decision_id): if called twice with the same inputs
it re-derives the same policy without double-counting caps (caller must only
increment usage once, after final approval).
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

import psycopg

from mal_pipeline.common.db import execute, fetchall


@dataclass(frozen=True)
class InputAvailability:
    bureau_present: bool
    bureau_fresh: bool  # within product-configured freshness window
    fraud_present: bool
    aml_present: bool
    internal_present: bool


@dataclass(frozen=True)
class PolicyDecision:
    policy_code: str
    allowed: bool
    reason: str
    requires_reassessment: bool  # true for degraded_no_bureau
    caps_checked: Dict[str, Any]


def _product_caps(conn: psycopg.Connection, product_id: str, policy_code: str) -> Optional[Dict[str, Any]]:
    rows = fetchall(
        conn,
        """
        select daily_count_cap, daily_exposure_cap_aed, per_decision_max_aed
        from gold.degraded_policy_cap
        where product_id = %(p)s
          and policy_code = %(c)s::gold.decision_policy_code
          and (effective_to is null or effective_to > now())
        order by effective_from desc
        limit 1
        """,
        {"p": product_id, "c": policy_code},
    )
    if not rows:
        return None
    r = rows[0]
    return {"daily_count_cap": int(r[0]), "daily_exposure_cap_aed": float(r[1]), "per_decision_max_aed": float(r[2])}


def _current_usage(conn: psycopg.Connection, product_id: str, policy_code: str, today: date) -> Dict[str, float]:
    rows = fetchall(
        conn,
        """
        select count_used, exposure_used_aed
        from gold.degraded_policy_daily_usage
        where as_of_date = %(d)s and product_id = %(p)s and policy_code = %(c)s::gold.decision_policy_code
        """,
        {"d": today, "p": product_id, "c": policy_code},
    )
    if not rows:
        return {"count_used": 0, "exposure_used_aed": 0.0}
    return {"count_used": int(rows[0][0]), "exposure_used_aed": float(rows[0][1])}


def derive_policy(
    conn: psycopg.Connection,
    *,
    product_id: str,
    product_type: str,
    requested_amount_aed: float,
    availability: InputAvailability,
    today: Optional[date] = None,
) -> PolicyDecision:
    """
    Returns the policy decision. Does NOT increment usage; caller increments
    once the final decision is written.
    """
    today = today or date.today()

    # Always require AML to be present (we accept degraded only for bureau/fraud).
    if not availability.aml_present:
        return PolicyDecision(
            policy_code="blocked",
            allowed=False,
            reason="aml_missing",
            requires_reassessment=False,
            caps_checked={},
        )

    # Full happy path
    if availability.bureau_present and availability.bureau_fresh and availability.fraud_present:
        return PolicyDecision(
            policy_code="full_inputs",
            allowed=True,
            reason="all_inputs_present",
            requires_reassessment=False,
            caps_checked={},
        )

    # PF / card-alt: hard-block if bureau missing or stale.
    if product_type in ("personal_finance", "card_alt") and (
        not availability.bureau_present or not availability.bureau_fresh
    ):
        return PolicyDecision(
            policy_code="required_bureau",
            allowed=False,
            reason="bureau_required_for_product",
            requires_reassessment=False,
            caps_checked={},
        )

    # BNPL: allow degraded_no_bureau under caps.
    if product_type == "bnpl" and (not availability.bureau_present or not availability.bureau_fresh):
        caps = _product_caps(conn, product_id, "degraded_no_bureau")
        if not caps:
            return PolicyDecision(
                policy_code="blocked",
                allowed=False,
                reason="no_degraded_cap_configured",
                requires_reassessment=False,
                caps_checked={},
            )
        if requested_amount_aed > caps["per_decision_max_aed"]:
            return PolicyDecision(
                policy_code="degraded_no_bureau",
                allowed=False,
                reason="exceeds_per_decision_max",
                requires_reassessment=True,
                caps_checked=caps,
            )
        usage = _current_usage(conn, product_id, "degraded_no_bureau", today)
        if usage["count_used"] + 1 > caps["daily_count_cap"]:
            return PolicyDecision(
                policy_code="degraded_no_bureau",
                allowed=False,
                reason="daily_count_cap_exhausted",
                requires_reassessment=True,
                caps_checked={"caps": caps, "usage": usage},
            )
        if usage["exposure_used_aed"] + requested_amount_aed > caps["daily_exposure_cap_aed"]:
            return PolicyDecision(
                policy_code="degraded_no_bureau",
                allowed=False,
                reason="daily_exposure_cap_exhausted",
                requires_reassessment=True,
                caps_checked={"caps": caps, "usage": usage},
            )
        return PolicyDecision(
            policy_code="degraded_no_bureau",
            allowed=True,
            reason="within_degraded_caps",
            requires_reassessment=True,
            caps_checked={"caps": caps, "usage": usage},
        )

    # Fraud missing (any product): only manual review.
    if not availability.fraud_present:
        return PolicyDecision(
            policy_code="manual_review",
            allowed=False,
            reason="fraud_missing_requires_review",
            requires_reassessment=True,
            caps_checked={},
        )

    return PolicyDecision(
        policy_code="blocked",
        allowed=False,
        reason="fallback_block",
        requires_reassessment=False,
        caps_checked={},
    )


def increment_usage(
    conn: psycopg.Connection,
    *,
    product_id: str,
    policy_code: str,
    exposure_aed: float,
    today: Optional[date] = None,
) -> None:
    today = today or date.today()
    execute(
        conn,
        """
        insert into gold.degraded_policy_daily_usage (
          as_of_date, product_id, policy_code, count_used, exposure_used_aed
        ) values (
          %(d)s, %(p)s, %(c)s::gold.decision_policy_code, 1, %(e)s
        )
        on conflict (as_of_date, product_id, policy_code) do update set
          count_used = gold.degraded_policy_daily_usage.count_used + 1,
          exposure_used_aed = gold.degraded_policy_daily_usage.exposure_used_aed + excluded.exposure_used_aed
        """,
        {"d": today, "p": product_id, "c": policy_code, "e": exposure_aed},
    )
