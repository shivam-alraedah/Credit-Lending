from __future__ import annotations

"""
Identity resolution sample (deterministic-first).

Algorithm (launch):
1) Strong keys:
   - emirates_id exact (AECB)
   - internal_uuid exact (internal profile)
2) Medium keys:
   - phone+email exact (fraud/internal)
3) Weak keys:
   - name_norm + dob exact (AML/internal) with low confidence

Outputs:
- inserts/updates `silver.customer` (stable customer_key)
- inserts `silver.customer_link` rows with match_rule/confidence/evidence
- records merges in `silver.customer_merge_event` when two keys are proven same
"""

import uuid
from typing import Any, Dict, Optional, Tuple

import psycopg

from mal_pipeline.common.db import execute, fetchall


def _find_customer_by_emirates_id(conn: psycopg.Connection, emirates_id: str) -> Optional[str]:
    rows = fetchall(
        conn,
        """
        select customer_key::text
        from silver.customer
        where emirates_id = %(emirates_id)s
        limit 1
        """,
        {"emirates_id": emirates_id},
    )
    return rows[0][0] if rows else None


def _find_customer_by_internal_uuid(conn: psycopg.Connection, internal_uuid: str) -> Optional[str]:
    rows = fetchall(
        conn,
        """
        select customer_key::text
        from silver.customer
        where internal_uuid = %(internal_uuid)s::uuid
        limit 1
        """,
        {"internal_uuid": internal_uuid},
    )
    return rows[0][0] if rows else None


def _find_customer_by_phone_email(conn: psycopg.Connection, phone_e164: str, email_norm: str) -> Optional[str]:
    rows = fetchall(
        conn,
        """
        select customer_key::text
        from silver.customer
        where phone_e164 = %(phone)s and email_norm = %(email)s
        limit 1
        """,
        {"phone": phone_e164, "email": email_norm},
    )
    return rows[0][0] if rows else None


def _find_customer_by_name_dob(conn: psycopg.Connection, full_name_norm: str, dob: str) -> Optional[str]:
    rows = fetchall(
        conn,
        """
        select customer_key::text
        from silver.customer
        where full_name_norm = %(name)s and dob = %(dob)s::date
        limit 1
        """,
        {"name": full_name_norm, "dob": dob},
    )
    return rows[0][0] if rows else None


def _create_customer(
    conn: psycopg.Connection,
    *,
    emirates_id: Optional[str],
    internal_uuid: Optional[str],
    phone_e164: Optional[str],
    email_norm: Optional[str],
    full_name_norm: Optional[str],
    dob: Optional[str],
    attributes: Optional[Dict[str, Any]] = None,
) -> str:
    customer_key = str(uuid.uuid4())
    execute(
        conn,
        """
        insert into silver.customer (
          customer_key, emirates_id, internal_uuid, phone_e164, email_norm, full_name_norm, dob, attributes
        ) values (
          %(customer_key)s::uuid, %(emirates_id)s, %(internal_uuid)s::uuid, %(phone)s, %(email)s, %(name)s, %(dob)s::date, %(attributes)s::jsonb
        )
        """,
        {
            "customer_key": customer_key,
            "emirates_id": emirates_id,
            "internal_uuid": internal_uuid,
            "phone": phone_e164,
            "email": email_norm,
            "name": full_name_norm,
            "dob": dob,
            "attributes": attributes or {},
        },
    )
    return customer_key


def _link_observation(
    conn: psycopg.Connection,
    *,
    observation_id: int,
    customer_key: str,
    match_rule: str,
    confidence: float,
    evidence: Dict[str, Any],
) -> None:
    execute(
        conn,
        """
        insert into silver.customer_link (observation_id, customer_key, match_rule, confidence, evidence)
        values (%(observation_id)s, %(customer_key)s::uuid, %(match_rule)s, %(confidence)s, %(evidence)s::jsonb)
        on conflict (observation_id, customer_key) do nothing
        """,
        {
            "observation_id": observation_id,
            "customer_key": customer_key,
            "match_rule": match_rule,
            "confidence": confidence,
            "evidence": evidence,
        },
    )


def resolve_one_observation(conn: psycopg.Connection, observation: Dict[str, Any]) -> Tuple[str, str, float]:
    """
    observation must include:
      observation_id, emirates_id, internal_uuid, phone_e164, email_norm, full_name_norm, dob
    Returns: (customer_key, match_rule, confidence)
    """
    obs_id = int(observation["observation_id"])
    emirates_id = observation.get("emirates_id")
    internal_uuid = observation.get("internal_uuid")
    phone = observation.get("phone_e164")
    email = observation.get("email_norm")
    name = observation.get("full_name_norm")
    dob = observation.get("dob")

    customer_key = None
    match_rule = "new_customer"
    confidence = 0.5
    evidence: Dict[str, Any] = {}

    if emirates_id:
        customer_key = _find_customer_by_emirates_id(conn, emirates_id)
        if customer_key:
            match_rule, confidence, evidence = "emirates_id_exact", 1.0, {"emirates_id": emirates_id}
    if not customer_key and internal_uuid:
        customer_key = _find_customer_by_internal_uuid(conn, internal_uuid)
        if customer_key:
            match_rule, confidence, evidence = "internal_uuid_exact", 1.0, {"internal_uuid": internal_uuid}
    if not customer_key and phone and email:
        customer_key = _find_customer_by_phone_email(conn, phone, email)
        if customer_key:
            match_rule, confidence, evidence = "phone_email_exact", 0.85, {"phone_e164": phone, "email_norm": email}
    if not customer_key and name and dob:
        customer_key = _find_customer_by_name_dob(conn, name, dob)
        if customer_key:
            match_rule, confidence, evidence = "name_dob_exact_norm", 0.65, {"full_name_norm": name, "dob": dob}

    if not customer_key:
        customer_key = _create_customer(
            conn,
            emirates_id=emirates_id,
            internal_uuid=internal_uuid,
            phone_e164=phone,
            email_norm=email,
            full_name_norm=name,
            dob=dob,
            attributes={"created_from_observation_id": obs_id},
        )
        evidence = {"created_from_observation_id": obs_id}

    _link_observation(
        conn,
        observation_id=obs_id,
        customer_key=customer_key,
        match_rule=match_rule,
        confidence=confidence,
        evidence=evidence,
    )

    return customer_key, match_rule, confidence

