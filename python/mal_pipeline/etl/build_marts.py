from __future__ import annotations

"""
MVP mart builders (Postgres-first).

Design intent:
- build incrementally by as_of_date
- easy to ELT into Redshift later (same logical tables)
"""

from datetime import date

import psycopg

from mal_pipeline.common.db import execute, fetchall


def build_risk_portfolio_daily(conn: psycopg.Connection, as_of: date) -> None:
    """
    Build `mart.risk_portfolio_daily` for the given date.

    Joins `gold.decision` aggregates with the day's DQ must-pass failure totals
    so dashboards can correlate decision volume with data-quality health.
    Idempotent via ON CONFLICT (as_of_date, product_type) and safe to re-run.
    """
    rows = fetchall(
        conn,
        """
        with d as (
          select
            requested_at::date as as_of_date,
            product_type,
            count(*) as decisions_total,
            sum(case when decision_status = 'approved' then 1 else 0 end) as approvals_total,
            sum(case when decision_status = 'declined' then 1 else 0 end) as declines_total,
            sum(case when decision_policy like 'degraded%%' then 1 else 0 end) as degraded_total,
            avg(requested_amount) as avg_requested_amount
          from gold.decision
          where requested_at::date = %(as_of)s::date
          group by 1, 2
        ),
        dqf as (
          select count(*) as must_pass_failed
          from dq.result res
          join dq.rule r   on r.rule_id = res.rule_id
          join dq.run run  on run.dq_run_id = res.dq_run_id
          where run.executed_at::date = %(as_of)s::date
            and r.severity = 'MUST_PASS'::dq.severity
            and res.passed = false
        )
        select d.as_of_date, d.product_type, d.decisions_total, d.approvals_total,
               d.declines_total, d.degraded_total, d.avg_requested_amount,
               coalesce((select must_pass_failed from dqf), 0) as dq_must_pass_failures
        from d
        """,
        {"as_of": as_of.isoformat()},
    )

    for r in rows:
        execute(
            conn,
            """
            insert into mart.risk_portfolio_daily (
              as_of_date, product_type, decisions_total, approvals_total, declines_total, degraded_total,
              avg_requested_amount, dq_must_pass_failures
            )
            values (
              %(as_of_date)s::date, %(product_type)s, %(decisions_total)s, %(approvals_total)s,
              %(declines_total)s, %(degraded_total)s, %(avg_requested_amount)s, %(dq_must_pass_failures)s
            )
            on conflict (as_of_date, product_type) do update set
              decisions_total=excluded.decisions_total,
              approvals_total=excluded.approvals_total,
              declines_total=excluded.declines_total,
              degraded_total=excluded.degraded_total,
              avg_requested_amount=excluded.avg_requested_amount,
              dq_must_pass_failures=excluded.dq_must_pass_failures
            """,
            {
                "as_of_date": r[0],
                "product_type": r[1],
                "decisions_total": r[2],
                "approvals_total": r[3],
                "declines_total": r[4],
                "degraded_total": r[5],
                "avg_requested_amount": r[6],
                "dq_must_pass_failures": r[7],
            },
        )


def build_dq_scorecard_daily(conn: psycopg.Connection, as_of: date) -> None:
    rows = fetchall(
        conn,
        """
        select
          %(as_of)s::date as as_of_date,
          r.layer,
          sum(case when r.severity = 'MUST_PASS'::dq.severity and res.passed = false then 1 else 0 end) as must_pass_failed,
          sum(case when r.severity = 'WARN'::dq.severity and res.passed = false then 1 else 0 end) as warn_failed
        from dq.result res
        join dq.rule r on r.rule_id = res.rule_id
        join dq.run run on run.dq_run_id = res.dq_run_id
        where run.executed_at::date = %(as_of)s::date
        group by 2
        """,
        {"as_of": as_of.isoformat()},
    )

    for r in rows:
        execute(
            conn,
            """
            insert into mart.dq_scorecard_daily (as_of_date, layer, must_pass_failed, warn_failed)
            values (%(as_of)s::date, %(layer)s, %(mp)s, %(w)s)
            on conflict (as_of_date, layer) do update set
              must_pass_failed=excluded.must_pass_failed,
              warn_failed=excluded.warn_failed
            """,
            {"as_of": as_of.isoformat(), "layer": r[1], "mp": r[2], "w": r[3]},
        )

