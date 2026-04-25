from __future__ import annotations

"""
Publication marker: one canonical "last known green DQ run" per layer.

Consumers read from dq.v_latest_green; producers (MWAA DAGs) call
`publish_if_green` at the end of a DQ run to move the marker forward if
and only if every MUST_PASS rule passed.

See docs/dq_consistency.md.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import psycopg

from mal_pipeline.common.db import execute, fetchall


VALID_LAYERS = {"bronze", "silver", "gold", "mart", "cross_db"}


@dataclass(frozen=True)
class PublicationMarker:
    layer: str
    last_green_run_id: str
    last_green_at: datetime
    updated_by: Optional[str] = None
    notes: Optional[str] = None


def get_last_green_run(conn: psycopg.Connection, layer: str) -> Optional[PublicationMarker]:
    """
    Return the current publication marker for a layer, or None if never set.
    """
    if layer not in VALID_LAYERS:
        raise ValueError(f"invalid layer {layer!r}")
    rows = fetchall(
        conn,
        """
        select layer, last_green_run_id::text, last_green_at, updated_by, notes
        from dq.publication_marker
        where layer = %(layer)s
        """,
        {"layer": layer},
    )
    if not rows:
        return None
    r = rows[0]
    return PublicationMarker(
        layer=r[0],
        last_green_run_id=r[1],
        last_green_at=r[2],
        updated_by=r[3],
        notes=r[4],
    )


def set_publication_marker(
    conn: psycopg.Connection,
    *,
    layer: str,
    dq_run_id: str,
    updated_by: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Upsert the marker for the given layer. Caller must ensure the run
    actually passed MUST_PASS (use `publish_if_green`).
    """
    if layer not in VALID_LAYERS:
        raise ValueError(f"invalid layer {layer!r}")
    execute(
        conn,
        """
        insert into dq.publication_marker (layer, last_green_run_id, last_green_at, updated_by, notes)
        values (%(layer)s, %(run_id)s::uuid, now(), %(updated_by)s, %(notes)s)
        on conflict (layer) do update set
          last_green_run_id = excluded.last_green_run_id,
          last_green_at     = excluded.last_green_at,
          updated_by        = excluded.updated_by,
          notes             = excluded.notes
        """,
        {
            "layer": layer,
            "run_id": dq_run_id,
            "updated_by": updated_by,
            "notes": notes,
        },
    )


def publish_if_green(
    conn: psycopg.Connection,
    *,
    layer: str,
    dq_run_id: str,
    must_pass: bool,
    updated_by: Optional[str] = "dq-runner",
    notes: Optional[str] = None,
) -> bool:
    """
    Move the marker forward if and only if must_pass is True.
    Returns True when the marker was updated, False otherwise.
    """
    if not must_pass:
        return False
    set_publication_marker(
        conn,
        layer=layer,
        dq_run_id=dq_run_id,
        updated_by=updated_by,
        notes=notes,
    )
    return True
