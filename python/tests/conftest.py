"""
Shared test fakes for DQ runner / reconcile tests.

Simulates psycopg.Connection enough for db.execute/fetchall to work in tests
without a real Postgres. Route queries to canned rows via substring matching.
"""

from __future__ import annotations

from typing import Dict, List


class FakeCursor:
    def __init__(self, parent: "FakeConn"):
        self.parent = parent
        self._rows: List[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql: str, params=None) -> None:
        self.parent.executed.append((sql, params))
        # Pick the first registered fragment that appears in the SQL.
        matched: List[tuple] = []
        for fragment, rows in self.parent.responses.items():
            if fragment in sql:
                matched = rows
                break
        self._rows = matched

    def fetchall(self) -> List[tuple]:
        return self._rows


class FakeConn:
    """Tiny stand-in for psycopg.Connection."""

    def __init__(self, responses: Dict[str, List[tuple]] | None = None):
        self.responses: Dict[str, List[tuple]] = responses or {}
        self.executed: List[tuple] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)
