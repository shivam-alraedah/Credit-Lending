from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Optional

import psycopg

from .config import PostgresConfig


def _dsn(cfg: PostgresConfig) -> str:
    return (
        f"host={cfg.host} port={cfg.port} dbname={cfg.dbname} "
        f"user={cfg.user} password={cfg.password} sslmode={cfg.sslmode}"
    )


@contextmanager
def get_conn(cfg: PostgresConfig) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(_dsn(cfg))
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(conn: psycopg.Connection, sql: str, params: Optional[dict] = None) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, params or {})


def fetchall(conn: psycopg.Connection, sql: str, params: Optional[dict] = None):
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()

