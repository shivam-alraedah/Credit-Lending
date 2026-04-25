from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class S3Location:
    bucket: str
    prefix: str

    def key(self, *parts: str) -> str:
        p = self.prefix.strip("/")
        tail = "/".join(s.strip("/") for s in parts if s)
        if not p:
            return tail
        if not tail:
            return p
        return f"{p}/{tail}"


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str = "require"


@dataclass(frozen=True)
class AppConfig:
    bronze: S3Location
    postgres: PostgresConfig
    aws_region: Optional[str] = None

