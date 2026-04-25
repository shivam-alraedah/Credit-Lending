from __future__ import annotations

import hashlib
from typing import Any


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_json_canonical(obj: Any) -> str:
    # Canonical JSON hashing: stable key order + no whitespace.
    # Keep simple (no Decimal-specific canonicalization) for this reference hasher.
    import json

    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256_text(payload)

