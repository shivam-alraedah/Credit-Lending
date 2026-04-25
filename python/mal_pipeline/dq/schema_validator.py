from __future__ import annotations

"""
JSON Schema validation for vendor payloads and Mal-owned snapshot payloads.

Usage sites:
- Ingestion modules call `validate_payload("fraud_response", "1.0", payload)`
  before writing Silver. A failure is a must-pass DQ violation; caller decides
  whether to reject the record or degrade.
- A scheduled DQ task calls `validate_recent_snapshots` / similar to re-verify
  stored Gold payloads, catching drift even if upstream validation was skipped.

Design:
- Schemas live as files under mal_pipeline.schemas, versioned by major (e.g.
  fraud_response.v1.json). The `version` arg is a full string like "1.0";
  we load the file matching the major component.
- jsonschema's Draft 2020-12 validator is used.
- Validation errors are returned as a structured list so DQ can log/sample
  them without raising. `validate_or_raise` is available for hot-path ingress.
"""

import json
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from typing import Any, Dict, Iterable, List, Optional, Tuple

from jsonschema import Draft202012Validator


@dataclass(frozen=True)
class SchemaViolation:
    path: str
    message: str
    schema_path: str

    def to_dict(self) -> Dict[str, str]:
        return {"path": self.path, "message": self.message, "schema_path": self.schema_path}


class SchemaValidationError(Exception):
    def __init__(self, source: str, version: str, violations: List[SchemaViolation]):
        super().__init__(f"{source} v{version} payload failed validation: {len(violations)} errors")
        self.source = source
        self.version = version
        self.violations = violations


def _major(version: str) -> str:
    return version.split(".", 1)[0]


@lru_cache(maxsize=64)
def _load_schema(source: str, major: str) -> Draft202012Validator:
    """
    Load schemas/<source>.v<major>.json from this package. Cached per process.
    """
    filename = f"{source}.v{major}.json"
    schema_files = resources.files("mal_pipeline.schemas")
    path = schema_files / filename
    if not path.is_file():
        raise FileNotFoundError(f"schema not found: {filename}")
    schema = json.loads(path.read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def validate_payload(source: str, version: str, payload: Any) -> List[SchemaViolation]:
    """
    Returns an empty list on success, else a list of violations.

    `source` is a schema-file prefix: e.g., "fraud_response", "aml_webhook",
    "decision_input_snapshot", "aecb", "internal_profile", "fraud_request".
    """
    validator = _load_schema(source, _major(version))
    errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.absolute_path))
    return [
        SchemaViolation(
            path="/" + "/".join(map(str, e.absolute_path)),
            message=e.message,
            schema_path="/" + "/".join(map(str, e.absolute_schema_path)),
        )
        for e in errors
    ]


def validate_or_raise(source: str, version: str, payload: Any) -> None:
    errs = validate_payload(source, version, payload)
    if errs:
        raise SchemaValidationError(source, version, errs)


def validate_batch(
    source: str,
    version: str,
    payloads: Iterable[Any],
) -> Tuple[int, List[Tuple[int, List[SchemaViolation]]]]:
    """
    Validate an iterable of payloads. Returns (total_count, failures) where
    failures is a list of (index, violations) for the failing entries. Keeps
    memory bounded — does not retain passing payloads.
    """
    total = 0
    failures: List[Tuple[int, List[SchemaViolation]]] = []
    for i, payload in enumerate(payloads):
        total += 1
        errs = validate_payload(source, version, payload)
        if errs:
            failures.append((i, errs))
    return total, failures


# Helper: select schema source name from a Bronze object's S3 metadata tag.
#
# We stamp `x-amz-meta-schema_source` and `x-amz-meta-schema_version` on every
# Bronze object (see ingestion modules). The DQ job fetches a sample, reads
# those tags, and calls validate_payload without needing per-source branching.
def schema_source_from_metadata(metadata: Dict[str, str]) -> Optional[Tuple[str, str]]:
    source = metadata.get("schema_source")
    version = metadata.get("schema_version")
    if not source or not version:
        return None
    return source, version
