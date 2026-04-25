from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3


@dataclass(frozen=True)
class PutResult:
    bucket: str
    key: str
    etag: Optional[str]
    version_id: Optional[str]


def put_bytes(
    *,
    bucket: str,
    key: str,
    data: bytes,
    content_type: str,
    metadata: Optional[Dict[str, str]] = None,
    kms_key_id: Optional[str] = None,
    region: Optional[str] = None,
) -> PutResult:
    s3 = boto3.client("s3", region_name=region)
    extra: Dict[str, Any] = {"ContentType": content_type, "Metadata": metadata or {}}
    if kms_key_id:
        extra.update({"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": kms_key_id})
    resp = s3.put_object(Bucket=bucket, Key=key, Body=data, **extra)
    return PutResult(
        bucket=bucket,
        key=key,
        etag=resp.get("ETag"),
        version_id=resp.get("VersionId"),
    )

