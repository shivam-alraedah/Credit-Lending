from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import boto3


def publish_sns_message(
    *,
    topic_arn: str,
    subject: str,
    message: str,
    sns_client: Optional[Any] = None,
) -> str:
    """
    Publish a plain-text message to an SNS topic.

    `sns_client` is injectable for tests; when omitted, a default client is
    created using the ambient AWS credential chain and AWS_REGION/AWS_DEFAULT_REGION.
    """
    client = sns_client or boto3.client(
        "sns",
        region_name=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
    )
    resp = client.publish(
        TopicArn=topic_arn,
        Subject=subject[:100],
        Message=message,
    )
    return str(resp["MessageId"])


def format_dq_run_alert(
    *,
    dq_run_id: str,
    must_pass: bool,
    failed_must_pass_rule_ids: list[str],
    scope: Optional[Dict[str, Any]] = None,
) -> tuple[str, str]:
    """Return (subject, body) for a DQ run notification. No row-level payloads."""
    status = "PASS" if must_pass else "FAIL"
    subject = f"[Mal pipeline] DQ run {status} ({dq_run_id[:8]}…)"
    body_lines = [
        f"dq_run_id: {dq_run_id}",
        f"must_pass_aggregate: {must_pass}",
    ]
    if scope:
        body_lines.append(f"scope: {json.dumps(scope, sort_keys=True)}")
    if failed_must_pass_rule_ids:
        body_lines.append("failed_must_pass_rules:")
        body_lines.extend(f"  - {rid}" for rid in failed_must_pass_rule_ids)
    else:
        body_lines.append("failed_must_pass_rules: []")
    body_lines.append("")
    body_lines.append("Note: row-level payloads are intentionally omitted from alerts.")
    return subject, "\n".join(body_lines)
