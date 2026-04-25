# Alerting (SNS)

This repo keeps alerting **small and explicit**: after a DQ run, optionally publish a **single SNS message** summarizing pass/fail and which MUST_PASS rules failed. Message bodies **never include row samples** (PII-safe default).

## Wiring

1. Create an SNS topic (e.g. `mal-pipeline-dq-alerts`) and subscribe email/Slack via AWS Chatbot or an email subscription.
2. Grant the runner role `sns:Publish` on that topic.
3. Configure either:
   - environment variable `MAL_PIPELINE_ALERT_SNS_TOPIC_ARN`, or
   - pass `sns_topic_arn=` into `mal_pipeline.dq.run_dq.run_dq(...)`.

## Behaviour

| `sns_notify` | When a message is sent |
| --- | --- |
| `on_fail` (default) | Only if the aggregate MUST_PASS gate fails |
| `always` | After every run (use sparingly; can be noisy) |

## CloudWatch (recommended add-on)

For production, complement SNS with **metric filters / alarms** on:

- MWAA task failures (orchestrator)
- Lambda / container **Error** metrics
- RDS **FreeStorageSpace** / **CPUUtilization**
- **DLQ depth** for async consumers

Those alarms are account-specific; keep them in Terraform/CDK alongside the SNS topic.
