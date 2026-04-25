# Architecture document

This repository’s **canonical system architecture specification** is a single Markdown file:

## [**docs/architecture.md**](docs/architecture.md)

Use that file as the **architecture document** for reviews, onboarding, and exports. It includes context, end-to-end diagrams (Mermaid), Bronze / Silver / Gold behavior, DQ and marts, Sharia-related data design, orchestration, scaling, and pointers to deeper trade-off analysis.

### Section outline

| § | Topic |
| --- | --- |
| 1 | Problem and requirements |
| 2 | End-to-end architecture |
| 3 | Bronze (S3) |
| 4 | Silver and identity |
| 5 | Gold (decision traceability) |
| 6 | Data quality scorecard |
| 7 | Portfolio monitoring mart |
| 8 | Sharia compliance data model |
| 9 | Degraded decisioning policy |
| 10 | Security, compliance, retention |
| 11 | Orchestration (MWAA) |
| 12 | Scaling plan (10K → 100K decisions/day) |
| 13 | Trade-offs (summary) |
| 14 | Deferred scope vs roadmap |

### PDF export

Pre-built **`exports/pdf/architecture.pdf`** (first 8 pages of `docs/architecture.md`). Other capped PDFs: `plan_30_60_90.pdf`, `tradeoffs_and_product_analysis.pdf`. Regenerate: `./scripts/export_pdfs.sh` (Docker or local xelatex).

### Related specifications

- **AWS deployment topology:** [`docs/aws_infrastructure.md`](docs/aws_infrastructure.md)  
- **Design criteria → evidence map:** [`docs/design_criteria_map.md`](docs/design_criteria_map.md)
