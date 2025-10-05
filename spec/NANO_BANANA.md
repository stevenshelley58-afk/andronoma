# NANO BANANA Protocol

> **NANO BANANA = Non-Negotiable Operational Baselines And Necessary Automation.**
>
> Codifies cross-stage guardrails implied by the master spec so every run is safe, observable, and recoverable.

## Run Control
- Pipeline executes sequential stages: scrape → process → audiences → creatives → images → qa → export.
- Use idempotency key `{project_id}:{stage}:{run_tag}` for every task; reruns must reuse the same key to avoid duplicate writes.
- Celery workers run with `concurrency=4`, `acks_late=true`, `prefetch=1`; apply backpressure when queue depth > 100.
- Soft timeouts: scrape 900 s, process 300 s, gen 600 s, images 900 s.
- On stage failure, persist diagnostic bundle and halt downstream work until operator reruns.

## Budget Enforcement
- Stage cost ceilings (cents): SCRAPE 50 · PROCESS 150 · AUDIENCE 300 · CREATIVE 200 · IMAGE 500.
- Track cumulative spend per project; refuse new work when projected overrun > 5 % of budget.
- Record every LLM/image call with prompt hash, token counts, provider, model, latency, and billable cost in `model_runs` table.

## Security & Compliance
- Authentication: API via `X-API-Key`; UI via JWT. Rotate keys regularly and log usage.
- Crawling safety: restrict to submitted base domain/subdomains; block RFC1918, localhost, protocol upgrades that bypass HTTPS.
- Storage: MinIO/S3 objects use signed URLs with short TTL; sanitize HTML/JS before persistence.
- Privacy: drop PII from scrape payloads; respect robots.txt directives.

## Observability
- Metrics to emit:
  - `andronoma_stage_latency_seconds{stage}`
  - `andronoma_stage_cost_cents{stage}`
  - `andronoma_tokens_total{provider,model,stage}`
  - `andronoma_qa_failures_total{kind}`
  - `andronoma_image_renders_total{status}`
- Tracing: one OTEL trace per run; spans per stage with `project_id` attribute.
- Alerts: trigger when SLO breach predicted (Run-All p95 ≥ 25 min or QA first-fix pass < 90 %).

## Data Retention & Backups
- S3 retention: raw 90 d, processed 180 d, outputs 365 d.
- Database backups: nightly `pg_dump` (retain 7 days) + weekly full snapshot (retain 4 weeks).
- Cleanup job purges orphaned S3 keys and expired signed URLs.

## QA & Definition of Done
- Spec-Kit check must pass before release.
- All QA reports must pass with no unresolved blocker coverage gaps.
- Deliverables: `audiences_master.csv` (≥100 rows, quotas satisfied), `scroll_stoppers.csv` (≥50 unique headlines with bucket coverage), rendered images (1080×1350 overlays), export bundle zipped and accessible via signed URL.
- Document rerun steps and attach QA summaries in `/qa_reports/{project_id}`.
