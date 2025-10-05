# Output Generation Requirements

## Stage Sequence & Idempotency
- Execute stages in order: **scrape → process → audiences → creatives → images → qa → export**.
- Use idempotency key `{project_id}:{stage}:{run_tag}` to ensure repeatable runs. On QA failure, stop pipeline and surface remediation notes; do not silently regenerate downstream assets.

## Audience Generation
- Target 120 rows; QA trims to ≥ 100.
- Quotas:
  - Functional ≥ 12
  - Emotional ≥ 12
  - Situational ≥ 12
  - Value/Price ≥ 10
  - Behavioral ≥ 10
  - Psychographic ≥ 10
  - Professional ≥ 8
  - Geo/Logistics ≥ 8
  - Retargeting ≥ 10
  - Edge/Contrarian ≥ 8
  - Intersections ≥ 30
  - Payment/Logistics ≥ 12
  - Time-based ≥ 10
- Seeds: 5–12 terms with ≥ 3 unique tokens per set.
- Blocker binding: ≥ 1 per row, with ≥ 60 rows binding two blockers.
- Retargeting states: PDP no ATC, ATC no checkout, checkout start no purchase, repeat 7d, viewed high-price, viewed small-only, bounced shipping/returns, engaged ads no visit, lapsed 30–90d, OOS viewers.
- Deliverables:
  - `outputs/audiences/audiences_master.csv`
  - Dedupe report (list suppressed or merged rows)
  - Gaps list (quota or persona deficits)

## Creative (Scroll-Stopper) Generation
- Produce ≥ 50 concepts.
- Bucket coverage: Shock, Proof/Engineering, Emotional Story, Absurd/Surreal, Pure Aesthetic — each ≥ 10 concepts.
- Blocker coverage: price, durability, scam/legitimacy, fit/dimension, delivery, style mismatch, returns friction, commitment fear, OOS — each addressed by ≥ 2 concepts.
- Headlines: 3–10 words; no duplicates across entire set. Each concept includes visual directive, angle, audience fit, blocker binding, and proof cue.
- Respect brand-safe tone: no discount or urgency hooks unless `PROMO_ALLOWED=on`.
- Output `outputs/creatives/scroll_stoppers.csv` with schema `#,Headline,Visual,Angle,Blocker,Audience Fit`.

## Image Stills
- For every creative selected for imagery, render 1080×1350 (4:5) still with overlayed text.
- Visual requirements: neutral interior scene, product hero focus, natural lighting, realistic shadows.
- Overlay rules: headline 72–110 px, subcopy 36–48 px, CTA 40 px pill within safe area (72 px margins). Automatic light/dark contrast.
- Naming: `/outputs/creatives/images/concept_##.jpg` matching creative index.

## QA & Export
- QA stage validates quotas, blocker coverage, naming consistency, CTA/value presence, duplicate guard, and image legibility.
- Observability metrics: emit `andronoma_qa_failures_total{kind}` for any violation.
- Export bundle contains `audiences_master.csv`, `scroll_stoppers.csv`, rendered images, QA reports, README mapping; optional Google Sheets mirror (tabs `audiences_master`, `scroll_stoppers`) and Meta CSV template.
- Final bundle accessible via signed S3 URL with short TTL.

## Budgets & Telemetry
- Enforce per-stage budget ceilings (cents): SCRAPE 50 · PROCESS 150 · AUDIENCE 300 · CREATIVE 200 · IMAGE 500.
- Record metrics per stage: `andronoma_stage_latency_seconds`, `andronoma_stage_cost_cents`, `andronoma_tokens_total`, `andronoma_image_renders_total{status}`.
- SLO: full pipeline p95 < 25 minutes; QA first-fix pass rate > 90 %.
