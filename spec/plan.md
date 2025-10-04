# Technical Plan

Services: FastAPI (auth, control, SSE logs, webhooks), Celery workers (stages), Postgres, Redis, MinIO/S3, React+Vite UI, Prometheus+Grafana, OTEL.

APIs:
- LLM: OpenAI primary, Anthropic fallback
- Image: Stability or Replicate (env‑select)
- Research: SERP (optional)
- Commerce: Shopify Admin (optional)
- Export: Google Sheets (optional), Meta (optional v2)

Rate limits: 60 rpm/key; burst 120; X‑RateLimit‑* headers.  
SSE logs: GET /runs/{id}/logs?stream=1.

Security: crawl allowlist; block RFC1918; CORS allowlist; CSRF for UI; API key rotation; audit log; sanitize HTML.

Budgets/SLO: per‑stage budgets; p95 “Run All” < 25 min.  
Retention: S3 lifecycle (raw 90 d, processed 180 d, outputs 365 d); nightly pg backups; orphan purge.

Modules (added for sales focus):
- **conversion_hypotheses**: processor emits value props, proofs, preferred CTAs
- **ad_readiness**: static heuristics for CTA/value/proof/mapping/legibility
- **brand_fit_score**: tone & aesthetic alignment scorer
- **dup_guard**: creative de‑duplication across runs

Flags:
- `PROMO_ALLOWED=off` (default) — unlocks promo CTAs only when on
- `CTA_TONE=default|discreet|direct` — controls CTA lexicon

Spec‑Kit gates: API, DDL, orchestration, QA, exports, security, telemetry.
