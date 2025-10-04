# ANDRONOMA — MASTER SPEC

## 0) Product in one line
Input: brand URL.  
Output: positioning report, ≥100 testable audiences, ≥50 scroll-stoppers, 1080×1350 stills with baked text, QA reports, and export bundle.

---

## 1) Operating model
Layered workflow: ChatGPT for design/debug, Cursor+Copilot for in-repo edits, Git for control.  
Spec-driven development (selective): enforce only API/DDL/QA/exports/security/telemetry.  
Exclude creative prompts, research notes, and image art direction.  
Single repo with hard QA gates. Idempotent, retryable stages.

---

## 2) Stack
**Backend:** Python 3.11 · FastAPI · Pydantic · SQLAlchemy  
**Workers:** Celery + Redis  
**DB:** Postgres 15  
**Storage:** MinIO/S3  
**Frontend:** React 18 + Vite + TypeScript + Tailwind + shadcn/ui + Zustand  
**Observability:** OpenTelemetry · Prometheus · Grafana  
**Runtime:** Docker Compose (dev) · Kubernetes (prod)

---

## 3) Repo layout
```
/andronoma
  /api                 # FastAPI app, auth, routes, SSE logs
  /workers             # Celery tasks per stage
  /scrape              # crawler, SERP, Shopify client
  /nlp                 # processing & clustering
  /gen                 # audiences & creatives (LLM)
  /image               # image client + overlay compositor
  /qa                  # validators + smoke tests
  /export              # bundles, Sheets, Meta (opt)
  /frontend            # React+Vite app
  /infra               # docker/k8s/terraform
  /prompts             # LLM prompts (excluded from spec checks)
  /spec                # Spec-Kit (enforced)
    constitution.md
    product.spec.md
    plan.md
    tasks.md
  /data/{raw,processed}
  /outputs/{audiences,creatives}
  /qa_reports
  /docs
```

---

## 9) Provider integration
**LLM (Processor / Audience / Creative / QA summaries)**  
Primary = `OPENAI_API_KEY`; Fallback = `ANTHROPIC_API_KEY`.  
Retries ×3 with exponential back-off; timeout 120 s; store prompt hash + tokens + cost in `model_runs`.

**Image (Image Agent)** → `STABILITY_API_KEY` or `REPLICATE_API_TOKEN`  
Photoreal base + overlay (Pillow) with headline / subcopy / CTA legibility.

**SERP (Research Agent, optional)** → `SERP_API_KEY`  

**Shopify Admin (optional)** → `SHOPIFY_STORE_DOMAIN`, `SHOPIFY_ADMIN_TOKEN`  

**Google Sheets (optional)** → writes `audiences_master` and `scroll_stoppers` tabs.  

**Meta Marketing API (optional v2)** → for asset upload / mockups only in v1.

---

## 10) Orchestration
Order: **scrape → process → audiences → creatives → images → qa → export**  
Idempotency key = `{project_id}:{stage}:{run_tag}`  
Celery: `concurrency=4 acks_late=true prefetch=1`  
Soft timeouts: scrape 900 s · process 300 s · gen 600 s · images 900 s  
Backpressure: reject when queue > 100

---

## 11) Scraping + research
Playwright headless (stealth). ≤ 2 concurrent per domain; 10 req/s cap; robots.txt respect; 2 s crawl delay.  
SSRF guard → only base domain + subdomains; block RFC1918/localhost.  
Cache → Redis 24 h by URL hash.  
Normalize → currency→minor units; length→cm; weight→kg.  
Collect → products/specs/variants/price/availability; brand voice pages; visual descriptors; reviews; 3–7 competitors (price range + shipping); SEO H1/H2/meta/alt/keywords; trend notes.

---

## 12) Processing
Outputs: Brand Position Analysis, Motivation Map (F/E/A/S), Blockers Ranking (freq × emotion weight), Market Summary.  
Add contrast, personas, cultural signals, objection flips, audience gaps, emotional frequency, barrier vs catalyst, whitespace.  
Label Direct vs Inferred; ≥ 2 views where ambiguous.

---

## 13) Audience generation
Target 120; QA trims to ≥ 100.  
Quotas: Functional ≥12, Emotional ≥12, Situational ≥12, Value/Price ≥10, Behavioral ≥10, Psychographic ≥10, Professional ≥8, Geo/Logistics ≥8, Retargeting ≥10, Edge/Contrarian ≥8; Intersections ≥30; Payment/Logistics ≥12; Time-based ≥10.  
Each row: Seeds 5–12 (≥ 3 unique terms per set); ≥ 1 blocker bound (≥ 60 rows bind 2).  
Retargeting states: PDP no ATC · ATC no checkout · checkout start no purchase · repeat 7 d · viewed high-price · viewed small-only · bounced shipping/returns · engaged ads no visit · lapsed 30–90 d · OOS viewers.  
Deliver `outputs/audiences/audiences_master.csv` + Dedupe Report + Gaps list.

---

## 14) Creative generation
≥ 50 concepts.  
Buckets ≥ 10 each: Shock · Proof/Engineering · Emotional Story · Absurd/Surreal · Pure Aesthetic.  
Cover blocker types ≥ 2 each (price · durability · scam · fit/dimension · delivery · style mismatch · returns · commitment fear · OOS).  
Concrete visuals; headlines 3–10 words; no repeats.  
Deliver `outputs/creatives/scroll_stoppers.csv`.

---

## 15) Image stills
1080×1350 (4:5) · neutral interior · product hero · natural light · realistic shadows.  
Overlay → Headline · Subcopy · CTA · high contrast · safe margins.  
Rules: safe area 72 px · headline 72–110 px · subcopy 36–48 px · CTA 40 px pill · auto contrast.  
Save → `/outputs/creatives/images/concept_##.jpg`.

---

## 17) Frontend spec
React + Vite + TypeScript · Tailwind · shadcn/ui · Zustand.  
Screens: Wizard (name · URL · category · keys) · Run Console (SSE logs) · Insights (positioning · motivations · blockers) · Audiences (table + quota meters) · Creatives (grid + bucket filters) · Images (gallery + legibility overlay) · QA reports · Export (CSV / Sheet / Zip).  
Accessibility → WCAG AA; keyboard nav; text scale 1.2 ×.

---

## 18) Security
Auth: `X-API-Key` (API) · JWT (UI). Rate limit 60 rpm · burst 120.  
Crawl SSRF guard → base domain only; block private IPs; deny redirects to others.  
CORS allowlist · CSRF for UI · API key rotation · audit logs.  
Sanitize HTML/JS before DB write. Signed S3 URLs with short TTL.

---

## 19) Observability / budgets / SLOs
Metrics:  
- `andronoma_stage_latency_seconds{stage}`  
- `andronoma_stage_cost_cents{stage}`  
- `andronoma_tokens_total{provider,model,stage}`  
- `andronoma_qa_failures_total{kind}`  
- `andronoma_image_renders_total{status}`  

Traces: one trace per run; span per stage; include project_id.  
Budgets (cents): SCRAPE 50 · PROCESS 150 · AUDIENCE 300 · CREATIVE 200 · IMAGE 500.  
SLO: Run-All p95 < 25 min; first-fix QA pass > 90 %.

---

## 20) Exports
**audiences_master.csv** columns  
`#,Audience Name,Who They Are,Seed Terms,Primary Motivation,Top 2 Blockers,Message Angle,Creative Concept,Format Notes,Proof/Offer,Success Metric,A/B Variable,Exclusions`  

**scroll_stoppers.csv** columns  
`#,Headline,Visual,Angle,Blocker,Audience Fit`  

Bundle = CSVs + images + QA reports + README mapping.  
Optional = Google Sheet mirror + Meta CSV template.

---

## 21) CI/CD summary
GitHub Actions → lint · type · security · Spec-Kit · QA smoke.  
Pre-commit hooks → Black · Ruff · Spec-Kit check.  
See `.github/workflows/ci.yml` and `.pre-commit-config.yaml`.

---

## 23) Feature flags
`FEATURE_SHOPIFY, FEATURE_SHEETS, FEATURE_META, FEATURE_HEADLESS, SPEC_ENFORCE_STRICT`

---

## 24) Retention and backups
S3 → raw 90 d, processed 180 d, outputs 365 d.  
DB → nightly pg_dump 7 d; weekly full 4 w.  
Cleanup → purge orphan S3 keys on delete.

---

## 25) Testing plan
Fixtures = demo site snapshot + golden CSVs/QA reports.  
Unit → parsers, mappers, validators, overlay fitter.  
Integration → E2E fixture · all QA pass.  
Load → 10 parallel projects · queue < 50 · no timeouts.  
Security → SSRF · rate-limit · CORS · API key misuse.

---

## 26) Start commands
```bash
cp .env.example .env
pip install 'git+https://github.com/github/spec-kit.git'
specify init andronoma-spec
specify check
docker compose up -d

# create project
curl -s -X POST http://localhost:8080/projects -H 'X-API-Key: dev' \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","base_url":"https://example.com","category":"home"}'

# run all
curl -s -X POST http://localhost:8080/projects/{project_uuid}/run \
  -H 'X-API-Key: dev' \
  -d '{"stages":["scrape","process","audiences","creatives","images","qa","export"]}'
```

---

## 27) Definition of done
- Spec-Kit check passes  
- All QA reports pass  
- `audiences_master.csv` ≥ 100 with quotas + appendices  
- `scroll_stoppers.csv` ≥ 50 with bucket coverage + no dup headlines  
- Images 1080×1350 · legible overlays  
- `bundle.zip` downloadable via signed URL

---

## 28) Risks and controls
Scrape gaps → sitemap + retries + manual seeds  
Creative sameness → bucket quotas + dup headline guard  
Spec churn → limit Spec-Kit to deterministic layers  
Cost spikes → per-stage budgets + alerts  
Non-idempotent reruns → idempotency keys + atomic writes
