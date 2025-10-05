# Task Batch — Platform Hardening

1. **Scrape stage foundation**
   - Provision the Celery task and FastAPI orchestration endpoint for the scrape stage.
   - Integrate Playwright crawler respecting crawl allowlist, robots.txt, and rate limits; persist raw data to storage.
   - Deliver telemetry counters for scrape latency and cost.

2. **Processing stage implementation**
   - Transform raw scrape artifacts into structured positioning outputs (brand position, motivation map, blockers, market summary).
   - Emit conversion hypotheses (value props, proofs, CTAs) and persist to Postgres.
   - Add OpenAI primary / Anthropic fallback client handling with retry + budget enforcement.

3. **Audience generation stage**
   - Generate ≥120 candidate audiences meeting quota mix; dedupe and trim to ≥100 final rows with blockers bound.
   - Persist `outputs/audiences/audiences_master.csv` and supporting gap/dedupe reports.
   - Record per-stage metrics (latency, cost, token usage).

4. **Creative generation stage**
   - Produce ≥50 scroll-stopper concepts across required buckets and blocker coverage.
   - Guard against duplicates using the `dup_guard` module and write `outputs/creatives/scroll_stoppers.csv`.
   - Surface SSE progress events for frontend console.

5. **Image rendering stage**
   - Integrate Stability/Replicate client for 1080×1350 renders plus overlay compositor enforcing safe areas and typography rules.
   - Save assets under `outputs/creatives/images/` with deterministic naming.
   - Emit observability spans and counters for render success/failure.

6. **QA stage**
   - Implement QA validators (ad readiness, quota checks, dedupe) consuming previous stage outputs.
   - Persist QA reports and expose failure reasons via API.
   - Ensure stage budgets and retry policies are respected.

7. **Export stage**
   - Bundle CSVs, images, and QA summaries into downloadable export packages.
   - Optionally sync Google Sheets when feature flag enabled.
   - Add FastAPI endpoint for export retrieval and audit logging.
