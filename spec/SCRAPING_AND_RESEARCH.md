# Scraping & Research Requirements

## Scope
- Stage = **scrape** in orchestration (`scrape → process → audiences → creatives → images → qa → export`).
- Input = project `{project_id, base_url, category, optional_keys}`.
- Output feeds processing layer with raw product, brand, competitor, and SEO data.

## Crawler Policy
- Use **Playwright headless (stealth mode)** with session replay disabled.
- Concurrency: ≤ 2 parallel Playwright contexts per domain.
- Global request budget: ≤ 10 requests per second per domain.
- Respect `robots.txt`; enforce 2 s crawl delay between hits on same host.
- SSRF guard: allow only the submitted base domain + subdomains. Reject RFC1918, localhost, or redirected external hosts.

## Coverage Goals
- Product catalog coverage ≥ 95 % of SKUs accessible from navigation, sitemap, or collection pages.
- Dimensional data (size, weight, length) captured for ≥ 90 % of products; normalize units → metric (cm, kg) and currency → minor units.
- Reviews: capture content, rating, author (if public), timestamp. Deduplicate by review id/hash.
- Brand voice: scrape About/Story/FAQ/Careers/Press pages + tone descriptors.
- Visual descriptors: collect hero images, color palettes, textures.
- Competitors: identify 3–7 relevant brands with price range, shipping model, standout differentiators.
- SEO signals: capture H1/H2, meta title/description, alt text, structured keywords; note frequency weights.
- Trend notes: log seasonal campaigns, collaborations, or timely hooks.

## Data Handling
- Cache responses in Redis for 24 h keyed by URL hash to enable idempotent reruns.
- Persist normalized payload to `/data/raw/research/{project_id}/...` with checksum metadata.
- Annotate each record with source URL, timestamp, and access method (direct crawl vs API).
- Mask or drop PII (emails, phone numbers beyond public support lines).

## Failure & Retry
- Soft timeout for stage: 900 s; implement retry with exponential backoff (max 3) for transient HTTP errors.
- On partial failure, flag gaps in QA notes rather than silently skipping.
- Emit telemetry metrics: `andronoma_stage_latency_seconds{stage="scrape"}`, `andronoma_stage_cost_cents`, `andronoma_tokens_total` (when LLM enrichments occur).
