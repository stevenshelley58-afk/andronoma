# Andronoma Constitution

Enforced by Spec‑Kit: API schemas, DB DDL, stage orchestration, QA rules, export field maps, security, telemetry.  
Excluded from Spec‑Kit: /prompts/**, /outputs/**, /data/raw/research/**.

## Core principles (sales without cheapening)
- **Value‑led conversion**: creative must sell on desire, identity, proof, utility. Discounts are **off by default**.  
- **Brand‑safe tone**: CTAs and claims match brand voice. No hype, no clickbait, no fake urgency.

## Non‑negotiable rules
1) **Processing outputs**: tag *Direct* vs *Inferred*; where ambiguous, include ≥2 interpretations.  
2) **Audiences**: `audiences_master.csv` ≥100 with quotas:  
   Functional ≥12, Emotional ≥12, Situational ≥12, Value/Price ≥10,  
   Behavioral ≥10, Psychographic ≥10, Professional ≥8, Geo/Logistics ≥8,  
   Retargeting ≥10, Edge/Contrarian ≥8; Intersections ≥30.  
   Payment/Logistics ≥12; Time‑based ≥10.  
   Seeds 5–12 (≥3 unique terms/set). Blocker binding ≥1 per row; ≥60 rows bind 2.  
3) **Creatives**: ≥50, buckets each ≥10 → Shock, Proof/Engineering, Emotional Story, Absurd/Surreal, Pure Aesthetic.  
   Cover blocker types ≥2 each (price, durability, scam, fit/dimension, delivery, style mismatch, returns friction, commitment fear, OOS).  
   Concrete visuals; no headline reuse.  
4) **Images**: 4:5 (1080×1350). Neutral interior; product hero; natural light; realistic shadows.  
   Overlay = Headline + Subcopy + CTA. High contrast; safe margins.  
   Overlay rules: safe area 72 px; headline 72–110 px; subcopy 36–48 px; CTA 40 px pill; auto light/dark contrast.  
5) **Scrape coverage**: SKUs ≥95%; dimensions ≥90%; competitors 3–7 with price ranges & shipping model; SEO signals with weights.  
6) **Orchestration**: scrape → process → audiences → creatives → images → qa → export. Stop on QA fail. No silent regeneration.  
7) **Security**: JWT + API keys; SSRF block RFC1918/localhost; crawl allowlist (base + subdomains); signed S3 URLs; sanitize HTML; drop PII.  
8) **Telemetry**: per‑stage traces, metrics, costs; budgets enforced; alerts on breach.  
9) **Definition of Done**: Spec‑Kit check passes; all QA pass; bundle exported.

## Ad‑readiness (brand‑safe conversion)
- CTA present and legible; **discount/promo CTAs disallowed** unless `PROMO_ALLOWED=on`.  
- Value prop present (benefit or proof signal).  
- At least one proof cue where applicable (review/spec/guarantee/origin).  
- Landing intent clear (product/collection).  
- Asset naming maps image → concept → audience.

## Algorithm‑fit heuristics (platform‑friendly)
- Early clarity (subject visible, text legible)  
- High contrast & sparse overlay (anti‑clutter)  
- Hook novelty vs prior concepts (dup guard)  
- Negative feedback risk low (no bait, no shock without relevance)
