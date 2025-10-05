# Scroll-Stoppers Playbook

## Purpose
Define requirements for the creative generation stage responsible for producing scroll-stopping ad concepts that sell through value, story, and proof without defaulting to discounts.

## Volume & Coverage
- Minimum concepts: **50** per run.
- Bucket quotas (each ≥ 10):
  1. Shock
  2. Proof / Engineering
  3. Emotional Story
  4. Absurd / Surreal
  5. Pure Aesthetic
- Blocker coverage: address price, durability, scam/legitimacy, fit/dimension, delivery, style mismatch, returns friction, commitment fear, out-of-stock. Each blocker must be resolved by ≥ 2 concepts.
- Audience alignment: each concept maps to at least one audience row and reflects its dominant motivation + blockers.

## Concept Structure
Every row in `scroll_stoppers.csv` must include:
- `#` – stable identifier synced with pipeline ordering.
- `Headline` – 3–10 words, unique across the run, brand-safe, and value-led. No fake urgency or discounting unless `PROMO_ALLOWED=on`.
- `Visual` – concrete art direction describing subject, setting, composition, and styling cues.
- `Angle` – strategic hook referencing motivation, proof, or cultural signal.
- `Blocker` – the primary objection being flipped; if concept covers two blockers, note both.
- `Audience Fit` – pointer to the matching audience segment identifier.

## Craft Guardrails
- Use concrete sensory language and proof cues (reviews, specs, certifications, origin stories) aligned with processing outputs.
- Ensure CTA tone matches brand voice; escalate to QA if ad_readiness heuristics flag missing CTA/value/proof.
- Run duplicate guard: reject identical or near-identical headlines, angles, or visual directions (cosine similarity threshold defined in `dup_guard`).
- Ensure creative mix includes cultural signals and whitespace opportunities surfaced by processing stage.
- Avoid claims that would trip policy (no medical, financial, or unverifiable superlatives) unless verified.

## Delivery & QA Hooks
- Save CSV to `outputs/creatives/scroll_stoppers.csv` with UTF-8 encoding.
- Emit metadata manifest summarizing bucket counts, blocker coverage, duplicate guard stats.
- Forward pass/fail flags to QA stage so violations increment `andronoma_qa_failures_total{kind="creative"}`.
- Surface suggestions for imagery handoff (headline variant, overlay guidance, CTA tone) for each concept selected for rendering.
