# Processing Stage Instructions

## Stage Overview
- Triggered after scrape completes successfully.
- Consumes normalized research payloads to generate positioning intelligence for downstream generation.
- Outputs saved under `/data/processed/{project_id}/processing.json` and summarized in QA reports.

## Core Deliverables
1. **Brand Position Analysis** – articulate category, promise, differentiators, proof pillars, and cost/value framing.
2. **Motivation Map (F/E/A/S)** – map Functional, Emotional, Aspirational, Social motivators with supporting evidence.
3. **Blockers Ranking** – rank objections by frequency × emotional weight; include triggers, personas affected, counter‑angles.
4. **Market Summary** – competitor contrasts, cultural signals, whitespace opportunities, and macro trend notes.
5. **Conversion Hypotheses** – value propositions + proof assets + CTA tone suggestions (from `conversion_hypotheses` module).
6. **Ad Readiness Heuristics** – feed `ad_readiness` module with CTA/value/proof/legibility recommendations.
7. **Brand Fit Score** – tone & aesthetic alignment output (0–100) from `brand_fit_score` module with rationale.

## Craft Requirements
- Every insight must be labeled **Direct** (explicitly observed) vs **Inferred** (derived). Ambiguous cases must surface ≥ 2 interpretations.
- Cite the originating URL/hash for Direct signals; include reasoning chain for Inferred insights.
- Highlight contrast: call out differentiators vs competitors, cultural signals, objection flips, audience gaps, barrier vs catalyst moments, whitespace.
- Personas: cluster by intent/identity; specify segments benefiting from each motivation and the blockers they face.
- Emotional frequency: quantify occurrence (High/Med/Low) to prioritize messaging.

## Data Handling & QA
- Persist intermediate embeddings/vectors needed by NLP modules (topic clustering, sentiment) to enable deterministic reruns.
- Validate schema: ensure required keys exist; log missing data before proceeding.
- Emit telemetry metrics `andronoma_stage_latency_seconds{stage="process"}` and `andronoma_stage_cost_cents` for cost tracking.
- Budget guardrail: halt if estimated spend > 150 cents for the stage.
- Soft timeout: 300 s with graceful cancellation and resumable checkpoints.
