# Product Spec

## Goal
Build an AI ad engine that **creates scroll‑stopping, brand‑safe creative that converts without relying on discounts**.

Andronoma ingests a brand URL, extracts brand DNA, maps motivations/blockers, and outputs ready‑to‑run ads — images, copy, and audiences — engineered to stop scroll and drive sales by value, story, and proof.

## Inputs
- base_url (brand/store URL)
- category (vertical)
- Optional keys (Shopify, Sheets, Meta)

## Core Stages
1. Scrape → brand data, visuals, reviews, competitors, SEO
2. Process → positioning, motivations, blockers, **conversion hypotheses**
3. Audiences → ≥100 testable, mapped to purchase drivers
4. Creatives → ≥50 **value‑led** concepts per audience (no default promos)
5. Images → 1080×1350 stills with baked copy and **tasteful CTA**
6. QA → quotas + coverage + **ad‑readiness**
7. Export → Meta‑ready CSVs, images, QA; optional Sheets/Meta

## Actors
Orchestrator, Scraper, Processor, Audience Generator, Creative Generator, Image Renderer, QA Engine, Exporter, Telemetry.

## User Journeys
Run All · Rerun a stage · Headless API · Webhook on completion.
