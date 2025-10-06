"""NLP enrichment stage for the Andronoma pipeline."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import time
import uuid
from collections import Counter, OrderedDict
from pathlib import Path
from itertools import cycle
from typing import Any, Dict, List, Mapping, Sequence, Tuple
from urllib.parse import urlparse

from sqlalchemy import select

from shared.logs import emit_log
from shared.models import AssetRecord
from shared.stages.base import BaseStage

from . import ad_readiness, brand_fit, conversion_hypotheses
from outputs.csv import write_records
from qa.validators import REQUIRED_AUDIENCE_COLUMNS, validate_audience_quotas


class ProcessStage(BaseStage):
    """Transform scraped research into positioning intelligence."""

    name = "process"

    COST_CEILING_CENTS = 150.0
    COST_PER_TOKEN_CENTS = 0.002
    SOFT_TIMEOUT_SECONDS = 300.0

    STOPWORDS: frozenset[str] = frozenset(
        {
            "the",
            "and",
            "a",
            "an",
            "is",
            "to",
            "of",
            "in",
            "for",
            "on",
            "with",
            "as",
            "by",
            "at",
            "from",
            "that",
            "this",
            "it",
            "be",
            "are",
            "or",
            "we",
            "you",
            "our",
            "their",
            "your",
            "they",
        }
    )

    def execute(self) -> Dict[str, Any]:
        start = time.monotonic()
        # Budget check is performed in dollars; convert the cents ceiling accordingly.
        self.ensure_budget(self.COST_CEILING_CENTS / 100.0)

        run = self.context.run
        session = self.context.session

        emit_log(session, run.id, "Loading research corpus for processing")
        documents = self._load_corpus()
        if not documents:
            raise ValueError("No research documents available for processing stage")

        prepared_docs = self._prepare_documents(documents)
        total_tokens = sum(doc["token_count"] for doc in prepared_docs)
        projected_cost = total_tokens * self.COST_PER_TOKEN_CENTS
        if projected_cost > self.COST_CEILING_CENTS:
            raise ValueError(
                f"Projected spend {projected_cost:.2f}¢ exceeds stage ceiling "
                f"{self.COST_CEILING_CENTS:.0f}¢"
            )

        emit_log(
            session,
            run.id,
            "Corpus prepared",
            metadata={"documents": len(prepared_docs), "tokens": total_tokens},
        )

        global_terms, per_doc_terms = self._compute_term_frequencies(prepared_docs)
        brand_position = self._build_brand_position(prepared_docs, global_terms)
        motivation_map = self._build_motivation_map(prepared_docs)
        blockers_ranking = self._rank_blockers(prepared_docs)
        market_summary = self._build_market_summary(
            prepared_docs, blockers_ranking, brand_position
        )

        conversions = conversion_hypotheses.generate_conversion_hypotheses(
            prepared_docs, brand_position, motivation_map, blockers_ranking
        )
        readiness = ad_readiness.evaluate_ad_readiness(
            prepared_docs,
            brand_position,
            motivation_map,
            blockers_ranking,
            conversions,
        )
        fit = brand_fit.score_brand_fit(
            prepared_docs,
            run.input_payload,
            brand_position,
            motivation_map,
        )

        generated_at = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        result_bundle = {
            "run_id": str(run.id),
            "generated_at": generated_at.isoformat(),
            "source_documents": [
                {
                    "id": doc["id"],
                    "url": doc.get("url"),
                    "title": doc.get("title"),
                    "hash": doc["hash"],
                    "word_count": doc["word_count"],
                }
                for doc in prepared_docs
            ],
            "artifacts": {
                "brand_position": brand_position,
                "motivation_map": motivation_map,
                "blockers_ranking": blockers_ranking,
                "market_summary": market_summary,
                "conversion_hypotheses": conversions,
                "ad_readiness": readiness,
                "brand_fit": fit,
            },
            "intermediate": {
                "term_frequencies": global_terms,
                "document_vectors": per_doc_terms,
            },
        }

        output_path = self._persist_results(run.id, result_bundle)

        latency = time.monotonic() - start

        diagnostics, deficits = self._compile_diagnostics(result_bundle)
        if latency > self.SOFT_TIMEOUT_SECONDS:
            diagnostics["timeout_exceeded"] = True
            deficits = list(deficits) + [
                "Processing latency exceeded soft timeout threshold; downstream stages should validate completeness.",
            ]
        else:
            diagnostics["timeout_exceeded"] = False

        telemetry = {
            "documents": len(prepared_docs),
            "unique_sources": len({doc.get("url") or doc["hash"] for doc in prepared_docs}),
            "andronoma_stage_latency_seconds": round(latency, 3),
            "andronoma_stage_cost_cents": round(projected_cost, 2),
            "tokens_consumed": total_tokens,
            "stage_ceiling_cents": self.COST_CEILING_CENTS,
            "output_path": str(output_path),
            "diagnostics": diagnostics,
            "deficits": deficits,
        }

        emit_log(
            session,
            run.id,
            "Processing artifacts generated",
            metadata={
                "output_path": str(output_path),
                "direct_insights": diagnostics.get("direct_insights", 0),
                "inferred_insights": diagnostics.get("inferred_insights", 0),
            },
        )

        return telemetry

    # ------------------------------------------------------------------
    # Corpus loading utilities
    # ------------------------------------------------------------------
    def _load_corpus(self) -> List[Dict[str, Any]]:
        session = self.context.session
        run_id = self.context.run.id

        stmt = (
            select(AssetRecord)
            .where(AssetRecord.run_id == run_id, AssetRecord.stage == "scrape")
            .order_by(AssetRecord.created_at.desc())
        )
        record = session.execute(stmt).scalars().first()

        documents: List[Dict[str, Any]] | None = None
        storage_key = None
        if record:
            storage_key = record.storage_key
            documents = self._load_from_storage(storage_key)

        if documents is None:
            documents = self._load_from_local()

        if documents is None:
            raise FileNotFoundError(
                "Unable to locate research payload in storage or local filesystem"
            )

        if not isinstance(documents, list):
            if isinstance(documents, dict) and "documents" in documents:
                documents = documents["documents"]  # type: ignore[assignment]
            else:
                raise TypeError("Research payload must be a list of documents")

        emit_log(
            self.context.session,
            run_id,
            "Research corpus loaded",
            metadata={"documents": len(documents), "storage_key": storage_key},
        )

        return documents

    def _load_from_storage(self, storage_key: str | None) -> List[Dict[str, Any]] | None:
        if not storage_key:
            return None
        parsed = urlparse(storage_key)
        if parsed.scheme not in {"s3", "minio"}:
            return self._load_from_path(storage_key)

        try:
            from shared.storage import client, settings

            bucket = parsed.netloc or settings.minio_bucket
            object_name = parsed.path.lstrip("/")
            response = client.get_object(bucket, object_name)
            try:
                payload = response.read()
            finally:
                response.close()
                response.release_conn()
        except Exception:  # pragma: no cover - network failures
            return None

        try:
            return json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError:  # pragma: no cover - corrupt payloads
            return None

    def _load_from_path(self, path_like: str) -> List[Dict[str, Any]] | None:
        parsed = urlparse(path_like)
        if parsed.scheme == "file":
            path = Path(parsed.path)
        else:
            path = Path(path_like)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

    def _load_from_local(self) -> List[Dict[str, Any]] | None:
        run_id = str(self.context.run.id)
        base = Path("/data/raw/research")
        candidates = [
            base / f"{run_id}.json",
            base / run_id / "corpus.json",
            base / run_id / "documents.json",
        ]
        for candidate in candidates:
            data = self._load_from_path(str(candidate))
            if data is not None:
                return data
        return None

    # ------------------------------------------------------------------
    # Document preparation
    # ------------------------------------------------------------------
    def _prepare_documents(
        self, documents: Sequence[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        prepared: List[Dict[str, Any]] = []
        for idx, doc in enumerate(documents):
            body = str(doc.get("body") or doc.get("content") or "")
            title = str(doc.get("title") or "").strip()
            url = doc.get("url") or doc.get("source")
            merged_text = f"{title}. {body}" if title else body
            sentences = [
                sentence.strip()
                for sentence in re.split(r"(?<=[.!?])\s+", merged_text)
                if sentence.strip()
            ]
            tokens = self._tokenize(merged_text)
            doc_hash = hashlib.sha1((str(url) + merged_text).encode("utf-8")).hexdigest()[:12]
            prepared.append(
                {
                    "id": doc.get("id") or f"doc-{idx+1}",
                    "url": url,
                    "title": title,
                    "body": body,
                    "sentences": sentences,
                    "token_count": len(tokens),
                    "word_count": len(tokens),
                    "hash": doc_hash,
                }
            )
        return prepared

    def _tokenize(self, text: str) -> List[str]:
        tokens = re.findall(r"\b\w+\b", text.lower())
        return [token for token in tokens if token not in self.STOPWORDS]

    def _compute_term_frequencies(
        self, documents: Sequence[Dict[str, Any]]
    ) -> Tuple[List[Tuple[str, int]], Dict[str, Dict[str, int]]]:
        global_counter: Counter[str] = Counter()
        per_doc: Dict[str, Dict[str, int]] = {}
        for doc in documents:
            tokens = self._tokenize(doc.get("body", ""))
            counter = Counter(tokens)
            global_counter.update(counter)
            per_doc[doc["id"]] = dict(counter)
        return global_counter.most_common(50), per_doc

    # ------------------------------------------------------------------
    # Artifact builders
    # ------------------------------------------------------------------
    def _build_brand_position(
        self,
        documents: Sequence[Dict[str, Any]],
        global_terms: Sequence[Tuple[str, int]],
    ) -> Dict[str, Any]:
        config = self.context.run.input_payload.get("config", {}) if self.context.run else {}
        brand_name = config.get("name") or "the brand"
        target_markets: List[str] = config.get("target_markets", [])

        top_terms = [term for term, _ in global_terms if term]
        category_term = (target_markets[0] if target_markets else None) or (top_terms[0] if top_terms else "market")
        category_sources = self._sentences_with_keyword(documents, category_term)
        category_type = "Direct" if category_sources else "Inferred"
        category_sources = category_sources or self._fallback_sources_from_terms(documents, top_terms[:2])

        promise_term = top_terms[1] if len(top_terms) > 1 else category_term
        promise_sources = self._sentences_with_keyword(documents, promise_term)
        promise_type = "Direct" if promise_sources else "Inferred"

        differentiators = []
        for term in top_terms[:5]:
            sources = self._sentences_with_keyword(documents, term)
            if not sources:
                continue
            differentiators.append(
                {
                    "statement": f"{brand_name} emphasises {term}",
                    "type": "Direct",
                    "sources": sources,
                }
            )
        if not differentiators and top_terms:
            differentiators.append(
                {
                    "statement": f"{brand_name} differentiates through {top_terms[0]}",
                    "type": "Inferred",
                    "sources": self._fallback_sources_from_terms(documents, top_terms[:1]),
                    "reasoning": "Dominant keyword frequency suggests emphasis despite limited direct articulation.",
                }
            )

        proof_terms = {"evidence", "data", "results", "case", "study", "roi", "customers"}
        proof_pillars = []
        for doc in documents:
            for sentence in doc.get("sentences", []):
                if any(term in sentence.lower() for term in proof_terms):
                    proof_pillars.append(
                        {
                            "statement": sentence,
                            "type": "Direct",
                            "sources": [self._source_entry(doc, sentence)],
                        }
                    )
        if not proof_pillars:
            proof_pillars.append(
                {
                    "statement": f"{brand_name} relies on qualitative testimonials due to limited quantitative proof.",
                    "type": "Inferred",
                    "sources": self._fallback_sources_from_terms(documents, top_terms[:2]),
                    "reasoning": "Research corpus lacks explicit metrics; emphasis inferred from thematic density.",
                }
            )

        value_keywords = {
            "premium": "premium",
            "luxury": "premium",
            "affordable": "value",
            "budget": "value",
            "cost": "value",
            "price": "value",
            "balanced": "balanced",
            "mid": "balanced",
        }
        value_hits: Dict[str, List[Dict[str, str]]] = {"premium": [], "value": [], "balanced": []}
        for doc in documents:
            for sentence in doc.get("sentences", []):
                lower = sentence.lower()
                for keyword, bucket in value_keywords.items():
                    if keyword in lower:
                        value_hits[bucket].append(self._source_entry(doc, sentence))

        if value_hits["premium"] and value_hits["value"]:
            framing_type = "Inferred"
            framing_sources = value_hits["premium"][:1] + value_hits["value"][:1]
            interpretations = [
                {
                    "label": "Premium",
                    "type": "Direct",
                    "sources": value_hits["premium"][:2],
                },
                {
                    "label": "Value",
                    "type": "Direct",
                    "sources": value_hits["value"][:2],
                },
            ]
        elif value_hits["premium"]:
            framing_type = "Direct"
            framing_sources = value_hits["premium"]
            interpretations = []
        elif value_hits["value"]:
            framing_type = "Direct"
            framing_sources = value_hits["value"]
            interpretations = []
        else:
            framing_type = "Inferred"
            framing_sources = self._fallback_sources_from_terms(documents, top_terms[:2])
            interpretations = [
                {
                    "label": "Balanced",
                    "type": "Inferred",
                    "sources": framing_sources,
                    "reasoning": "No explicit price cues; assume balanced messaging emphasising value proof.",
                }
            ]

        value_framing = {
            "statement": f"{brand_name} positions pricing as {framing_sources[0]['evidence'] if framing_sources else 'balanced value'}",
            "type": framing_type,
            "sources": framing_sources,
            "interpretations": interpretations,
        }

        return {
            "category": {
                "statement": f"{brand_name} competes within {category_term}",
                "type": category_type,
                "sources": category_sources,
            },
            "promise": {
                "statement": f"Delivers {promise_term} outcomes for target customers",
                "type": promise_type,
                "sources": promise_sources
                or self._fallback_sources_from_terms(documents, top_terms[:2]),
            },
            "differentiators": differentiators,
            "proof_pillars": proof_pillars,
            "value_framing": value_framing,
        }

    def _build_motivation_map(
        self, documents: Sequence[Dict[str, Any]]
    ) -> Dict[str, Any]:
        keyword_map = {
            "functional": {
                "keywords": [
                    "efficiency",
                    "automation",
                    "integration",
                    "performance",
                    "workflow",
                    "data",
                ],
            },
            "emotional": {
                "keywords": [
                    "confidence",
                    "trust",
                    "stress",
                    "frustration",
                    "feel",
                    "fear",
                ],
            },
            "aspirational": {
                "keywords": [
                    "growth",
                    "scale",
                    "vision",
                    "future",
                    "lead",
                    "innov",
                ],
            },
            "social": {
                "keywords": [
                    "community",
                    "team",
                    "collaboration",
                    "advocate",
                    "share",
                    "peer",
                ],
            },
        }

        personas = self.context.run.input_payload.get("config", {}).get("target_markets", [])

        motivation_map: Dict[str, Any] = {}
        for motiv, config in keyword_map.items():
            hits: List[Dict[str, Any]] = []
            for doc in documents:
                for sentence in doc.get("sentences", []):
                    lower = sentence.lower()
                    if any(keyword in lower for keyword in config["keywords"]):
                        hits.append(
                            {
                                "statement": sentence,
                                "type": "Direct",
                                "sources": [self._source_entry(doc, sentence)],
                                "personas": personas[:3],
                                "emotion_frequency": "High",
                            }
                        )
            if not hits:
                fallback_sources = self._fallback_sources_from_terms(
                    documents, config["keywords"][:2]
                )
                hits.append(
                    {
                        "statement": f"Motivator inferred from thematic cues around {', '.join(config['keywords'][:2])}",
                        "type": "Inferred",
                        "sources": fallback_sources,
                        "personas": personas[:3],
                        "emotion_frequency": "Medium",
                        "reasoning": "Keywords absent verbatim; inference based on related topic density.",
                    }
                )

            motivation_map[motiv] = {
                "intensity": self._motivation_intensity(hits),
                "insights": hits,
            }

        return motivation_map

    def _motivation_intensity(self, hits: Sequence[Dict[str, Any]]) -> str:
        direct = sum(1 for hit in hits if hit["type"] == "Direct")
        if direct > 3:
            return "High"
        if direct > 1:
            return "Medium"
        return "Low"

    def _rank_blockers(self, documents: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        blocker_terms = {
            "cost": ["cost", "price", "budget", "expensive"],
            "trust": ["trust", "risk", "concern", "uncertain"],
            "time": ["time", "delay", "slow", "wait"],
            "integration": ["integrat", "compat", "system", "stack"],
            "evidence": ["proof", "evidence", "roi", "case", "results"],
        }

        blockers: Dict[str, Dict[str, Any]] = {}
        for doc in documents:
            for sentence in doc.get("sentences", []):
                lower = sentence.lower()
                for blocker, keywords in blocker_terms.items():
                    if any(keyword in lower for keyword in keywords):
                        record = blockers.setdefault(
                            blocker,
                            {
                                "blocker": blocker,
                                "mentions": 0,
                                "type": "Direct",
                                "sources": [],
                                "reasoning": "",
                                "personas": self.context.run.input_payload.get("config", {}).get(
                                    "target_markets", []
                                )[:3],
                            },
                        )
                        record["mentions"] += 1
                        record["sources"].append(self._source_entry(doc, sentence))

        if not blockers:
            inferred_sources = self._fallback_sources_from_terms(documents, ["risk", "cost"])
            blockers["uncertainty"] = {
                "blocker": "uncertainty",
                "mentions": 1,
                "type": "Inferred",
                "sources": inferred_sources,
                "reasoning": "General market research lacked explicit objections; defaulting to uncertainty.",
                "personas": self.context.run.input_payload.get("config", {}).get("target_markets", [])[:3],
            }

        ranked = sorted(blockers.values(), key=lambda item: item["mentions"], reverse=True)
        output: List[Dict[str, Any]] = []
        for idx, record in enumerate(ranked, start=1):
            emotional_weight = self._emotional_weight(record["sources"])
            output.append(
                {
                    "rank": idx,
                    "blocker": record["blocker"],
                    "frequency": "High" if record["mentions"] > 3 else "Medium" if record["mentions"] > 1 else "Low",
                    "mentions": record["mentions"],
                    "emotional_weight": emotional_weight,
                    "type": record["type"],
                    "sources": record["sources"],
                    "reasoning": record.get("reasoning", ""),
                    "personas": record.get("personas", []),
                }
            )
        return output

    def _emotional_weight(self, sources: Sequence[Dict[str, str]]) -> str:
        keywords = {"fear", "risk", "trust", "stress"}
        hits = 0
        for source in sources:
            if any(keyword in source["evidence"].lower() for keyword in keywords):
                hits += 1
        if hits > 2:
            return "High"
        if hits:
            return "Medium"
        return "Low"

    def _build_market_summary(
        self,
        documents: Sequence[Dict[str, Any]],
        blockers: Sequence[Dict[str, Any]],
        brand_position: Dict[str, Any],
    ) -> Dict[str, Any]:
        competitor_signals: List[Dict[str, Any]] = []
        cultural_signals: List[Dict[str, Any]] = []
        macro_trends: List[Dict[str, Any]] = []
        whitespace: List[Dict[str, Any]] = []

        competitor_keywords = {"vs", "versus", "alternative", "competitor", "compared"}
        culture_keywords = {"trend", "culture", "community", "movement", "signal"}
        macro_keywords = {"market", "industry", "growth", "demand", "trend", "regulation"}

        for doc in documents:
            for sentence in doc.get("sentences", []):
                lower = sentence.lower()
                if any(keyword in lower for keyword in competitor_keywords):
                    competitor_signals.append(
                        {
                            "statement": sentence,
                            "type": "Direct",
                            "sources": [self._source_entry(doc, sentence)],
                        }
                    )
                if any(keyword in lower for keyword in culture_keywords):
                    cultural_signals.append(
                        {
                            "statement": sentence,
                            "type": "Direct",
                            "sources": [self._source_entry(doc, sentence)],
                        }
                    )
                if any(keyword in lower for keyword in macro_keywords):
                    macro_trends.append(
                        {
                            "statement": sentence,
                            "type": "Direct",
                            "sources": [self._source_entry(doc, sentence)],
                        }
                    )

        if not competitor_signals:
            competitor_signals.append(
                {
                    "statement": "Competitor positioning inferred from lack of explicit comparisons; emphasize whitespace messaging.",
                    "type": "Inferred",
                    "sources": self._fallback_sources_from_terms(documents, ["unique", "only"]),
                    "reasoning": "Absence of competitor mentions suggests opportunity to define category narrative.",
                }
            )

        if not cultural_signals:
            cultural_signals.append(
                {
                    "statement": "Cultural conversation inferred around community-driven adoption cues.",
                    "type": "Inferred",
                    "sources": self._fallback_sources_from_terms(documents, ["community", "advocate"]),
                    "reasoning": "Keywords indicate latent cultural energy despite limited direct references.",
                }
            )

        if not macro_trends:
            macro_trends.append(
                {
                    "statement": "Macro environment inferred as growth-oriented with regulatory watchpoints.",
                    "type": "Inferred",
                    "sources": self._fallback_sources_from_terms(documents, ["growth", "market"]),
                    "reasoning": "General market cues extracted from thematic frequency counts.",
                }
            )

        if blockers:
            top_blocker = blockers[0]
            whitespace.append(
                {
                    "statement": f"Address {top_blocker['blocker']} blocker to unlock whitespace.",
                    "type": "Inferred" if top_blocker["type"] == "Inferred" else "Direct",
                    "sources": top_blocker["sources"][:2],
                    "reasoning": "Highest ranked blocker reveals unmet demand and messaging whitespace.",
                }
            )

        if not whitespace:
            proof_pillars = brand_position.get("proof_pillars", [])
            pillar_sources: List[Dict[str, str]] = []
            if proof_pillars:
                pillar_sources = proof_pillars[0].get("sources", [])
            if not pillar_sources:
                pillar_sources = self._fallback_sources_from_terms(documents, ["proof", "evidence"])
            whitespace.append(
                {
                    "statement": "Whitespace derived from differentiated proof pillars in brand positioning.",
                    "type": "Inferred",
                    "sources": pillar_sources,
                    "reasoning": "Reusing proof pillar evidence to frame opportunity gaps.",
                }
            )

        return {
            "competitor_contrast": competitor_signals,
            "cultural_signals": cultural_signals,
            "whitespace_opportunities": whitespace,
            "macro_trends": macro_trends,
        }

    # ------------------------------------------------------------------
    # Diagnostics helpers
    # ------------------------------------------------------------------
    def _compile_diagnostics(self, results: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        insights = self._collect_insights(results)
        direct = sum(1 for insight in insights if insight.get("type") == "Direct")
        inferred = sum(1 for insight in insights if insight.get("type") == "Inferred")
        source_gaps = sum(
            1
            for insight in insights
            if not insight.get("sources")
            or not all(source.get("source") or source.get("hash") for source in insight.get("sources", []))
        )

        diagnostics = {
            "direct_insights": direct,
            "inferred_insights": inferred,
            "insights_without_sources": source_gaps,
        }

        deficits: List[str] = []
        if direct == 0:
            deficits.append("No direct insights detected; messaging may lack evidence.")
        if source_gaps:
            deficits.append("Some insights missing explicit source references.")

        return diagnostics, deficits

    def _collect_insights(self, data: Any) -> List[Dict[str, Any]]:
        stack: List[Any] = [data]
        insights: List[Dict[str, Any]] = []
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                if {"type", "sources"}.issubset(item.keys()):
                    insights.append(item)
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)
        return insights

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------
    def _persist_results(self, run_id: Any, payload: Dict[str, Any]) -> Path:
        run_dir = Path("/data/processed") / str(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        output_path = run_dir / "processing.json"
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return output_path

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    def _sentences_with_keyword(
        self, documents: Sequence[Dict[str, Any]], keyword: str | None
    ) -> List[Dict[str, str]]:
        if not keyword:
            return []
        keyword = keyword.lower()
        matches: List[Dict[str, str]] = []
        for doc in documents:
            for sentence in doc.get("sentences", []):
                if keyword in sentence.lower():
                    matches.append(self._source_entry(doc, sentence))
        return matches

    def _fallback_sources_from_terms(
        self, documents: Sequence[Dict[str, Any]], terms: Sequence[str]
    ) -> List[Dict[str, str]]:
        matches: List[Dict[str, str]] = []
        for term in terms:
            matches.extend(self._sentences_with_keyword(documents, term))
        if matches:
            return matches[:3]
        if documents:
            doc = documents[0]
            sentence = doc.get("sentences", [doc.get("body", "")])[0]
            return [self._source_entry(doc, sentence)]
        return []

    def _source_entry(self, doc: Dict[str, Any], sentence: str) -> Dict[str, str]:
        return {
            "source": doc.get("url") or doc["id"],
            "hash": doc["hash"],
            "evidence": sentence.strip(),
        }


class AudienceStage(BaseStage):
    name = "audiences"

    MIN_RECORDS = 100
    TARGET_RECORDS = 140
    GAP_THRESHOLD_MOTIVATION = 0.45
    GAP_THRESHOLD_BLOCKER = 0.35
    OUTPUT_PATH = Path("outputs/audiences/audiences_master.csv")
    PROCESSED_ROOT = Path("/data/processed")

    DEFAULT_PERSONAS = [
        "Growth-minded operators",
        "Marketing leadership",
        "Lifecycle strategists",
        "Acquisition specialists",
    ]
    FORMAT_NOTES = [
        "Meta Advantage+ with lookalike expansion",
        "TikTok Spark Ads highlighting community proof",
        "Meta carousel sequencing blocker flips",
        "Short-form UGC with testimonial stitch",
        "LinkedIn thought leadership carousel",
    ]
    SUCCESS_METRICS = [
        "Qualified lead volume",
        "Add-to-cart conversion",
        "Cost per booked demo",
        "Subscriber growth",
        "Return on ad spend",
    ]
    AB_VARIANTS = [
        "Hook positioning",
        "CTA framing",
        "Offer depth",
        "Visual treatment",
        "Social proof order",
    ]
    EXCLUSION_RULES = [
        "Exclude existing customers",
        "Exclude current partners",
        "Exclude employee lists",
        "Exclude low LTV cohorts",
        "Exclude recent purchasers",
    ]

    def execute(self) -> Dict[str, Any]:
        start = time.monotonic()
        self.ensure_budget(30.0)

        run = self.context.run
        session = self.context.session

        emit_log(
            session,
            run.id,
            "Loading processed positioning outputs for audience synthesis",
        )
        processed_payload = self._load_processed_payload(run)
        artifacts = processed_payload.get("artifacts", {})

        emit_log(
            session,
            run.id,
            "Synthesising quota-compliant audience records",
        )
        records, metadata = self._generate_records(run, artifacts)

        qa_result = validate_audience_quotas(records)
        if qa_result.is_blocker():
            raise ValueError(
                f"Audience generation failed quota validation: {qa_result.message}"
            )

        emit_log(
            session,
            run.id,
            "Persisting audience master CSV",
            metadata={"rows": len(records)},
        )
        csv_path = write_records(self.OUTPUT_PATH, records)
        asset = self._register_asset(csv_path, len(records))

        latency = time.monotonic() - start

        telemetry = {
            "csv_path": str(csv_path),
            "records": records,
            "row_count": len(records),
            "dedupe": metadata["dedupe"],
            "coverage": metadata["coverage"],
            "gaps": metadata["gaps"],
            "deficits": metadata["gaps"],
            "qa": qa_result.to_dict(),
            "asset_id": str(asset.id),
            "andronoma_stage_latency_seconds": round(latency, 3),
        }

        return telemetry

    # ------------------------------------------------------------------
    # Payload loading utilities
    # ------------------------------------------------------------------
    def _load_processed_payload(self, run) -> Dict[str, Any]:
        run_id = run.id
        candidate = self.PROCESSED_ROOT / str(run_id) / "processing.json"
        if candidate.exists():
            try:
                return json.loads(candidate.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:  # pragma: no cover - defensive
                raise ValueError(
                    f"Processing payload for run {run_id} is not valid JSON"
                ) from exc
        telemetry = run.telemetry or {}
        process_data = telemetry.get("process", {})
        if process_data:
            return process_data if isinstance(process_data, dict) else {}
        raise FileNotFoundError(
            f"Processing artifacts missing for run {run_id}; audiences stage cannot continue"
        )

    # ------------------------------------------------------------------
    # Record generation helpers
    # ------------------------------------------------------------------
    def _generate_records(
        self, run, artifacts: Mapping[str, Any]
    ) -> Tuple[List[Mapping[str, Any]], Dict[str, Any]]:
        personas = self._persona_pool(run)
        motivations = self._motivation_pool(artifacts)
        blockers = self._blocker_pool(artifacts)
        if not blockers:
            blockers = ["uncertainty"]
        angles = self._angle_pool(artifacts)
        concepts = self._concept_pool(artifacts)
        proofs = self._proof_pool(artifacts)
        format_notes = self.FORMAT_NOTES or ["Meta Advantage+ campaign"]
        metrics = self.SUCCESS_METRICS or ["Return on ad spend"]
        ab_variants = self.AB_VARIANTS or ["Hook positioning"]
        exclusions = self.EXCLUSION_RULES or ["Exclude existing customers"]

        seed_context = self._seed_context(artifacts)
        total_required = max(self.TARGET_RECORDS, self.MIN_RECORDS)
        rows: List[OrderedDict[str, Any]] = []

        blocker_cycle = cycle(blockers)
        angle_cycle = cycle(angles)
        concept_cycle = cycle(concepts)
        proof_cycle = cycle(proofs)
        format_cycle = cycle(format_notes)
        metric_cycle = cycle(metrics)
        ab_cycle = cycle(ab_variants)
        exclusion_cycle = cycle(exclusions)

        for index in range(total_required):
            persona = personas[index % len(personas)]
            motivation_key, motivation_statement = motivations[index % len(motivations)]
            blocker_primary = next(blocker_cycle)
            blocker_secondary = next(blocker_cycle)
            blockers_text = (
                f"{blocker_primary} | {blocker_secondary}"
                if blocker_primary != blocker_secondary
                else blocker_primary
            )

            angle = next(angle_cycle)
            concept = next(concept_cycle)
            proof = next(proof_cycle)
            format_note = next(format_cycle)
            metric = next(metric_cycle)
            ab_variant = next(ab_cycle)
            exclusion = next(exclusion_cycle)

            motivation_label = motivation_key.title()
            seed_terms = self._seed_terms(
                persona, motivation_key, [blocker_primary, blocker_secondary], seed_context
            )

            record = OrderedDict()
            for column in REQUIRED_AUDIENCE_COLUMNS:
                record[column] = ""
            record["#"] = index + 1
            record["Audience Name"] = self._audience_name(
                persona, motivation_label, index
            )
            record["Who They Are"] = (
                f"{persona} who respond to {motivation_label.lower()} outcomes and {angle.lower()}."
            )
            record["Seed Terms"] = seed_terms
            record["Primary Motivation"] = motivation_label
            record["Top 2 Blockers"] = blockers_text
            record["Message Angle"] = f"{motivation_label}: {angle}"
            record["Creative Concept"] = concept
            record["Format Notes"] = (
                f"{format_note}. Spotlight {motivation_label.lower()} proof points."
            )
            record["Proof/Offer"] = proof
            record["Success Metric"] = metric
            record["A/B Variable"] = ab_variant
            record["Exclusions"] = exclusion

            rows.append(record)

        deduped_records, dedupe_meta = self._dedupe(rows)
        if len(deduped_records) < self.MIN_RECORDS:
            raise ValueError(
                "Audience dedupe process dropped below minimum quota; generation config insufficient"
            )
        coverage_meta, gaps = self._coverage(deduped_records, motivations, blockers)

        metadata = {
            "dedupe": dedupe_meta,
            "coverage": coverage_meta,
            "gaps": gaps,
        }
        return deduped_records, metadata

    def _persona_pool(self, run) -> List[str]:
        config = run.input_payload.get("config", {}) if run else {}
        personas = [
            persona.strip()
            for persona in config.get("target_markets", [])
            if isinstance(persona, str) and persona.strip()
        ]
        if personas:
            return personas
        telemetry = run.telemetry or {}
        fallback = []
        process_docs = telemetry.get("process", {}).get("source_documents") if telemetry else None
        if isinstance(process_docs, list):
            for doc in process_docs:
                title = doc.get("title") if isinstance(doc, dict) else None
                if isinstance(title, str) and title.strip():
                    fallback.append(title.strip())
        if fallback:
            return fallback[: len(self.DEFAULT_PERSONAS)]
        return self.DEFAULT_PERSONAS

    def _motivation_pool(
        self, artifacts: Mapping[str, Any]
    ) -> List[Tuple[str, str]]:
        motivation_map = artifacts.get("motivation_map", {}) if artifacts else {}
        pool: List[Tuple[str, str]] = []
        for key, payload in motivation_map.items():
            insights = payload.get("insights", []) if isinstance(payload, Mapping) else []
            statement = ""
            for insight in insights:
                text = insight.get("statement") if isinstance(insight, Mapping) else None
                if text:
                    statement = text
                    break
            if not statement:
                intensity = payload.get("intensity", "") if isinstance(payload, Mapping) else ""
                statement = f"{key.title()} motivation with {intensity.lower()} intensity".strip()
            pool.append((str(key), statement))
        if pool:
            return pool
        return [
            ("functional", "Operational buyers seeking efficiency gains"),
            ("emotional", "Leaders wanting confidence their team is supported"),
            ("aspirational", "Executives chasing category leadership"),
            ("social", "Community builders who amplify peer proof"),
        ]

    def _blocker_pool(self, artifacts: Mapping[str, Any]) -> List[str]:
        blockers = []
        for record in artifacts.get("blockers_ranking", []) or []:
            blocker = record.get("blocker") if isinstance(record, Mapping) else None
            if blocker:
                blockers.append(str(blocker))
        if blockers:
            return blockers
        return ["uncertainty", "cost", "integration", "time", "trust"]

    def _angle_pool(self, artifacts: Mapping[str, Any]) -> List[str]:
        brand = artifacts.get("brand_position", {}) if artifacts else {}
        angles: List[str] = []
        category = brand.get("category", {}) if isinstance(brand, Mapping) else {}
        category_statement = category.get("statement") if isinstance(category, Mapping) else None
        if category_statement:
            angles.append(category_statement)
        promise = brand.get("promise", {}) if isinstance(brand, Mapping) else {}
        promise_statement = promise.get("statement") if isinstance(promise, Mapping) else None
        if promise_statement:
            angles.append(promise_statement)
        for differentiator in brand.get("differentiators", []) or []:
            if isinstance(differentiator, Mapping):
                statement = differentiator.get("statement")
                if statement:
                    angles.append(statement)
        if not angles:
            angles = [
                "Lead with proof that outcomes are guaranteed",
                "Contrast the status quo against automated workflows",
                "Reinforce community validation to overcome doubt",
            ]
        return angles

    def _concept_pool(self, artifacts: Mapping[str, Any]) -> List[str]:
        market = artifacts.get("market_summary", {}) if artifacts else {}
        concepts: List[str] = []
        whitespace = market.get("whitespace_opportunities", []) if isinstance(market, Mapping) else []
        for entry in whitespace or []:
            if isinstance(entry, Mapping):
                statement = entry.get("statement")
                if statement:
                    concepts.append(f"Whitespace push: {statement}")
        cultural = market.get("cultural_signals", []) if isinstance(market, Mapping) else []
        for entry in cultural or []:
            if isinstance(entry, Mapping):
                statement = entry.get("statement")
                if statement:
                    concepts.append(f"Culture tap: {statement}")
        if not concepts:
            concepts = [
                "Use narrative proof contrasting old vs new way",
                "UGC testimonial stitched with data overlay",
                "Product walkthrough emphasising ease",
                "Founder POV on mission and community",
            ]
        return concepts

    def _proof_pool(self, artifacts: Mapping[str, Any]) -> List[str]:
        brand = artifacts.get("brand_position", {}) if artifacts else {}
        proofs: List[str] = []
        for pillar in brand.get("proof_pillars", []) or []:
            if isinstance(pillar, Mapping):
                statement = pillar.get("statement")
                if statement:
                    proofs.append(statement)
        value = brand.get("value_framing", {}) if isinstance(brand, Mapping) else {}
        framing_statement = value.get("statement") if isinstance(value, Mapping) else None
        if framing_statement:
            proofs.append(framing_statement)
        if not proofs:
            proofs = [
                "Leverage testimonial quoting quantified ROI",
                "Offer live demo with onboarding concierge",
                "Feature free trial with guided setup",
            ]
        return proofs

    def _seed_context(self, artifacts: Mapping[str, Any]) -> List[str]:
        brand = artifacts.get("brand_position", {}) if artifacts else {}
        context_terms: List[str] = []
        category = brand.get("category", {}) if isinstance(brand, Mapping) else {}
        category_statement = category.get("statement") if isinstance(category, Mapping) else ""
        promise = brand.get("promise", {}) if isinstance(brand, Mapping) else {}
        promise_statement = promise.get("statement") if isinstance(promise, Mapping) else ""
        for statement in [category_statement, promise_statement]:
            context_terms.extend(self._keyword_slice(statement))
        for differentiator in brand.get("differentiators", []) or []:
            if isinstance(differentiator, Mapping):
                context_terms.extend(self._keyword_slice(differentiator.get("statement", "")))
        return context_terms[:10]

    def _seed_terms(
        self,
        persona: str,
        motivation_key: str,
        blockers: Sequence[str],
        context_terms: Sequence[str],
    ) -> str:
        tokens: List[str] = []
        tokens.extend(self._keyword_slice(persona))
        tokens.append(motivation_key.lower())
        for blocker in blockers:
            tokens.extend(self._keyword_slice(blocker))
        tokens.extend(term.lower() for term in context_terms)
        unique: List[str] = []
        for token in tokens:
            cleaned = token.strip().lower()
            if cleaned and cleaned not in unique:
                unique.append(cleaned)
        return ", ".join(unique[:6])

    def _keyword_slice(self, text: str | None) -> List[str]:
        if not text:
            return []
        cleaned = re.findall(r"[a-zA-Z0-9]+", text)
        return [token.lower() for token in cleaned[:4]]

    def _audience_name(self, persona: str, motivation: str, index: int) -> str:
        base = persona.split(" ")[:3]
        base_name = " ".join(base).strip()
        if not base_name:
            base_name = "Audience"
        return f"{base_name} · {motivation} #{index + 1:02d}"

    def _dedupe(
        self, records: Sequence[Mapping[str, Any]]
    ) -> Tuple[List[Mapping[str, Any]], Dict[str, Any]]:
        seen: Dict[str, Mapping[str, Any]] = {}
        duplicates: List[str] = []
        for record in records:
            name = str(record.get("Audience Name", "")).strip().lower()
            if name in seen:
                duplicates.append(record.get("Audience Name", ""))
                continue
            seen[name] = record
        deduped = list(seen.values())
        metadata = {
            "initial_candidates": len(records),
            "final_count": len(deduped),
            "duplicates_removed": len(records) - len(deduped),
            "duplicate_names": sorted({dup for dup in duplicates if dup}),
        }
        return deduped, metadata

    def _coverage(
        self,
        records: Sequence[Mapping[str, Any]],
        motivations: Sequence[Tuple[str, str]],
        blockers: Sequence[str],
    ) -> Tuple[Dict[str, Any], List[str]]:
        motivation_counts: Counter[str] = Counter()
        blocker_counts: Counter[str] = Counter()
        for record in records:
            motivation_counts[record.get("Primary Motivation", "")] += 1
            blockers_field = record.get("Top 2 Blockers", "")
            for blocker in blockers_field.split("|"):
                normalized = blocker.strip()
                if normalized:
                    blocker_counts[normalized] += 1
        motivation_labels = [key.title() for key, _ in motivations]
        gaps: List[str] = []
        total = len(records)
        if motivation_labels:
            expected = max(1, int(total * self.GAP_THRESHOLD_MOTIVATION / max(len(motivation_labels), 1)))
            for label in motivation_labels:
                if motivation_counts.get(label, 0) < expected:
                    gaps.append(
                        f"Motivation '{label}' has limited coverage ({motivation_counts.get(label, 0)} < {expected})."
                    )
        if blockers:
            expected_blocker = max(1, int(total * self.GAP_THRESHOLD_BLOCKER / max(len(blockers), 1)))
            for blocker in blockers:
                if blocker_counts.get(blocker, 0) < expected_blocker:
                    gaps.append(
                        f"Blocker '{blocker}' appears infrequently ({blocker_counts.get(blocker, 0)} < {expected_blocker})."
                    )

        coverage = {
            "motivation_counts": dict(motivation_counts),
            "blocker_counts": dict(blocker_counts),
            "total_records": total,
        }
        return coverage, gaps

    def _register_asset(self, path: Path, rows: int) -> AssetRecord:
        session = self.context.session
        asset = AssetRecord(
            id=uuid.uuid4(),
            run_id=self.context.run.id,
            stage=self.name,
            asset_type="csv",
            storage_key=str(path),
            extra={"rows": rows, "name": "audiences_master"},
        )
        session.add(asset)
        session.commit()
        session.refresh(asset)
        return asset
