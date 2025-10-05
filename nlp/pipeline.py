"""NLP enrichment stage for the Andronoma pipeline."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple
from urllib.parse import urlparse

from sqlalchemy import select

from shared.logs import emit_log
from shared.models import AssetRecord
from shared.stages.base import BaseStage

from . import ad_readiness, brand_fit, conversion_hypotheses


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

    def execute(self) -> Dict[str, int]:
        self.ensure_budget(30.0)
        return {"segments": 4, "personas": 3}
