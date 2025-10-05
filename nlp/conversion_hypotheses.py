"""Generate conversion hypotheses from research artifacts."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence


CTA_KEYWORDS = {
    "try", "start", "join", "book", "schedule", "demo", "buy", "explore", "learn", "discover"
}


def generate_conversion_hypotheses(
    documents: Sequence[Dict[str, Any]],
    brand_position: Dict[str, Any],
    motivation_map: Dict[str, Any],
    blockers: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Return structured conversion hypotheses with evidence labels."""

    cta_sentences: List[Dict[str, Any]] = []
    for doc in documents:
        for sentence in doc.get("sentences", []):
            if any(keyword in sentence.lower() for keyword in CTA_KEYWORDS):
                cta_sentences.append(
                    {
                        "statement": sentence,
                        "type": "Direct",
                        "sources": [
                            {
                                "source": doc.get("url") or doc["id"],
                                "hash": doc["hash"],
                                "evidence": sentence.strip(),
                            }
                        ],
                        "doc": doc,
                    }
                )

    hypotheses: List[Dict[str, Any]] = []
    motivation_priorities = _motivation_priority(motivation_map)
    blocker_top = blockers[0] if blockers else None

    for idx, evidence in enumerate(cta_sentences[:3], start=1):
        hypotheses.append(
            {
                "id": f"hypothesis-{idx}",
                "type": "Direct",
                "value_proposition": f"{_primary_promise(brand_position)} reinforced by {motivation_priorities[0]}",
                "proof_assets": _proof_assets(brand_position),
                "cta_tone": _cta_tone(evidence["statement"]),
                "sources": evidence["sources"],
                "reasoning": "CTA language observed directly within research corpus.",
            }
        )

    if len(hypotheses) < 3:
        inferred_sources = _aggregate_sources(motivation_map, brand_position)
        for idx in range(len(hypotheses), 3):
            blocker_clause = (
                f" while neutralising {blocker_top['blocker']} concerns"
                if blocker_top
                else ""
            )
            hypotheses.append(
                {
                    "id": f"hypothesis-{idx+1}",
                    "type": "Inferred",
                    "value_proposition": f"Translate {motivation_priorities[idx % len(motivation_priorities)]}{blocker_clause}",
                    "proof_assets": _proof_assets(brand_position),
                    "cta_tone": "Reassuring" if blocker_top else "Uplifting",
                    "sources": inferred_sources,
                    "reasoning": "Derived from dominant motivations and blockers due to absent direct CTA signals.",
                }
            )

    return hypotheses


def _motivation_priority(motivation_map: Dict[str, Any]) -> List[str]:
    ordered: List[str] = []
    for key in ["functional", "emotional", "aspirational", "social"]:
        bucket = motivation_map.get(key, {})
        insights = bucket.get("insights", [])
        label = key.capitalize()
        if insights and insights[0].get("type") == "Direct":
            label = f"{label} ({insights[0]['statement'][:60]})"
        ordered.append(label)
    return ordered or ["Core Value"]


def _primary_promise(brand_position: Dict[str, Any]) -> str:
    promise = brand_position.get("promise", {})
    return promise.get("statement", "deliver measurable outcomes")


def _proof_assets(brand_position: Dict[str, Any]) -> List[Dict[str, Any]]:
    assets: List[Dict[str, Any]] = []
    for pillar in brand_position.get("proof_pillars", [])[:3]:
        assets.append(
            {
                "type": "Direct" if pillar.get("type") == "Direct" else "Inferred",
                "statement": pillar.get("statement"),
                "sources": pillar.get("sources", []),
            }
        )
    if not assets:
        assets.append(
            {
                "type": "Inferred",
                "statement": "Surface customer testimonials highlighting outcome metrics.",
                "sources": brand_position.get("promise", {}).get("sources", []),
            }
        )
    return assets


def _cta_tone(sentence: str) -> str:
    lower = sentence.lower()
    if any(keyword in lower for keyword in {"today", "now", "immediately"}):
        return "Urgent"
    if any(keyword in lower for keyword in {"explore", "learn", "discover"}):
        return "Curious"
    return "Supportive"


def _aggregate_sources(
    motivation_map: Dict[str, Any], brand_position: Dict[str, Any]
) -> List[Dict[str, Any]]:
    sources: List[Dict[str, Any]] = []
    for bucket in motivation_map.values():
        for insight in bucket.get("insights", [])[:1]:
            sources.extend(insight.get("sources", [])[:1])
    sources.extend(brand_position.get("promise", {}).get("sources", [])[:1])
    unique: List[Dict[str, Any]] = []
    seen = set()
    for source in sources:
        key = (source.get("source"), source.get("hash"), source.get("evidence"))
        if key not in seen:
            seen.add(key)
            unique.append(source)
    return unique

