"""Ad readiness heuristics derived from research insights."""
from __future__ import annotations

from typing import Any, Dict, List, Sequence


def evaluate_ad_readiness(
    documents: Sequence[Dict[str, Any]],
    brand_position: Dict[str, Any],
    motivation_map: Dict[str, Any],
    blockers: Sequence[Dict[str, Any]],
    conversion_hypotheses: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Return readiness heuristics, labelled Direct vs Inferred."""

    direct_hypotheses = [hyp for hyp in conversion_hypotheses if hyp.get("type") == "Direct"]
    direct_sources = _collect_sources(direct_hypotheses)
    blocker_sources = blockers[0]["sources"] if blockers else []

    readiness_score = min(100, 60 + len(direct_hypotheses) * 10)

    return {
        "overall": {
            "score": readiness_score,
            "type": "Inferred",
            "sources": direct_sources or _fallback_sources(brand_position),
            "reasoning": (
                "Direct CTA evidence elevates readiness." if direct_hypotheses else "Reliant on inferred motivations due to missing CTA signals."
            ),
        },
        "cta": {
            "status": "Confident" if direct_hypotheses else "Needs validation",
            "type": "Direct" if direct_hypotheses else "Inferred",
            "sources": direct_sources or blocker_sources,
            "recommendations": _cta_recommendations(direct_hypotheses, blockers),
        },
        "value_proposition": {
            "status": "Aligned",
            "type": "Direct" if brand_position.get("promise", {}).get("sources") else "Inferred",
            "sources": brand_position.get("promise", {}).get("sources", []),
            "recommendations": [
                "Lead with the most emotionally resonant motivator to maximise click intent.",
            ],
        },
        "proof": {
            "status": "Robust" if brand_position.get("proof_pillars") else "Sparse",
            "type": "Direct" if any(p.get("type") == "Direct" for p in brand_position.get("proof_pillars", [])) else "Inferred",
            "sources": _collect_sources(brand_position.get("proof_pillars", [])),
            "recommendations": [
                "Pair each CTA with a proof point clarifying measurable impact.",
            ],
        },
        "legibility": {
            "status": _legibility_status(documents),
            "type": "Direct",
            "sources": _legibility_sources(documents),
            "recommendations": [
                "Highlight the product noun early in copy to reduce cognitive load.",
            ],
        },
    }


def _collect_sources(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sources: List[Dict[str, Any]] = []
    seen = set()
    for item in items:
        for source in item.get("sources", []):
            key = (source.get("source"), source.get("hash"), source.get("evidence"))
            if key not in seen:
                seen.add(key)
                sources.append(source)
    return sources


def _fallback_sources(brand_position: Dict[str, Any]) -> List[Dict[str, Any]]:
    sources = brand_position.get("promise", {}).get("sources", [])
    if sources:
        return sources
    return _collect_sources(brand_position.get("differentiators", []))


def _cta_recommendations(
    direct_hypotheses: Sequence[Dict[str, Any]],
    blockers: Sequence[Dict[str, Any]],
) -> List[str]:
    recommendations: List[str] = []
    if not direct_hypotheses:
        recommendations.append("Prototype CTA variants using the dominant functional motivator.")
    if blockers:
        recommendations.append(f"Address {blockers[0]['blocker']} objection immediately after CTA copy.")
    if not recommendations:
        recommendations.append("A/B test tonal shifts between urgency and reassurance.")
    return recommendations


def _legibility_status(documents: Sequence[Dict[str, Any]]) -> str:
    sentence_lengths: List[int] = []
    for doc in documents:
        for sentence in doc.get("sentences", []):
            sentence_lengths.append(len(sentence.split()))
    if not sentence_lengths:
        return "Unknown"
    avg = sum(sentence_lengths) / len(sentence_lengths)
    return "Clear" if avg <= 22 else "Verbose"


def _legibility_sources(documents: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sources: List[Dict[str, Any]] = []
    for doc in documents:
        for sentence in doc.get("sentences", [])[:1]:
            sources.append(
                {
                    "source": doc.get("url") or doc["id"],
                    "hash": doc["hash"],
                    "evidence": sentence,
                }
            )
    return sources[:3]

