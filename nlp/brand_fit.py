"""Brand fit scoring heuristics."""
from __future__ import annotations

from typing import Any, Dict, List, Sequence


def score_brand_fit(
    documents: Sequence[Dict[str, Any]],
    pipeline_input: Dict[str, Any],
    brand_position: Dict[str, Any],
    motivation_map: Dict[str, Any],
) -> Dict[str, Any]:
    """Return brand-fit metrics with labelled evidence."""

    tone_alignment = _tone_alignment(documents, motivation_map)
    aesthetic_alignment = _aesthetic_alignment(documents, pipeline_input)

    direct_sources = tone_alignment.get("sources", []) + aesthetic_alignment.get("sources", [])
    score = min(100, 55 + len(direct_sources) * 5)

    return {
        "overall": {
            "score": score,
            "type": "Inferred",
            "sources": direct_sources or brand_position.get("promise", {}).get("sources", []),
            "reasoning": "Score synthesises tonal evidence with desired positioning cues.",
        },
        "tone_alignment": tone_alignment,
        "aesthetic_alignment": aesthetic_alignment,
        "gaps": _fit_gaps(brand_position, tone_alignment, aesthetic_alignment),
    }


def _tone_alignment(
    documents: Sequence[Dict[str, Any]], motivation_map: Dict[str, Any]
) -> Dict[str, Any]:
    emotional = motivation_map.get("emotional", {}).get("insights", [])
    social = motivation_map.get("social", {}).get("insights", [])
    sources: List[Dict[str, Any]] = []
    for bucket in (emotional, social):
        for insight in bucket[:2]:
            sources.extend(insight.get("sources", []))
    return {
        "statement": "Tone mirrors audience emotional and social motivators.",
        "type": "Direct" if sources else "Inferred",
        "sources": sources,
        "recommendations": [
            "Maintain empathetic language that reflects the research vernacular.",
        ],
    }


def _aesthetic_alignment(
    documents: Sequence[Dict[str, Any]], pipeline_input: Dict[str, Any]
) -> Dict[str, Any]:
    metadata = pipeline_input.get("config", {}).get("metadata", {})
    desired_tone = metadata.get("tone", "modern")
    style_keywords = metadata.get("style_keywords", [])

    sources: List[Dict[str, Any]] = []
    for doc in documents:
        if any(keyword.lower() in doc.get("body", "").lower() for keyword in style_keywords):
            sources.append(
                {
                    "source": doc.get("url") or doc["id"],
                    "hash": doc["hash"],
                    "evidence": doc.get("sentences", [doc.get("body", "")])[0],
                }
            )

    return {
        "statement": f"Research indicates alignment with {desired_tone} aesthetic cues.",
        "type": "Direct" if sources else "Inferred",
        "sources": sources,
        "recommendations": [
            "Reference collected imagery or descriptors within creative briefs.",
        ],
    }


def _fit_gaps(
    brand_position: Dict[str, Any],
    tone_alignment: Dict[str, Any],
    aesthetic_alignment: Dict[str, Any],
) -> List[Dict[str, Any]]:
    gaps: List[Dict[str, Any]] = []
    if tone_alignment.get("type") != "Direct":
        gaps.append(
            {
                "statement": "Validate tone cues through qualitative interviews.",
                "type": "Inferred",
                "sources": brand_position.get("promise", {}).get("sources", []),
            }
        )
    if aesthetic_alignment.get("type") != "Direct":
        proof_sources: List[Dict[str, Any]] = []
        for pillar in brand_position.get("proof_pillars", []):
            proof_sources.extend(pillar.get("sources", []))
            if proof_sources:
                break
        gaps.append(
            {
                "statement": "Supplement aesthetic guidance with asset audits or mood boards.",
                "type": "Inferred",
                "sources": proof_sources,
            }
        )
    return gaps

