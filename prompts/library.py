"""Prompt templates for generation stages."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass
class PromptTemplate:
    name: str
    template: str
    description: str


PROMPTS: Dict[str, PromptTemplate] = {
    "creative_brief": PromptTemplate(
        name="creative_brief",
        template="""You are creating campaign copy for {product}. Focus on {audience}.""",
        description="High level creative direction for the creative generation stage.",
    ),
    "image_brief": PromptTemplate(
        name="image_brief",
        template="""Design a {style} visual featuring {subject} in a {mood} tone.""",
        description="Guidance for the image generation stage.",
    ),
}
