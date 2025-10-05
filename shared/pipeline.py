"""Utilities for orchestrating the scrape→process→audiences→creatives→images→qa→export pipeline."""
from __future__ import annotations

from typing import List

PIPELINE_ORDER: List[str] = [
    "scrape",
    "process",
    "audiences",
    "creatives",
    "images",
    "qa",
    "export",
]
