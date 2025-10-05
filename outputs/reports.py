"""Generate HTML and text reports for the campaign."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from jinja2 import Environment, PackageLoader, select_autoescape

env = Environment(loader=PackageLoader("outputs", "templates"), autoescape=select_autoescape())


def render_report(template_name: str, context: Dict[str, Any], destination: Path) -> Path:
    template = env.get_template(template_name)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(template.render(**context), encoding="utf-8")
    return destination
