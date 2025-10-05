"""Image generation stage."""
from __future__ import annotations

import base64
import hashlib
import io
import os
import random
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
from PIL import Image, ImageDraw, ImageFont, ImageStat

from shared.models import AssetRecord
from shared.stages.base import BaseStage

SAFE_AREA_MARGIN = 72
CANVAS_SIZE = (1080, 1350)
HEADLINE_FONT_RANGE = (72, 110)
SUBCOPY_FONT_RANGE = (36, 48)
CTA_FONT_RANGE = (32, 40)
CONTRAST_THRESHOLD = 4.5
DEFAULT_PROVIDER = "stub"
DEFAULT_PROMPT_SUFFIX = (
    "Neutral interior scene, product hero focus, natural lighting, realistic shadows."
)


class RenderError(RuntimeError):
    """Raised when the image provider cannot produce a render."""


class ConfigurationError(RenderError):
    """Raised when a provider is selected but is not properly configured."""


@dataclass
class CreativePayload:
    """Representation of a creative concept that requires imagery."""

    concept_id: str
    headline: str
    subcopy: str
    cta: str
    prompt: str
    raw: Dict[str, Any]


class RenderClient:
    """Client responsible for requesting renders from the configured provider."""

    def __init__(
        self,
        provider: str,
        unit_cost: float = 0.08,
        max_retries: int = 2,
        backoff_seconds: float = 1.5,
    ) -> None:
        self.provider = (provider or DEFAULT_PROVIDER).lower()
        self.unit_cost = unit_cost
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self._client: Optional[httpx.Client] = None
        self._last_provider = self.provider

    @property
    def client(self) -> httpx.Client:
        if not self._client:
            self._client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0))
        return self._client

    @property
    def active_provider(self) -> str:
        return self._last_provider

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def render(
        self, concept: CreativePayload, budget_remaining: float
    ) -> Tuple[Image.Image, float, Dict[str, Any]]:
        """Render a creative concept, respecting retries and the provided budget."""

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                image_bytes, metadata = self._render_once(concept)
                cost = float(metadata.get("cost", self.unit_cost))
                if cost > budget_remaining + 1e-9:
                    raise RenderError(
                        f"Insufficient budget for render ({cost:.2f} > {budget_remaining:.2f})"
                    )
                image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
                metadata.setdefault("provider", self.active_provider)
                metadata.setdefault("prompt", concept.prompt)
                metadata.setdefault("cost", cost)
                return image, cost, metadata
            except ConfigurationError:
                image_bytes, metadata = self._render_stub(concept)
                cost = float(metadata.get("cost", self.unit_cost))
                if cost > budget_remaining + 1e-9:
                    raise RenderError(
                        f"Insufficient budget for render ({cost:.2f} > {budget_remaining:.2f})"
                    )
                image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
                metadata.setdefault("provider", self.active_provider)
                metadata.setdefault("prompt", concept.prompt)
                metadata.setdefault("cost", cost)
                return image, cost, metadata
            except Exception as exc:  # pragma: no cover - defensive network handling
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(self.backoff_seconds * (attempt + 1))
        raise RenderError(str(last_error) if last_error else "unknown render failure")

    def _render_once(self, concept: CreativePayload) -> Tuple[bytes, Dict[str, Any]]:
        provider = self.provider
        if provider in {"stability", "stabilityai", "stability_ai"}:
            self._last_provider = "stability"
            return self._render_stability(concept)
        if provider == "replicate":
            self._last_provider = "replicate"
            return self._render_replicate(concept)
        return self._render_stub(concept)

    def _render_stub(self, concept: CreativePayload) -> Tuple[bytes, Dict[str, Any]]:
        """Create a deterministic stub image when a provider is unavailable."""

        self._last_provider = "stub"
        width, height = CANVAS_SIZE
        seed = int(hashlib.sha256(concept.prompt.encode("utf-8")).hexdigest()[0:8], 16)
        rng = random.Random(seed)
        primary = tuple(rng.randint(70, 200) for _ in range(3))
        secondary = tuple(min(255, max(0, c + rng.randint(-30, 60))) for c in primary)
        image = Image.new("RGB", (width, height), primary)
        overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        for idx in range(12):
            radius = rng.randint(width // 12, width // 4)
            cx = rng.randint(radius, width - radius)
            cy = rng.randint(radius, height - radius)
            color = tuple(min(255, max(0, val + rng.randint(-20, 20))) for val in secondary)
            alpha = int(180 * (1 - idx / 12))
            draw.ellipse(
                [cx - radius, cy - radius, cx + radius, cy + radius],
                fill=(*color, alpha),
            )
        image.paste(overlay, mask=overlay.split()[-1])
        buffer = io.BytesIO()
        image.save(buffer, format="JPEG", quality=95)
        return buffer.getvalue(), {
            "mode": "stub",
            "seed": seed,
            "cost": self.unit_cost,
        }

    def _render_stability(self, concept: CreativePayload) -> Tuple[bytes, Dict[str, Any]]:
        api_key = os.getenv("STABILITY_API_KEY")
        if not api_key:
            raise ConfigurationError("STABILITY_API_KEY is not configured")
        engine = os.getenv("STABILITY_ENGINE", "stable-diffusion-xl-1024-v1-0")
        payload = {
            "text_prompts": [
                {"text": concept.prompt, "weight": 1},
                {
                    "text": "Neutral interior scene, product hero focus, natural lighting",
                    "weight": 0.4,
                },
            ],
            "cfg_scale": 7,
            "clip_guidance_preset": "FAST_BLUE",
            "samples": 1,
            "steps": 30,
            "width": CANVAS_SIZE[0],
            "height": CANVAS_SIZE[1],
        }
        response = self.client.post(
            f"https://api.stability.ai/v1/generation/{engine}/text-to-image",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
        artifacts = data.get("artifacts", [])
        artifact = next((item for item in artifacts if item.get("type") == "image"), None)
        if not artifact:
            raise RenderError("Stability response did not include an image artifact")
        image_bytes = base64.b64decode(artifact["base64"])
        gpu_duration = float(data.get("metrics", {}).get("gpu_duration", 0.0))
        cost = gpu_duration * 0.0005 or self.unit_cost
        return image_bytes, {
            "mode": "stability",
            "engine": engine,
            "cost": cost,
        }

    def _render_replicate(self, concept: CreativePayload) -> Tuple[bytes, Dict[str, Any]]:
        token = os.getenv("REPLICATE_API_TOKEN")
        if not token:
            raise ConfigurationError("REPLICATE_API_TOKEN is not configured")
        model = os.getenv("REPLICATE_MODEL", "stability-ai/sdxl")
        version = os.getenv(
            "REPLICATE_MODEL_VERSION",
            "8f26ab1a5664638fd8759b5b6b85aa6c7466626900b8f359d6616bc8b39c0e6f",
        )
        payload = {
            "version": version,
            "input": {
                "prompt": concept.prompt,
                "width": CANVAS_SIZE[0],
                "height": CANVAS_SIZE[1],
                "apply_watermark": False,
            },
            "model": model,
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        creation = self.client.post(
            "https://api.replicate.com/v1/predictions",
            json=payload,
            headers=headers,
        )
        creation.raise_for_status()
        prediction = creation.json()
        prediction_id = prediction.get("id")
        status = prediction.get("status")
        while status not in {"succeeded", "failed", "canceled"}:
            time.sleep(2)
            poll = self.client.get(
                f"https://api.replicate.com/v1/predictions/{prediction_id}",
                headers=headers,
            )
            poll.raise_for_status()
            prediction = poll.json()
            status = prediction.get("status")
        if status != "succeeded":
            raise RenderError(f"Replicate prediction {prediction_id} finished with status {status}")
        outputs = prediction.get("output") or []
        if not outputs:
            raise RenderError("Replicate response did not include image output")
        image_url = outputs[0]
        image_response = self.client.get(image_url)
        image_response.raise_for_status()
        predict_time = float(prediction.get("metrics", {}).get("predict_time", 0.0))
        cost = predict_time * 0.002 or self.unit_cost
        return image_response.content, {
            "mode": "replicate",
            "model": model,
            "prediction_id": prediction_id,
            "cost": cost,
        }


def _rgb_to_hex(color: Sequence[int]) -> str:
    r, g, b = [max(0, min(255, int(c))) for c in color[:3]]
    return f"#{r:02x}{g:02x}{b:02x}"


def _relative_luminance(color: Sequence[float]) -> float:
    def channel(value: float) -> float:
        v = value / 255.0
        return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4

    r, g, b = [channel(c) for c in color[:3]]
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _contrast_ratio(l1: float, l2: float) -> float:
    lighter = max(l1, l2)
    darker = min(l1, l2)
    return (lighter + 0.05) / (darker + 0.05)


def _font_candidates(bold: bool = False) -> Iterable[Path]:
    base_paths = [
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"),
    ]
    bold_paths = [
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
        Path("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"),
    ]
    if bold:
        for path in bold_paths + base_paths:
            yield path
    else:
        for path in base_paths:
            yield path


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for candidate in _font_candidates(bold=bold):
        if candidate.exists():
            try:
                return ImageFont.truetype(str(candidate), size=size)
            except OSError:  # pragma: no cover - font fallback
                continue
    return ImageFont.load_default()


def _wrap_text(
    text: str, draw: ImageDraw.ImageDraw, font: ImageFont.ImageFont, max_width: int
) -> List[str]:
    lines: List[str] = []
    for paragraph in text.splitlines() or [text]:
        words = paragraph.split()
        if not words:
            lines.append("")
            continue
        current: List[str] = []
        for word in words:
            test_line = " ".join(current + [word])
            bbox = draw.textbbox((0, 0), test_line, font=font)
            if bbox[2] - bbox[0] <= max_width:
                current.append(word)
            else:
                if current:
                    lines.append(" ".join(current))
                    current = [word]
                else:
                    lines.append(word)
                    current = []
        if current:
            lines.append(" ".join(current))
    return lines


def _fit_text_block(
    text: str,
    draw: ImageDraw.ImageDraw,
    font_range: Tuple[int, int],
    max_width: int,
    max_height: int,
    *,
    bold: bool = False,
) -> Tuple[ImageFont.ImageFont, List[str], int]:
    cleaned = text.strip()
    if not cleaned:
        font = _load_font(font_range[0], bold=bold)
        return font, [], 0
    min_size, max_size = font_range
    for size in range(max_size, min_size - 1, -2):
        font = _load_font(size, bold=bold)
        lines = _wrap_text(cleaned, draw, font, max_width)
        spacing = max(8, int(size * 0.2)) if lines else 0
        total_height = 0
        max_line_width = 0
        for index, line in enumerate(lines):
            sample = line or " "
            bbox = draw.textbbox((0, 0), sample, font=font)
            width = bbox[2] - bbox[0]
            height = bbox[3] - bbox[1]
            total_height += height
            if index < len(lines) - 1:
                total_height += spacing
            max_line_width = max(max_line_width, width)
        if total_height <= max_height and max_line_width <= max_width:
            return font, lines, total_height
    raise RenderError("Text block does not fit within the safe area")


def _fit_cta(
    text: str, draw: ImageDraw.ImageDraw, max_width: int
) -> Tuple[str, ImageFont.ImageFont, int, int]:
    label = (text or "Learn More").strip().upper()
    for size in range(CTA_FONT_RANGE[1], CTA_FONT_RANGE[0] - 1, -2):
        font = _load_font(size, bold=True)
        bbox = draw.textbbox((0, 0), label, font=font)
        width = bbox[2] - bbox[0] + 48
        height = bbox[3] - bbox[1] + 32
        if width <= max_width:
            return label, font, width, height
    raise RenderError("CTA does not fit within the safe area")


def compose_overlay(image: Image.Image, creative: CreativePayload) -> Tuple[Image.Image, Dict[str, Any]]:
    base = image
    if base.size != CANVAS_SIZE:
        base = base.resize(CANVAS_SIZE, Image.LANCZOS)
    overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    safe_left = SAFE_AREA_MARGIN
    safe_top = SAFE_AREA_MARGIN
    safe_right = base.width - SAFE_AREA_MARGIN
    safe_bottom = base.height - SAFE_AREA_MARGIN
    safe_box = (safe_left, safe_top, safe_right, safe_bottom)

    safe_crop = base.crop(safe_box)
    mean_color = ImageStat.Stat(safe_crop).mean
    luminance = _relative_luminance(mean_color)
    white_ratio = _contrast_ratio(_relative_luminance((255, 255, 255)), luminance)
    dark_rgb = (20, 20, 24)
    dark_ratio = _contrast_ratio(_relative_luminance(dark_rgb), luminance)

    if white_ratio >= dark_ratio:
        text_color = (255, 255, 255)
        safe_fill = (0, 0, 0, 180)
        cta_text_color = (20, 20, 24)
        cta_fill = (255, 255, 255, 235)
    else:
        text_color = dark_rgb
        safe_fill = (255, 255, 255, 215)
        cta_text_color = (255, 255, 255)
        cta_fill = (0, 0, 0, 210)

    draw.rounded_rectangle(safe_box, radius=48, fill=safe_fill)

    padding = 32
    content_left = safe_left + padding
    content_top = safe_top + padding
    content_right = safe_right - padding
    content_bottom = safe_bottom - padding
    content_width = content_right - content_left
    content_height = content_bottom - content_top

    y = content_top

    headline_font, headline_lines, headline_height = _fit_text_block(
        creative.headline,
        draw,
        HEADLINE_FONT_RANGE,
        content_width,
        int(content_height * 0.45),
        bold=True,
    )
    headline_top = y
    line_spacing = max(10, int(getattr(headline_font, "size", HEADLINE_FONT_RANGE[0]) * 0.18)) if headline_lines else 0
    for index, line in enumerate(headline_lines):
        bbox = draw.textbbox((0, 0), line, font=headline_font)
        line_height = bbox[3] - bbox[1]
        draw.text((content_left, y), line, font=headline_font, fill=text_color)
        y += line_height
        if index < len(headline_lines) - 1:
            y += line_spacing
    headline_bottom = y
    if headline_lines:
        y += 24

    subcopy_font, subcopy_lines, subcopy_height = _fit_text_block(
        creative.subcopy,
        draw,
        SUBCOPY_FONT_RANGE,
        content_width,
        int(content_height * 0.3),
    )
    subcopy_top = y
    subcopy_spacing = max(8, int(getattr(subcopy_font, "size", SUBCOPY_FONT_RANGE[0]) * 0.25)) if subcopy_lines else 0
    for index, line in enumerate(subcopy_lines):
        sample = line or " "
        bbox = draw.textbbox((0, 0), sample, font=subcopy_font)
        line_height = bbox[3] - bbox[1]
        draw.text((content_left, y), line, font=subcopy_font, fill=text_color)
        y += line_height
        if index < len(subcopy_lines) - 1:
            y += subcopy_spacing
    subcopy_bottom = y
    if subcopy_lines:
        y += 28

    cta_label, cta_font, pill_width, pill_height = _fit_cta(creative.cta, draw, content_width)
    cta_x = content_left
    cta_y = content_bottom - pill_height
    if y + 16 > cta_y:
        raise RenderError("Insufficient vertical space for CTA overlay")

    pill_box = (cta_x, cta_y, cta_x + pill_width, cta_y + pill_height)
    draw.rounded_rectangle(pill_box, radius=pill_height // 2, fill=cta_fill)

    text_bbox = draw.textbbox((0, 0), cta_label, font=cta_font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]
    text_x = cta_x + (pill_width - text_width) / 2
    text_y = cta_y + (pill_height - text_height) / 2
    draw.text((text_x, text_y), cta_label, font=cta_font, fill=cta_text_color)

    base_contrast = _contrast_ratio(
        _relative_luminance(text_color), _relative_luminance(safe_fill[:3])
    )
    cta_contrast = _contrast_ratio(
        _relative_luminance(cta_text_color), _relative_luminance(cta_fill[:3])
    )
    if base_contrast < CONTRAST_THRESHOLD or cta_contrast < CONTRAST_THRESHOLD:
        raise RenderError("Overlay contrast below legibility threshold")

    composite = base.convert("RGB")
    composite.paste(overlay, mask=overlay.split()[-1])

    overlay_meta = {
        "headline": {
            "text": creative.headline,
            "font_size": getattr(headline_font, "size", HEADLINE_FONT_RANGE[0]),
            "lines": headline_lines,
            "box": [
                int(content_left),
                int(headline_top),
                int(content_right),
                int(headline_bottom),
            ]
            if headline_lines
            else [],
        },
        "subcopy": {
            "text": creative.subcopy,
            "font_size": getattr(subcopy_font, "size", SUBCOPY_FONT_RANGE[0]),
            "lines": subcopy_lines,
            "box": [
                int(content_left),
                int(subcopy_top),
                int(content_right),
                int(subcopy_bottom),
            ]
            if subcopy_lines
            else [],
        },
        "cta": {
            "text": cta_label,
            "font_size": getattr(cta_font, "size", CTA_FONT_RANGE[0]),
            "box": [int(pill_box[0]), int(pill_box[1]), int(pill_box[2]), int(pill_box[3])],
            "text_color": _rgb_to_hex(cta_text_color),
            "background": _rgb_to_hex(cta_fill[:3]),
            "contrast_ratio": round(cta_contrast, 2),
        },
        "safe_area": {
            "margin": SAFE_AREA_MARGIN,
            "box": [safe_left, safe_top, safe_right, safe_bottom],
            "canvas": list(CANVAS_SIZE),
        },
        "contrast": {
            "text_color": _rgb_to_hex(text_color),
            "background": _rgb_to_hex(safe_fill[:3]),
            "ratio": round(base_contrast, 2),
        },
    }
    return composite, overlay_meta


def _iter_creative_dicts(source: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(source, list):
        for item in source:
            yield from _iter_creative_dicts(item)
        return
    if not isinstance(source, dict):
        return
    candidate_keys = {
        "headline",
        "Headline",
        "visual",
        "Visual",
        "image_prompt",
        "cta",
        "call_to_action",
    }
    if any(key in source for key in candidate_keys):
        yield source
    for key in (
        "concepts",
        "items",
        "creatives",
        "data",
        "selected_for_imagery",
        "imagery_queue",
    ):
        value = source.get(key)
        if isinstance(value, list):
            for item in value:
                yield from _iter_creative_dicts(item)
    for value in source.values():
        if isinstance(value, (dict, list)):
            yield from _iter_creative_dicts(value)


def _find_value(data: Any, keys: Sequence[str]) -> Any:
    if isinstance(data, dict):
        for key in keys:
            if key in data:
                value = data[key]
                if value not in (None, ""):
                    return value
        for value in data.values():
            result = _find_value(value, keys)
            if result not in (None, ""):
                return result
    if isinstance(data, list):
        for item in data:
            result = _find_value(item, keys)
            if result not in (None, ""):
                return result
    return None


def _extract_text(creative: Dict[str, Any], keys: Sequence[str], default: str = "") -> str:
    value = _find_value(creative, keys)
    if isinstance(value, (int, float)):
        value = str(value)
    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed:
            return trimmed
    return default


def _extract_prompt(creative: Dict[str, Any], headline: str) -> str:
    prompt_parts: List[str] = []
    prompt_value = _find_value(creative, ["image_prompt", "visual_prompt", "visual", "prompt"])
    if isinstance(prompt_value, str) and prompt_value.strip():
        prompt_parts.append(prompt_value.strip())
    elif isinstance(prompt_value, list):
        for item in prompt_value:
            if isinstance(item, str) and item.strip():
                prompt_parts.append(item.strip())
    if headline:
        prompt_parts.append(headline)
    angle = _extract_text(creative, ["angle", "Angle"], "")
    if angle:
        prompt_parts.append(f"Angle: {angle}")
    audience = _extract_text(creative, ["audience_fit", "Audience Fit"], "")
    if audience:
        prompt_parts.append(f"Audience focus: {audience}")
    prompt_parts.append(DEFAULT_PROMPT_SUFFIX)
    seen: set[str] = set()
    ordered: List[str] = []
    for part in prompt_parts:
        normalized = part.strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(part.strip())
    return ", ".join(ordered)


def _requires_imagery(creative: Dict[str, Any]) -> bool:
    flag_keys = [
        "needs_image",
        "requires_image",
        "image_required",
        "render_image",
        "has_image",
        "image",
    ]
    for key in flag_keys:
        value = _find_value(creative, [key])
        if isinstance(value, bool) and value:
            return True
        if isinstance(value, (int, float)) and value:
            return True
        if isinstance(value, str) and value.strip().lower() in {
            "yes",
            "true",
            "required",
            "imagery",
            "render",
            "y",
            "1",
        }:
            return True
    prompt_value = _find_value(creative, ["image_prompt", "visual_prompt", "visual"])
    if isinstance(prompt_value, str) and prompt_value.strip():
        return True
    if isinstance(prompt_value, list) and any(
        isinstance(item, str) and item.strip() for item in prompt_value
    ):
        return True
    return False


def _normalize_concept_id(raw_id: Any, fallback_index: int) -> str:
    if isinstance(raw_id, (int, float)):
        return f"concept_{int(raw_id):02d}"
    if isinstance(raw_id, str):
        cleaned = raw_id.strip()
        if cleaned:
            normalized = cleaned.replace(" ", "_").lower()
            digits = "".join(ch for ch in normalized if ch.isdigit())
            if normalized.startswith("concept_") and digits:
                return f"concept_{int(digits):02d}"
            if digits:
                return f"concept_{int(digits):02d}"
            return f"concept_{normalized}"
    return f"concept_{fallback_index:02d}"


def _collect_creatives(run) -> List[CreativePayload]:  # type: ignore[no-untyped-def]
    sources: List[Any] = []
    telemetry = getattr(run, "telemetry", None) or {}
    if isinstance(telemetry, dict):
        creatives_section = telemetry.get("creatives")
        if creatives_section is not None:
            sources.append(creatives_section)
    payload = getattr(run, "input_payload", None) or {}
    if isinstance(payload, dict):
        if "images" in payload:
            sources.append(payload["images"])
        if "creatives" in payload:
            sources.append(payload["creatives"])
    results: List[CreativePayload] = []
    seen: set[str] = set()
    index = 1
    for source in sources:
        for creative in _iter_creative_dicts(source):
            if not isinstance(creative, dict):
                continue
            if not _requires_imagery(creative):
                continue
            concept_id = _normalize_concept_id(
                _find_value(creative, ["concept_id", "id", "#", "index"]),
                index,
            )
            if concept_id in seen:
                index += 1
                continue
            headline = _extract_text(
                creative,
                ["headline", "Headline", "title", "Title", "primary_text"],
                "Brand-safe headline",
            )
            subcopy = _extract_text(
                creative,
                ["subcopy", "body", "description", "supporting_copy", "secondary_text"],
                "",
            )
            cta = _extract_text(
                creative,
                ["cta", "call_to_action", "cta_text", "CTA"],
                "Learn More",
            )
            prompt = _extract_prompt(creative, headline)
            payload_obj = CreativePayload(
                concept_id=concept_id,
                headline=headline,
                subcopy=subcopy,
                cta=cta,
                prompt=prompt,
                raw=creative,
            )
            results.append(payload_obj)
            seen.add(concept_id)
            index += 1
    return results


class ImageStage(BaseStage):
    name = "images"

    def execute(self) -> Dict[str, Any]:
        self.ensure_budget(50.0)
        run = self.context.run
        session = self.context.session

        creatives = _collect_creatives(run)
        provider = os.getenv("ANDRONOMA_IMAGE_PROVIDER", DEFAULT_PROVIDER)
        unit_cost = float(os.getenv("ANDRONOMA_IMAGE_UNIT_COST", "0.08"))

        if not creatives:
            return {
                "provider": provider,
                "requested": 0,
                "rendered": 0,
                "failures": 0,
                "cost": 0.0,
                "assets": [],
                "failures_detail": {},
            }

        budgets = run.budgets or {}
        stage_budget = float(budgets.get(self.name, 0.0) or 0.0)
        if stage_budget <= 0.0:
            stage_budget = float(os.getenv("ANDRONOMA_IMAGE_BUDGET", "500"))
        stage_state = next((state for state in run.stages if state.name == self.name), None)
        spent = float(stage_state.budget_spent if stage_state else 0.0)
        budget_remaining = max(stage_budget - spent, 0.0)

        output_dir = Path("outputs/creatives/images")
        output_dir.mkdir(parents=True, exist_ok=True)

        renderer = RenderClient(provider, unit_cost=unit_cost)
        staged: List[Tuple[CreativePayload, Path, float, Dict[str, Any]]] = []
        telemetry_assets: List[Dict[str, Any]] = []
        failures: Dict[str, str] = {}
        total_cost = 0.0

        try:
            for idx, creative in enumerate(creatives, start=1):
                concept_id = creative.concept_id or f"concept_{idx:02d}"
                filename = f"{concept_id}.jpg"
                file_path = output_dir / filename
                try:
                    image, cost, render_meta = renderer.render(creative, budget_remaining)
                    budget_remaining -= cost
                    total_cost += cost
                    composite, overlay_meta = compose_overlay(image, creative)
                    composite.save(file_path, format="JPEG", quality=95)
                    overlay_meta.update(
                        {
                            "concept_id": concept_id,
                            "prompt": creative.prompt,
                            "render": render_meta,
                        }
                    )
                    staged.append((creative, file_path, cost, overlay_meta))
                    telemetry_assets.append(
                        {
                            "concept_id": concept_id,
                            "path": str(file_path),
                            "cost": round(cost, 4),
                            "contrast_ratio": overlay_meta["contrast"]["ratio"],
                            "provider": render_meta.get("mode", renderer.active_provider),
                        }
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    failures[concept_id] = str(exc)
                    if file_path.exists():
                        file_path.unlink(missing_ok=True)
            if failures:
                for _, file_path, _, _ in staged:
                    if file_path.exists():
                        file_path.unlink(missing_ok=True)
                raise RenderError(
                    "Image rendering failed: "
                    + "; ".join(f"{cid}: {error}" for cid, error in failures.items())
                )

            for creative, file_path, cost, overlay_meta in staged:
                record = AssetRecord(
                    id=uuid.uuid4(),
                    run_id=run.id,
                    stage=self.name,
                    asset_type="image",
                    storage_key=str(file_path),
                    extra={
                        "concept_id": overlay_meta["concept_id"],
                        "overlay": {
                            "headline": overlay_meta["headline"],
                            "subcopy": overlay_meta["subcopy"],
                            "cta": overlay_meta["cta"],
                            "safe_area": overlay_meta["safe_area"],
                            "contrast": overlay_meta["contrast"],
                        },
                        "prompt": overlay_meta["prompt"],
                        "render": overlay_meta["render"],
                        "cost": cost,
                    },
                )
                session.add(record)
            if stage_state:
                stage_state.budget_spent = spent + total_cost
            session.commit()
        finally:
            renderer.close()

        telemetry = {
            "provider": renderer.active_provider,
            "requested": len(creatives),
            "rendered": len(staged),
            "failures": len(failures),
            "cost": round(total_cost, 4),
            "assets": telemetry_assets,
            "failures_detail": failures,
        }
        return telemetry
