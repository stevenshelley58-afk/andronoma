"""Creative generation stage that produces scroll-stopper concepts."""
from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import cycle
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

from outputs.csv import write_records
from shared.stages.base import BaseStage

from .dup_guard import DuplicateGuard


CREATIVE_BUCKETS: List[str] = [
    "Shock",
    "Proof / Engineering",
    "Emotional Story",
    "Absurd / Surreal",
    "Pure Aesthetic",
]

REQUIRED_BLOCKERS: List[str] = [
    "price",
    "durability",
    "scam/legitimacy",
    "fit/dimension",
    "delivery",
    "style mismatch",
    "returns friction",
    "commitment fear",
    "out-of-stock",
]

BLOCKER_LABELS: Dict[str, str] = {
    "price": "Price",
    "durability": "Durability",
    "scam/legitimacy": "Scam / Legitimacy",
    "fit/dimension": "Fit / Dimension",
    "delivery": "Delivery",
    "style mismatch": "Style Mismatch",
    "returns friction": "Returns Friction",
    "commitment fear": "Commitment Fear",
    "out-of-stock": "Out Of Stock",
}

BLOCKER_ALIASES: Dict[str, str] = {
    "price": "price doubts",
    "durability": "durability fear",
    "scam/legitimacy": "trust jitters",
    "fit/dimension": "fit guesswork",
    "delivery": "delivery waits",
    "style mismatch": "style clash",
    "returns friction": "return hassle",
    "commitment fear": "commitment nerves",
    "out-of-stock": "restock panic",
}

PROMO_TERMS = [
    "sale",
    "discount",
    "limited time",
    "hurry",
    "deal",
    "promo",
    "% off",
    "clearance",
]

DEFAULT_PILLARS: List[Dict[str, str]] = [
    {
        "pillar": "Crafted Longevity",
        "evidence": "Lab stress tests outlasting category norms",
        "implications": "Sell the lifetime value over throwaway swaps",
    },
    {
        "pillar": "Human-Centered Fit",
        "evidence": "Thousands of data points across body types",
        "implications": "Reassure tailored comfort without tailoring",
    },
    {
        "pillar": "Transparent Sourcing",
        "evidence": "Certified partner supply chain with traceability",
        "implications": "Invite the shopper into the build story",
    },
]

DEFAULT_BLOCKER_MESSAGES: Dict[str, str] = {
    "price": "Show lifetime ROI that dwarfs upfront spend",
    "durability": "Highlight stress data and repair-friendly design",
    "scam/legitimacy": "Lean on certifications and social proof",
    "fit/dimension": "Show flexible sizing intelligence and trials",
    "delivery": "Publish transparent timelines with proactive alerts",
    "style mismatch": "Demonstrate modular styling in real spaces",
    "returns friction": "Spell out hassle-free returns and humans on support",
    "commitment fear": "Offer try-now framing with low-risk entry",
    "out-of-stock": "Explain small-batch drops and restock alerts",
}

DEFAULT_MOTIVATIONS: Dict[str, List[str]] = {
    "Functional": [
        "remove constant maintenance",
        "solve daily friction",
    ],
    "Emotional": [
        "feel proud inviting friends over",
        "trade chaos for calm",
    ],
    "Social": [
        "share conversation-worthy craftsmanship",
    ],
}


def _short_phrase(text: str, *, max_words: int = 3, default: str = "Creator") -> str:
    tokens = [re.sub(r"[^A-Za-z0-9']", "", token) for token in text.split()]
    clean = [token for token in tokens if token]
    if not clean:
        return default
    return " ".join(clean[:max_words])


def _canonical_blocker(raw: str) -> Optional[str]:
    text = raw.lower()
    if not text:
        return None
    if "price" in text or "cost" in text or "$" in text:
        return "price"
    if "durab" in text or "wear" in text or "last" in text:
        return "durability"
    if "scam" in text or "legit" in text or "trust" in text or "fraud" in text:
        return "scam/legitimacy"
    if "fit" in text or "size" in text or "dimension" in text or "space" in text:
        return "fit/dimension"
    if "ship" in text or "deliver" in text or "arrival" in text or "timeline" in text:
        return "delivery"
    if "style" in text or "match" in text or "look" in text or "aesthetic" in text:
        return "style mismatch"
    if "return" in text or "refund" in text or "exchange" in text:
        return "returns friction"
    if "commit" in text or "long" in text or "contract" in text or "subscription" in text:
        return "commitment fear"
    if "stock" in text or "sold" in text or "backorder" in text or "waitlist" in text:
        return "out-of-stock"
    return None


def _human_blocker_label(blocker: str) -> str:
    return BLOCKER_LABELS.get(blocker, blocker.title())


def _join_blockers(blockers: Sequence[str]) -> str:
    humanised = [_human_blocker_label(blocker) for blocker in blockers]
    return " · ".join(dict.fromkeys(humanised))


def _sanitize_headline(headline: str) -> str:
    words = headline.split()
    if len(words) < 3:
        words.extend(["Value"] * (3 - len(words)))
    if len(words) > 10:
        words = words[:10]
    capped = " ".join(words)
    return capped.strip().rstrip(",")


def _sanitize_sentence(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


@dataclass
class AudienceSegment:
    identifier: str
    name: str
    persona: str
    motivation: str
    blockers: List[str]
    message_angle: str
    proof: str

    @classmethod
    def from_row(cls, row: Mapping[str, str]) -> "AudienceSegment":
        identifier = str(row.get("#") or row.get("Id") or row.get("id") or row.get("identifier") or "")
        name = row.get("Audience Name") or row.get("name") or identifier or "Audience"
        persona = row.get("Who They Are") or row.get("persona") or name
        motivation = row.get("Primary Motivation") or row.get("motivation") or "stay inspired"
        blockers_text = row.get("Top 2 Blockers") or row.get("blockers") or ""
        angle = row.get("Message Angle") or row.get("angle") or "Value-first reassurance"
        proof = row.get("Proof/Offer") or row.get("proof") or "Customer stories prove staying power"
        blockers = []
        for token in re.split(r"[,/·]| and ", blockers_text):
            canon = _canonical_blocker(token.strip())
            if canon:
                blockers.append(canon)
        if not blockers:
            blockers = ["price"]
        identifier = identifier or _short_phrase(name, max_words=2)
        return cls(
            identifier=identifier.strip(),
            name=name.strip(),
            persona=persona.strip(),
            motivation=_sanitize_sentence(motivation),
            blockers=blockers,
            message_angle=_sanitize_sentence(angle),
            proof=_sanitize_sentence(proof),
        )

    @property
    def fit_label(self) -> str:
        return f"{self.identifier} - {self.short_name}"

    @property
    def short_name(self) -> str:
        return _short_phrase(self.name, max_words=3, default="Audience")

    @property
    def descriptor(self) -> str:
        return _sanitize_sentence(self.persona or self.name)


@dataclass
class ProcessArtifacts:
    pillars: List[Dict[str, str]]
    motivation_map: Dict[str, List[str]]
    blocker_messages: Dict[str, str]
    market_summary: str
    brand_voice: str

    @classmethod
    def from_telemetry(
        cls, data: Mapping[str, Any], fallback_voice: str = "assured and optimistic"
    ) -> "ProcessArtifacts":
        pillars = [dict(pillar) for pillar in data.get("positioning_pillars", []) if isinstance(pillar, Mapping)]
        if not pillars:
            pillars = DEFAULT_PILLARS

        motivation_map: Dict[str, List[str]] = {}
        raw_map = data.get("motivation_map", {})
        if isinstance(raw_map, Mapping):
            for key, value in raw_map.items():
                if isinstance(value, (list, tuple)):
                    motivation_map[str(key)] = [
                        _sanitize_sentence(str(entry)) for entry in value if str(entry).strip()
                    ]
        if not motivation_map:
            motivation_map = DEFAULT_MOTIVATIONS

        blocker_messages: Dict[str, str] = {}
        for blocker in data.get("blockers", []) or []:
            if isinstance(blocker, Mapping):
                name = blocker.get("blocker") or blocker.get("name") or ""
                canon = _canonical_blocker(str(name))
                if canon:
                    blocker_messages[canon] = _sanitize_sentence(
                        blocker.get("counter")
                        or blocker.get("counter_message")
                        or blocker.get("resolution")
                        or DEFAULT_BLOCKER_MESSAGES.get(canon, "Deliver concrete reassurance")
                    )
        for key, value in DEFAULT_BLOCKER_MESSAGES.items():
            blocker_messages.setdefault(key, value)

        market_summary = _sanitize_sentence(str(data.get("market_summary", ""))) or (
            "Category in flux with whitespace around trusted, durable craft."
        )
        brand_voice = _sanitize_sentence(str(data.get("brand_voice", fallback_voice)))

        return cls(
            pillars=pillars,
            motivation_map=motivation_map,
            blocker_messages=blocker_messages,
            market_summary=market_summary,
            brand_voice=brand_voice,
        )

    @property
    def proof_points(self) -> List[str]:
        points: List[str] = []
        for pillar in self.pillars:
            evidence = pillar.get("evidence") or pillar.get("implications") or pillar.get("pillar")
            if evidence:
                points.append(_sanitize_sentence(str(evidence)))
        if not points:
            points = ["Independent testing confirmed long-haul performance"]
        return points

    @property
    def product_keywords(self) -> List[str]:
        keywords: List[str] = []
        for pillar in self.pillars:
            keywords.append(_short_phrase(str(pillar.get("pillar", "Hero Piece")), max_words=2, default="Hero"))
        if not keywords:
            keywords = ["Hero Piece"]
        return keywords

    @property
    def motivations(self) -> List[str]:
        values: List[str] = []
        for entries in self.motivation_map.values():
            values.extend(entries)
        if not values:
            values = ["Feel confident choosing better"]
        return values

    @property
    def cta_tone(self) -> str:
        voice = self.brand_voice.lower()
        if "playful" in voice:
            return "Playful invitation"
        if "bold" in voice or "confident" in voice:
            return "Confident nudge"
        if "calm" in voice or "soothing" in voice:
            return "Calming reassurance"
        return "Assured guidance"


@dataclass
class CreativeConcept:
    index: int
    bucket: str
    headline: str
    visual: str
    angle: str
    blocker: str
    audience_fit: str
    audience_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_csv_row(self) -> Dict[str, str]:
        return {
            "#": self.index,
            "Headline": self.headline,
            "Visual": self.visual,
            "Angle": self.angle,
            "Blocker": self.blocker,
            "Audience Fit": self.audience_fit,
        }


HEADLINE_TEMPLATES: Dict[str, List[str]] = {
    "Shock": [
        "{audience_phrase} Never Expected {product_phrase}",
        "Wait Until You See {product_phrase}",
        "This {product_phrase} Breaks {blocker_alias}",
        "Proof {audience_phrase} Gasped At {product_phrase}",
        "No One Saw {product_phrase} Coming",
        "First Reaction To {product_phrase}",
        "Turns {blocker_alias} Upside Down",
        "Why {audience_phrase} Can't Ignore {product_phrase}",
        "{product_phrase} That Stops Scrolls",
        "Suddenly {audience_phrase} Trust {product_phrase}",
    ],
    "Proof / Engineering": [
        "Precision Crafted {product_phrase}",
        "Lab Proven {product_phrase}",
        "Engineered Confidence In {product_phrase}",
        "Test Bench Validated {product_phrase}",
        "Metric Driven {product_phrase} Upgrade",
        "Certified Strength For {audience_phrase}",
        "Why Engineers Back {product_phrase}",
        "Spec Sheet Wins With {product_phrase}",
        "Design Lab Perfected {product_phrase}",
        "Reliability Report On {product_phrase}",
    ],
    "Emotional Story": [
        "{audience_phrase} Finally Feels Seen",
        "The Moment {audience_phrase} Breathed",
        "A Quiet Win For {audience_phrase}",
        "From Doubt To Daily Joy",
        "How {audience_phrase} Found Ease",
        "The Promise {audience_phrase} Needed",
        "Shareable Joy For {audience_phrase}",
        "A Story {audience_phrase} Retell",
        "What {audience_phrase} Tell Friends",
        "Small Ritual Big Relief",
    ],
    "Absurd / Surreal": [
        "{product_phrase} Meets Moonlight",
        "Dream Logic For {audience_phrase}",
        "When {audience_phrase} Bend Physics",
        "Gravity Takes A Break",
        "{product_phrase} In Alternate Orbit",
        "Surreal Signals For {audience_phrase}",
        "{audience_phrase} Rewrite {blocker_alias}",
        "Mythic {product_phrase} Cameo",
        "Unexpected Portal To {motivation_phrase}",
        "Outlandish Proof Of {product_phrase}",
    ],
    "Pure Aesthetic": [
        "Soft Light On {product_phrase}",
        "Minimal Lines Maximum Calm",
        "Palette Reset For {audience_phrase}",
        "Tactile Glow Of {product_phrase}",
        "Still Life With {product_phrase}",
        "Quiet Luxury For {audience_phrase}",
        "Balanced Form Guides {audience_phrase}",
        "Monochrome Focus On {product_phrase}",
        "Texture Study Inspires {audience_phrase}",
        "Gallery Ready Take On {product_phrase}",
    ],
}


VISUAL_TEMPLATES: Dict[str, List[str]] = {
    "Shock": [
        "High-contrast split screen showing before {blocker_alias_lower} and after glow with {product_phrase_lower} in focus.",
        "Macro of {product_phrase_lower} blasting through symbolic {blocker_alias_lower} wall with sparks.",
        "Freeze-frame reaction of {audience_descriptor_lower} witnessing luminous {product_phrase_lower} reveal.",
        "Slow-motion drop of {product_phrase_lower} onto shattering {blocker_alias_lower} icon over neon grid.",
        "Top-down scene where {product_phrase_lower} interrupts chaotic {blocker_alias_lower} clutter on desk.",
        "Dramatic light sweep over {product_phrase_lower} with blurred crowd gasping in foreground.",
        "Storm of negative keywords dissolving while {product_phrase_lower} glows center frame.",
        "Unexpected rooftop setting: {product_phrase_lower} hovering over city nightscape with bold captions.",
        "Flash photography aesthetic capturing {audience_descriptor_lower} mid gasp holding {product_phrase_lower}.",
        "Comic-book burst framing {product_phrase_lower} smashing oversized {blocker_alias_lower} typography.",
    ],
    "Proof / Engineering": [
        "Precision diagram overlaying {product_phrase_lower} with callouts to {proof_phrase_lower} data points.",
        "Bench test rig with instruments measuring {product_phrase_lower} performance, readouts visible.",
        "Cutaway render exposing inner structure of {product_phrase_lower} highlighting {blocker_resolution_lower}.",
        "Technician hands calibrating {product_phrase_lower} beside spec sheet floating in frame.",
        "Thermal camera view contrasting {product_phrase_lower} efficiency against outdated option.",
        "Factory floor spotlight on {product_phrase_lower} passing inspection stamps and certifications.",
        "Blueprint aesthetic layering {product_phrase_lower} measurements over clean grid.",
        "Macro materials shot showing texture proof with annotation bubbles calling out strengths.",
        "Data viz style bars comparing {product_phrase_lower} metrics beating {blocker_alias_lower} worry.",
        "Microscope-inspired imagery zooming into {product_phrase_lower} materials verifying durability.",
    ],
    "Emotional Story": [
        "Golden hour portrait of {audience_descriptor_lower} interacting with {product_phrase_lower} in lived-in space.",
        "Before-and-after storyboard panels showing {audience_descriptor_lower} moving from stress to ease.",
        "Close-up of hands embracing {product_phrase_lower} with handwritten note sharing relief.",
        "Candid living room scene where {audience_descriptor_lower} celebrates micro-win with {product_phrase_lower}.",
        "Soft focus vignette capturing {audience_descriptor_lower} gifting {product_phrase_lower} to loved one.",
        "Journal page style layout narrating {motivation_phrase_lower} moment supported by {product_phrase_lower}.",
        "Warm kitchen tableau showing {audience_descriptor_lower} enjoying calm thanks to {product_phrase_lower}.",
        "Series of Polaroids documenting {audience_descriptor_lower} weekly ritual anchored by {product_phrase_lower}.",
        "Cinematic over-the-shoulder shot revealing {blocker_resolution_lower} turning point.",
        "Intimate detail of {product_phrase_lower} in use framed by relaxed expression on {audience_descriptor_lower}.",
    ],
    "Absurd / Surreal": [
        "Floating {product_phrase_lower} orbiting miniature planet that embodies {blocker_alias_lower} breaking apart.",
        "Dreamlike hallway of doors where {audience_descriptor_lower} opens portal to {motivation_phrase_lower} world.",
        "Levitation scene with {product_phrase_lower} defying gravity above melted clock inspired by surrealism.",
        "Whimsical collage of {product_phrase_lower} sprouting botanical forms that swallow {blocker_alias_lower} icon.",
        "Celestial chalkboard showing constellations shaped like {product_phrase_lower} guiding {audience_phrase_lower}.",
        "Double exposure merging {audience_descriptor_lower} silhouette with flowing {proof_phrase_lower} typography.",
        "Hyper-saturated landscape where {product_phrase_lower} beams signal wiping {blocker_alias_lower} graffiti.",
        "Retro sci-fi set of {audience_descriptor_lower} piloting {product_phrase_lower} through color warp tunnel.",
        "Playful scale shift turning {product_phrase_lower} into architectural landmark towering over doubts.",
        "Surreal mirror maze reflecting infinite {product_phrase_lower} angles dissolving uncertainty.",
    ],
    "Pure Aesthetic": [
        "Studio still life with {product_phrase_lower} on travertine plinth, soft diffused lighting.",
        "Tonal gradient backdrop emphasizing silhouette of {product_phrase_lower} with gentle rim light.",
        "Flat lay of complementary textures framing {product_phrase_lower} in calm palette.",
        "Architectural nook featuring {product_phrase_lower} styled with minimal props for timeless feel.",
        "Macro focus on material grain of {product_phrase_lower} paired with muted typography.",
        "Top-lit composition capturing {product_phrase_lower} casting elegant shadow geometry.",
        "Soft morning light through sheer curtains highlighting {product_phrase_lower} on pedestal.",
        "Monochrome setup isolating {product_phrase_lower} with high-end editorial vibe.",
        "Symmetrical arrangement aligning {product_phrase_lower} with linear design cues.",
        "Gallery inspired negative space giving {product_phrase_lower} room to breathe.",
    ],
}


ANGLE_TEMPLATES: Dict[str, List[str]] = {
    "Shock": [
        "Shock value proof that {blocker_resolution} lands instantly for {audience_phrase}.",
        "Expose hidden strength so {audience_phrase} forget {blocker_alias_lower} overnight.",
        "Live reaction storytelling flipping {blocker_alias_lower} script with undeniable proof.",
        "Reveal the unexpected engineering that makes {motivation_phrase_lower} effortless.",
        "Interrupt assumptions—show {audience_phrase} the moment {blocker_alias_lower} vanishes.",
        "Let raw reactions validate why this {product_phrase_lower} rewrites the rules.",
        "Demonstrate instant payoff and leave skeptics no room to doubt.",
        "Frame the jaw-drop insight: transparency plus performance crushes {blocker_alias_lower}.",
        "High drama entry point for social feeds craving something undeniably real.",
        "Spill the secret reveal that powers word-of-mouth among {audience_phrase_lower}.",
    ],
    "Proof / Engineering": [
        "Lead with quantifiable gains and back every promise with {proof_phrase_lower}.",
        "Show the build story so {audience_phrase_lower} trusts every seam.",
        "Engineer-first messaging that dismantles {blocker_alias_lower} via transparent testing.",
        "Walk through the data narrative proving this isn't marketing fluff.",
        "Underscore obsessive craft translating to measurable ROI for the buyer.",
        "Bridge skepticism with lab visuals and honest materials science.",
        "Translate spec-sheet wins into plain language for {audience_phrase_lower}.",
        "Highlight third-party validation to neutralise {blocker_alias_lower} whispers.",
        "Invite the viewer into the workshop—evidence beats hype.",
        "Anchor the story in verifiable credentials and long-haul reliability.",
    ],
    "Emotional Story": [
        "Narrate the before/after arc where {audience_phrase_lower} finds calm.",
        "Center the human moment that proves value beyond specs.",
        "Let testimonials and rituals carry the message, not hype.",
        "Paint the feeling of {motivation_phrase_lower} coming to life daily.",
        "Humanise the proof—show faces, spaces, and small victories.",
        "Lean into community language that dissolves {blocker_alias_lower} stigma.",
        "Storytell the ripple effect after switching to the new solution.",
        "Celebrate belonging and identity cues important to {audience_phrase_lower}.",
        "Make the audience the hero; the product simply unlocks the scene.",
        "Capture the ""told my friend"" energy policy teams love.",
    ],
    "Absurd / Surreal": [
        "Use cultural surrealism to jolt attention while reinforcing {blocker_resolution}.",
        "Lean into dreamlike metaphor so {audience_phrase_lower} rethinks the category.",
        "Highlight whitespace—this brand plays where competitors won't.",
        "Turn {blocker_alias_lower} into a character the product defeats with style.",
        "Fuse artful nonsense with real proof to stay memorable.",
        "Signal creative bravery backed by transparent receipts.",
        "Offer playful commentary on tired industry tropes and flip them.",
        "Reassure through humor: if we can bend physics, we can solve {blocker_alias_lower}.",
        "Mash unexpected references to spark shares among {audience_phrase_lower}.",
        "Deliver a scroll-stopping loop fans replay and discuss.",
    ],
    "Pure Aesthetic": [
        "Lead with visual serenity that mirrors the product experience.",
        "Let premium craft and light tell the durability story implicitly.",
        "Frame the object like design media would—instant credibility.",
        "Minimal copy, maximal texture proof of quality.",
        "Use color theory to nod at {motivation_phrase_lower} without shouting.",
        "Invite the viewer to imagine the piece in their curated space.",
        "Show modular styling to counter {blocker_alias_lower} quietly.",
        "Focus on tactility and restraint—luxury without ego.",
        "Deliver editorial polish that signals trusted brand energy.",
        "Let silence and negative space reassure anxious buyers.",
    ],
}


class CreativeStage(BaseStage):
    name = "creatives"

    def execute(self) -> Dict[str, Any]:
        self.ensure_budget(200.0)

        run = self.context.run
        run_payload = run.input_payload or {}
        raw_config = run_payload.get("config", {})
        config = raw_config if isinstance(raw_config, Mapping) else {}
        metadata = config.get("metadata") if isinstance(config.get("metadata"), Mapping) else {}

        promo_source = (
            run_payload.get("PROMO_ALLOWED")
            or run_payload.get("promo_allowed")
            or (metadata.get("PROMO_ALLOWED") if isinstance(metadata, Mapping) else None)
            or (metadata.get("promo_allowed") if isinstance(metadata, Mapping) else None)
        )
        promo_allowed = bool(promo_source) or (
            str(run_payload.get("PROMO_ALLOWED", "")).lower() in {"1", "true", "yes", "on"}
        )

        processing_data = (run.telemetry or {}).get("process", {}) if run.telemetry else {}
        fallback_voice = run_payload.get("brand_voice")
        if not fallback_voice and isinstance(config, Mapping):
            fallback_voice = config.get("brand_voice")
        artifacts = ProcessArtifacts.from_telemetry(processing_data, fallback_voice or "assured and optimistic")

        audiences = self._load_audiences(run)
        if not audiences:
            raise ValueError("Creative generation requires audience inputs; none found")

        concepts, guard_stats, coverage, tone_metrics, hints = self._build_concepts(
            audiences=audiences,
            artifacts=artifacts,
            promo_allowed=promo_allowed,
        )

        csv_path = write_records(Path("outputs/creatives/scroll_stoppers.csv"), [concept.to_csv_row() for concept in concepts])
        manifest_path = Path("outputs/creatives/manifest.json")
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest = {
            "totals": {
                "concepts": len(concepts),
                "unique_audiences": coverage["unique_audiences"],
            },
            "bucket_counts": coverage["bucket_counts"],
            "blocker_counts": coverage["blocker_counts"],
            "duplicate_guard": guard_stats,
            "brand_voice": {
                "tone": artifacts.brand_voice,
                "cta_tone": artifacts.cta_tone,
            },
            "selection_hints": hints,
        }
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        telemetry: Dict[str, Any] = {
            "concepts_generated": len(concepts),
            "csv_path": str(csv_path),
            "manifest_path": str(manifest_path),
            "bucket_counts": coverage["bucket_counts"],
            "blocker_counts": coverage["blocker_counts"],
            "audience_unique": coverage["unique_audiences"],
            "duplicate_guard": guard_stats,
            "brand_tone": tone_metrics,
        }
        return telemetry

    def _load_audiences(self, run) -> List[AudienceSegment]:
        telemetry = run.telemetry or {}
        audience_data = telemetry.get("audiences", {}) if isinstance(telemetry, Mapping) else {}
        records: List[AudienceSegment] = []

        if isinstance(audience_data, Mapping):
            for record in audience_data.get("records", []) or []:
                if isinstance(record, Mapping):
                    records.append(AudienceSegment.from_row(record))
            for record in audience_data.get("rows", []) or []:
                if isinstance(record, Mapping):
                    records.append(AudienceSegment.from_row(record))
            csv_hint = audience_data.get("csv_path") or audience_data.get("path")
            if csv_hint:
                records.extend(self._load_audience_csv(Path(csv_hint)))

        default_csv = Path("outputs/audiences/audiences_master.csv")
        records.extend(self._load_audience_csv(default_csv))

        if not records:
            records = self._default_audiences()

        unique: Dict[str, AudienceSegment] = {}
        for segment in records:
            unique[segment.fit_label] = segment
        return list(unique.values())

    def _load_audience_csv(self, path: Path) -> List[AudienceSegment]:
        segments: List[AudienceSegment] = []
        for row in _load_csv_rows(path):
            segments.append(AudienceSegment.from_row(row))
        return segments

    def _default_audiences(self) -> List[AudienceSegment]:
        defaults = [
            {
                "#": "A1",
                "Audience Name": "Design-led renters",
                "Who They Are": "Urban renters upgrading essentials without renovation",
                "Primary Motivation": "Make small spaces feel intentional",
                "Top 2 Blockers": "Price / Style",
                "Message Angle": "Show multi-use setups that justify every inch",
                "Proof/Offer": "Tagged IG lofts using the product",
            },
            {
                "#": "A2",
                "Audience Name": "DIY upgraders",
                "Who They Are": "Hands-on homeowners solving chronic pain points",
                "Primary Motivation": "Invest once in durable fixes",
                "Top 2 Blockers": "Durability / Returns",
                "Message Angle": "Pair lab data with lifetime service",
                "Proof/Offer": "Warranty snapshots + stress test clips",
            },
            {
                "#": "A3",
                "Audience Name": "Skeptical gifters",
                "Who They Are": "Shoppers vetting legitimacy before gifting",
                "Primary Motivation": "Earn trust with transparent sourcing",
                "Top 2 Blockers": "Scam / Delivery",
                "Message Angle": "Show traceable build + shipping alerts",
                "Proof/Offer": "Certifications + tracking UI demo",
            },
            {
                "#": "A4",
                "Audience Name": "Compact space planners",
                "Who They Are": "Small-space stylists managing dimensions",
                "Primary Motivation": "Avoid awkward fit surprises",
                "Top 2 Blockers": "Fit / Style",
                "Message Angle": "Render dimension overlays + modular styling",
                "Proof/Offer": "AR try-on GIF + modular sets",
            },
            {
                "#": "A5",
                "Audience Name": "Delayed delivery skeptics",
                "Who They Are": "Customers burned by shipping delays",
                "Primary Motivation": "Know exact arrival",
                "Top 2 Blockers": "Delivery / Out of stock",
                "Message Angle": "Broadcast proactive updates + small batch drops",
                "Proof/Offer": "Timeline dashboard screenshot",
            },
            {
                "#": "A6",
                "Audience Name": "Trend resistant minimalists",
                "Who They Are": "Shoppers avoiding impulse trends",
                "Primary Motivation": "Invest in timeless forms",
                "Top 2 Blockers": "Commitment / Price",
                "Message Angle": "Frame cost-per-use with heirloom vibes",
                "Proof/Offer": "Cost calculator snippet",
            },
            {
                "#": "A7",
                "Audience Name": "On-the-go professionals",
                "Who They Are": "Busy pros juggling travel and returns",
                "Primary Motivation": "Zero-hassle swaps",
                "Top 2 Blockers": "Returns / Delivery",
                "Message Angle": "Highlight concierge logistics",
                "Proof/Offer": "Chat transcript showing white-glove service",
            },
            {
                "#": "A8",
                "Audience Name": "New move-in planners",
                "Who They Are": "Moving soon, fearful of stockouts",
                "Primary Motivation": "Guarantee inventory",
                "Top 2 Blockers": "Out of stock / Delivery",
                "Message Angle": "Show reserve-now pipeline",
                "Proof/Offer": "Inventory heatmap",
            },
            {
                "#": "A9",
                "Audience Name": "Style-switch skeptics",
                "Who They Are": "Worried product clashes with decor",
                "Primary Motivation": "Blend seamlessly",
                "Top 2 Blockers": "Style / Commitment",
                "Message Angle": "Demonstrate modular lookbook",
                "Proof/Offer": "Three-style carousel",
            },
            {
                "#": "A10",
                "Audience Name": "Repair avoiders",
                "Who They Are": "Tired of replacing flimsy options",
                "Primary Motivation": "Buy once",
                "Top 2 Blockers": "Durability / Scam",
                "Message Angle": "Combine teardown and reviews",
                "Proof/Offer": "Exploded view + Trustpilot",
            },
        ]
        return [AudienceSegment.from_row(row) for row in defaults]

    def _build_concepts(
        self,
        *,
        audiences: Sequence[AudienceSegment],
        artifacts: ProcessArtifacts,
        promo_allowed: bool,
    ) -> tuple[List[CreativeConcept], Dict[str, Any], Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
        guard = DuplicateGuard()
        headline_registry: set[str] = set()
        proof_cycle = cycle(artifacts.proof_points)
        product_cycle = cycle(artifacts.product_keywords)
        motivation_cycle = cycle(artifacts.motivations)

        audiences_by_blocker: Dict[str, List[AudienceSegment]] = defaultdict(list)
        for audience in audiences:
            for blocker in audience.blockers:
                audiences_by_blocker[blocker].append(audience)
        fallback_audience_cycle = cycle(audiences)

        blocker_sequence: List[str] = []
        blocker_iter = cycle(REQUIRED_BLOCKERS)
        total_required = len(CREATIVE_BUCKETS) * 10
        for _ in range(total_required):
            blocker_sequence.append(next(blocker_iter))

        concepts: List[CreativeConcept] = []
        tone_metrics = {
            "promo_allowed": promo_allowed,
            "violations": [],
            "auto_corrected": 0,
        }

        selection_hints: List[Dict[str, Any]] = []

        for idx, bucket in enumerate(self._bucket_plan()):
            blocker = blocker_sequence[idx]
            audience_pool = audiences_by_blocker.get(blocker) or []
            audience = audience_pool[idx % len(audience_pool)] if audience_pool else next(fallback_audience_cycle)
            proof_point = next(proof_cycle)
            product_keyword = next(product_cycle)
            motivation_point = audience.motivation or next(motivation_cycle)
            secondary_blocker = next(
                (b for b in audience.blockers if b != blocker),
                None,
            )

            context = self._build_context(
                audience=audience,
                product_keyword=product_keyword,
                proof_point=proof_point,
                blocker=blocker,
                motivation_point=motivation_point,
                blocker_resolution=artifacts.blocker_messages.get(blocker, DEFAULT_BLOCKER_MESSAGES[blocker]),
            )

            base_headline = self._format_template(HEADLINE_TEMPLATES[bucket], idx % 10, context)
            base_headline = self._enforce_tone(base_headline, promo_allowed, tone_metrics, field="headline")
            base_headline = self._ensure_unique_headline(base_headline, headline_registry, context)

            base_visual = self._format_template(VISUAL_TEMPLATES[bucket], idx % 10, context)
            base_angle = self._format_template(ANGLE_TEMPLATES[bucket], idx % 10, context)
            base_angle = self._enforce_tone(base_angle, promo_allowed, tone_metrics, field="angle")

            blockers_for_row = [blocker]
            if secondary_blocker and secondary_blocker not in blockers_for_row:
                blockers_for_row.append(secondary_blocker)
            blocker_field = _join_blockers(blockers_for_row)

            metadata = {
                "bucket": bucket,
                "blockers": blockers_for_row,
                "audience_name": audience.name,
                "proof_point": proof_point,
                "motivation": motivation_point,
                "blocker_resolution": artifacts.blocker_messages.get(blocker),
            }

            attempt_headline = base_headline
            attempt_visual = base_visual
            attempt_angle = base_angle
            accepted = False
            for _attempt in range(3):
                accepted = guard.register(
                    {"headline": attempt_headline, "visual": attempt_visual, "angle": attempt_angle},
                    metadata={"bucket": bucket, "audience": audience.fit_label},
                )
                if accepted:
                    break
                unique_suffix = _short_phrase(proof_point, max_words=1, default="Proof").lower()
                attempt_headline = self._ensure_unique_headline(
                    f"{base_headline} {unique_suffix}", headline_registry, context
                )
                attempt_visual = f"{base_visual} Distinct cue: {unique_suffix} overlay.".strip()
                attempt_angle = self._enforce_tone(
                    f"{base_angle} Spotlight {unique_suffix} evidence.",
                    promo_allowed,
                    tone_metrics,
                    field="angle",
                )
            if not accepted:
                raise ValueError("Duplicate guard exhausted attempts while generating creatives")

            final_headline = attempt_headline
            final_visual = attempt_visual
            final_angle = attempt_angle

            headline_registry.add(final_headline.lower())
            concept_index = len(concepts) + 1
            concepts.append(
                CreativeConcept(
                    index=concept_index,
                    bucket=bucket,
                    headline=final_headline,
                    visual=_sanitize_sentence(final_visual),
                    angle=_sanitize_sentence(final_angle),
                    blocker=blocker_field,
                    audience_fit=audience.fit_label,
                    audience_id=audience.identifier,
                    metadata=metadata,
                )
            )
            selection_hints.append(
                {
                    "id": concept_index,
                    "bucket": bucket,
                    "audience_fit": audience.fit_label,
                    "headline": final_headline,
                    "visual_prompt": _sanitize_sentence(final_visual),
                    "cta_tone": artifacts.cta_tone,
                    "proof_focus": proof_point,
                    "motivation": motivation_point,
                }
            )

        coverage = self._coverage(concepts)
        self._assert_quotas(coverage)

        return concepts, guard.summary(), coverage, tone_metrics, selection_hints

    def _bucket_plan(self) -> List[str]:
        plan: List[str] = []
        for bucket in CREATIVE_BUCKETS:
            plan.extend([bucket] * 10)
        return plan

    def _build_context(
        self,
        *,
        audience: AudienceSegment,
        product_keyword: str,
        proof_point: str,
        blocker: str,
        motivation_point: str,
        blocker_resolution: str,
    ) -> Dict[str, str]:
        audience_phrase = _short_phrase(audience.name, max_words=3, default="Audience")
        product_phrase = _short_phrase(product_keyword, max_words=3, default="Flagship")
        blocker_alias = BLOCKER_ALIASES.get(blocker, blocker)
        context = {
            "audience_phrase": audience_phrase,
            "audience_phrase_lower": audience_phrase.lower(),
            "audience_descriptor": audience.descriptor,
            "audience_descriptor_lower": audience.descriptor.lower(),
            "product_phrase": product_phrase,
            "product_phrase_lower": product_phrase.lower(),
            "proof_phrase": proof_point,
            "proof_phrase_lower": proof_point.lower(),
            "motivation_phrase": motivation_point,
            "motivation_phrase_lower": motivation_point.lower(),
            "blocker_alias": blocker_alias,
            "blocker_alias_lower": blocker_alias.lower(),
            "blocker_resolution": blocker_resolution,
            "blocker_resolution_lower": blocker_resolution.lower(),
            "market_signal": "Trusted craft in a noisy category",
        }
        return context

    def _format_template(self, templates: Sequence[str], index: int, context: Mapping[str, str]) -> str:
        template = templates[index % len(templates)]
        formatted = template.format(**context)
        if template in HEADLINE_TEMPLATES.get("Shock", []):
            formatted = _sanitize_headline(formatted)
        return formatted

    def _ensure_unique_headline(
        self, headline: str, registry: set[str], context: Mapping[str, str]
    ) -> str:
        headline = _sanitize_headline(headline)
        candidate = headline
        attempts = 0
        while candidate.lower() in registry and attempts < 3:
            suffix = _short_phrase(context.get("motivation_phrase", "Momentum"), max_words=1, default="Momentum")
            if len(candidate.split()) < 10:
                candidate = f"{candidate} {suffix}".strip()
            else:
                candidate = f"New {candidate}".strip()
            candidate = _sanitize_headline(candidate)
            attempts += 1
        return candidate

    def _enforce_tone(
        self,
        text: str,
        promo_allowed: bool,
        tone_metrics: MutableMapping[str, Any],
        *,
        field: str,
    ) -> str:
        if promo_allowed:
            return text
        lowered = text.lower()
        updated = text
        for term in PROMO_TERMS:
            if term in lowered:
                tone_metrics["violations"].append({"field": field, "term": term, "original": text})
                replacement = "value" if field == "headline" else "value-led confidence"
                updated = re.sub(term, replacement, updated, flags=re.IGNORECASE)
                tone_metrics["auto_corrected"] += 1
                lowered = updated.lower()
        return updated

    def _coverage(self, concepts: Iterable[CreativeConcept]) -> Dict[str, Any]:
        bucket_counts: Counter[str] = Counter()
        blocker_counts: Counter[str] = Counter()
        audiences: set[str] = set()
        for concept in concepts:
            bucket_counts[concept.bucket] += 1
            audiences.add(concept.audience_id)
            for piece in concept.blocker.split("·"):
                canonical = _canonical_blocker(piece.strip())
                if canonical:
                    blocker_counts[canonical] += 1
        return {
            "bucket_counts": {bucket: bucket_counts.get(bucket, 0) for bucket in CREATIVE_BUCKETS},
            "blocker_counts": {blocker: blocker_counts.get(blocker, 0) for blocker in REQUIRED_BLOCKERS},
            "unique_audiences": len(audiences),
        }

    def _assert_quotas(self, coverage: Mapping[str, Any]) -> None:
        bucket_counts = coverage["bucket_counts"]
        for bucket in CREATIVE_BUCKETS:
            if bucket_counts.get(bucket, 0) < 10:
                raise ValueError(f"Creative bucket quota not met for {bucket}")
        blocker_counts = coverage["blocker_counts"]
        for blocker in REQUIRED_BLOCKERS:
            if blocker_counts.get(blocker, 0) < 2:
                raise ValueError(f"Blocker coverage shortfall for {blocker}")


__all__ = ["CreativeStage"]
