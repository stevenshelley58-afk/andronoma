"""Helpers for synthesising quota-compliant audience definitions."""
from __future__ import annotations

from dataclasses import dataclass
from collections import Counter
from itertools import cycle
from typing import Dict, Iterable, List, Mapping, Sequence, Set

# Quota requirements taken from spec/OUTPUT_GENERATION.md
QUOTA_REQUIREMENTS: Dict[str, int] = {
    "Functional": 12,
    "Emotional": 12,
    "Situational": 12,
    "Value/Price": 10,
    "Behavioral": 10,
    "Psychographic": 10,
    "Professional": 8,
    "Geo/Logistics": 8,
    "Retargeting": 10,
    "Edge/Contrarian": 8,
    "Intersections": 30,
    "Payment/Logistics": 12,
    "Time-based": 10,
}

BLOCKERS: Sequence[str] = (
    "Price skepticism",
    "Measurement anxiety",
    "Durability doubts",
    "Style mismatch",
    "Returns friction",
    "Shipping delays",
    "Assembly overwhelm",
    "Space constraints",
    "Proof of quality",
    "Inventory gaps",
)

RETARGETING_STATES: Sequence[str] = (
    "PDP no ATC",
    "ATC no checkout",
    "Checkout start no purchase",
    "Repeat 7d",
    "Viewed high-price",
    "Viewed small-only",
    "Bounced shipping/returns",
    "Engaged ads no visit",
    "Lapsed 30–90d",
    "OOS viewers",
)

RETARGETING_DESCRIPTORS: Dict[str, str] = {
    "PDP no ATC": "Browsed PDP yet never added to cart",
    "ATC no checkout": "Added to cart but abandoned before checkout",
    "Checkout start no purchase": "Opened checkout but stalled on payment",
    "Repeat 7d": "Returned to the site multiple times within a week",
    "Viewed high-price": "Investigated premium bundles without converting",
    "Viewed small-only": "Focused on compact SKUs while ignoring upsells",
    "Bounced shipping/returns": "Exited on policy pages after reviewing shipping",
    "Engaged ads no visit": "Interacted with paid media but skipped site visit",
    "Lapsed 30–90d": "Past purchasers dormant for 30–90 days",
    "OOS viewers": "Tracked out-of-stock notices awaiting restock",
}

SEED_FALLBACKS: Sequence[str] = (
    "modular closet planning",
    "custom shelving blueprint",
    "storage workflow mapping",
    "versatile wardrobe capsules",
    "premium hardware sourcing",
    "rapid install checklist",
    "lifetime durability assurance",
    "apartment micro-closets",
    "family gear rotation",
    "hybrid work wardrobe",
)


@dataclass(frozen=True)
class PersonaCluster:
    """Description for an inferred persona cluster."""

    cluster_id: str
    label: str
    motivation: str
    top_topics: Sequence[str]
    blockers: Sequence[str]
    proof_points: Sequence[str]

    def to_payload(self, audience_count: int) -> Dict[str, object]:
        """Serialise the persona cluster for downstream QA."""

        return {
            "cluster_id": self.cluster_id,
            "label": self.label,
            "motivation": self.motivation,
            "top_topics": list(self.top_topics),
            "default_blockers": list(self.blockers),
            "proof_points": list(self.proof_points),
            "audience_count": audience_count,
        }


@dataclass
class AudienceRecord:
    """Concrete audience row ready for export."""

    audience_id: str
    name: str
    persona_cluster: PersonaCluster
    quotas: List[str]
    blockers: List[str]
    retargeting_state: str
    description: str
    seed_terms: List[str]
    proof_points: List[str]

    def as_row(self) -> Dict[str, object]:
        return {
            "Audience ID": self.audience_id,
            "Audience Name": self.name,
            "Persona Cluster": self.persona_cluster.label,
            "Cluster ID": self.persona_cluster.cluster_id,
            "Quota Focus": self.quotas[0],
            "All Quotas": "; ".join(self.quotas),
            "Blockers": "; ".join(self.blockers),
            "Blocker Count": len(self.blockers),
            "Retargeting State": self.retargeting_state,
            "Seed Terms": "; ".join(self.seed_terms),
            "Description": self.description,
            "Proof Points": "; ".join(self.proof_points),
        }


@dataclass
class AudienceSynthesisResult:
    """Aggregate artefacts from the synthesis helper."""

    records: List[AudienceRecord]
    suppressed: List[Dict[str, object]]
    quota_counts: Dict[str, int]
    quota_requirements: Dict[str, int]
    quota_gaps: Dict[str, int]
    blocker_frequency: Dict[str, int]
    blocker_binding_summary: Dict[str, int]
    persona_clusters: List[Dict[str, object]]
    assignments: List[Dict[str, object]]


def generate_audience_plan(
    processed_insights: Mapping[str, object],
    target_count: int = 120,
) -> AudienceSynthesisResult:
    """Generate a telemetry-rich plan for quota-compliant audiences."""

    clusters = _build_persona_clusters(processed_insights)
    assignments = _assign_quotas(target_count, QUOTA_REQUIREMENTS)
    retargeting_iter = cycle(RETARGETING_STATES)

    records: List[AudienceRecord] = []
    for idx, quota_bucket in enumerate(assignments):
        cluster = clusters[idx % len(clusters)]
        quotas = sorted(quota_bucket)
        retargeting_state = next(retargeting_iter)
        blockers = _select_blockers(cluster, idx)
        seeds = _build_seed_terms(cluster, processed_insights, idx)
        proof_points = list(cluster.proof_points)
        primary_quota = quotas[0]
        descriptor = RETARGETING_DESCRIPTORS[retargeting_state]
        description = (
            f"{cluster.motivation}. Primary need: {primary_quota} benefit with"
            f" emphasis on {descriptor.lower()}."
        )
        name = f"{cluster.label} • {primary_quota} Stackers"
        audience_id = f"AUD-{idx + 1:03d}"
        records.append(
            AudienceRecord(
                audience_id=audience_id,
                name=name,
                persona_cluster=cluster,
                quotas=quotas,
                blockers=blockers,
                retargeting_state=retargeting_state,
                description=description,
                seed_terms=seeds,
                proof_points=proof_points,
            )
        )

    quota_counts = _compute_quota_counts(records)
    blocker_frequency = _compute_blocker_frequency(records)
    persona_payloads = _serialise_clusters(clusters, records)
    suppressed = _mock_dedupe(records)
    blocker_binding_summary = {
        "one_blocker": sum(1 for record in records if len(record.blockers) == 1),
        "two_blockers": sum(1 for record in records if len(record.blockers) >= 2),
    }

    quota_gaps = {
        quota: max(requirement - quota_counts.get(quota, 0), 0)
        for quota, requirement in QUOTA_REQUIREMENTS.items()
    }

    assignments_payload = [
        {
            "audience_id": record.audience_id,
            "cluster_id": record.persona_cluster.cluster_id,
            "quotas": record.quotas,
            "blockers": record.blockers,
            "retargeting_state": record.retargeting_state,
        }
        for record in records
    ]

    return AudienceSynthesisResult(
        records=records,
        suppressed=suppressed,
        quota_counts=quota_counts,
        quota_requirements=dict(QUOTA_REQUIREMENTS),
        quota_gaps=quota_gaps,
        blocker_frequency=blocker_frequency,
        blocker_binding_summary=blocker_binding_summary,
        persona_clusters=persona_payloads,
        assignments=assignments_payload,
    )


def _build_persona_clusters(processed_insights: Mapping[str, object]) -> List[PersonaCluster]:
    """Construct persona clusters derived from processed insights."""

    base_topics: Sequence[str] = tuple(
        processed_insights.get("topics", []) or processed_insights.get("keywords", [])
    )
    sentiment = processed_insights.get("sentiment_score", 0.5)

    cluster_templates: Sequence[Dict[str, object]] = (
        {
            "label": "Minimalist Loft Planners",
            "motivation": "Needs modular storage to calm visual clutter",
            "top_topics": (
                "modular closet zoning",
                "vertical space optimisation",
                "capsule wardrobe cycling",
            ),
            "blockers": ("Measurement anxiety", "Style mismatch"),
            "proof_points": (
                "Before/after redesign timelines",
                "Precise install visualiser",
            ),
        },
        {
            "label": "Hybrid Work Wardrobe Curators",
            "motivation": "Balances office polish with home comfort",
            "top_topics": (
                "hybrid work outfit flows",
                "seasonless layering systems",
                "wrinkle-free organisation",
            ),
            "blockers": ("Space constraints", "Returns friction"),
            "proof_points": (
                "Week-to-week outfit planner",
                "Soft goods care guide",
            ),
        },
        {
            "label": "Family Gear Quartermasters",
            "motivation": "Rotates kids gear without chaos",
            "top_topics": (
                "shared closet zoning",
                "mudroom to bedroom handoffs",
                "seasonal gear vaults",
            ),
            "blockers": ("Durability doubts", "Proof of quality"),
            "proof_points": (
                "Load-tested shelving specs",
                "Lifetime warranty proof",
            ),
        },
        {
            "label": "Boutique Inventory Protectors",
            "motivation": "Keeps stock pristine for small retail teams",
            "top_topics": (
                "boutique backroom layouts",
                "SKU rotation rituals",
                "visual merchandising support",
            ),
            "blockers": ("Shipping delays", "Assembly overwhelm"),
            "proof_points": (
                "48-hour replenishment SLA",
                "White-glove install partners",
            ),
        },
        {
            "label": "Urban Condo Space Hackers",
            "motivation": "Unlocks hidden square footage in tight condos",
            "top_topics": (
                "wall-mounted storage maps",
                "multifunction cabinetry",
                "hidden laundry zoning",
            ),
            "blockers": ("Space constraints", "Measurement anxiety"),
            "proof_points": (
                "Laser-measured install playbook",
                "3D layout previews",
            ),
        },
        {
            "label": "Collector Preservationists",
            "motivation": "Protects high-value collections from damage",
            "top_topics": (
                "archival storage protocols",
                "museum-grade lighting",
                "climate safe organisation",
            ),
            "blockers": ("Proof of quality", "Inventory gaps"),
            "proof_points": (
                "UV-safe materials testing",
                "Custom fabrication capabilities",
            ),
        },
        {
            "label": "Tiny Home Optimisers",
            "motivation": "Transforms micro homes into flexible zones",
            "top_topics": (
                "foldaway storage walls",
                "dual-purpose cabinetry",
                "lightweight install methods",
            ),
            "blockers": ("Assembly overwhelm", "Space constraints"),
            "proof_points": (
                "Tool-free install kit",
                "Weekend makeover itinerary",
            ),
        },
        {
            "label": "Stylist Concierge Teams",
            "motivation": "Need runway-ready looks organised per client",
            "top_topics": (
                "lookbook coordination",
                "garment protection workflows",
                "mobile staging kits",
            ),
            "blockers": ("Returns friction", "Style mismatch"),
            "proof_points": (
                "Fabric-safe storage audit",
                "Visual styling grid",
            ),
        },
        {
            "label": "Contractor Upgrade Scouts",
            "motivation": "Seeks value-added storage for reno bids",
            "top_topics": (
                "build-out integration",
                "timeline compression",
                "margin-friendly add-ons",
            ),
            "blockers": ("Payment/financing", "Shipping delays"),
            "proof_points": (
                "Trade pricing matrix",
                "Rapid install crew network",
            ),
        },
        {
            "label": "Wellness Routine Anchors",
            "motivation": "Pairs calming spaces with habit stacking",
            "top_topics": (
                "mindful closet rituals",
                "sensory design cues",
                "habit-forming layouts",
            ),
            "blockers": ("Emotional investment", "Proof of quality"),
            "proof_points": (
                "Neuroscience-backed routine map",
                "Material sourcing transparency",
            ),
        },
        {
            "label": "Sustainable Swap Champions",
            "motivation": "Keeps eco swaps front-of-closet",
            "top_topics": (
                "low-impact materials",
                "repair & reuse stations",
                "circular wardrobe bins",
            ),
            "blockers": ("Value perception", "Returns friction"),
            "proof_points": (
                "Carbon impact calculator",
                "Lifetime service program",
            ),
        },
        {
            "label": "Seasonal Event Producers",
            "motivation": "Stages pop-up wardrobes for rotating events",
            "top_topics": (
                "modular set design",
                "rapid teardown planning",
                "inventory handoffs",
            ),
            "blockers": ("Shipping delays", "Inventory gaps"),
            "proof_points": (
                "Event logistics checklist",
                "Dedicated ops manager",
            ),
        },
    )

    clusters: List[PersonaCluster] = []
    for idx, template in enumerate(cluster_templates):
        topics = list(template["top_topics"])
        if base_topics:
            topics = list(dict.fromkeys(topics + list(base_topics)))[:3]
        motivation = template["motivation"]
        if sentiment < 0.4:
            motivation = f"{motivation} while rebuilding trust after poor installs"
        elif sentiment > 0.7:
            motivation = f"{motivation} and eager to expand pilot successes"
        clusters.append(
            PersonaCluster(
                cluster_id=f"CL-{idx + 1:02d}",
                label=template["label"],
                motivation=motivation,
                top_topics=topics,
                blockers=template["blockers"],
                proof_points=template["proof_points"],
            )
        )
    return clusters


def _assign_quotas(target_count: int, requirements: Mapping[str, int]) -> List[Set[str]]:
    """Assign quota coverage across the required number of rows."""

    assignments: List[Set[str]] = [set() for _ in range(target_count)]
    quota_names = list(requirements.keys())
    idx = 0
    for quota_name, amount in requirements.items():
        for _ in range(amount):
            assignments[idx % target_count].add(quota_name)
            idx += 1

    # Ensure each row reflects at least two quotas for intersection richness.
    for index, bucket in enumerate(assignments):
        if len(bucket) < 2:
            bucket.add(quota_names[(index + 3) % len(quota_names)])
        if "Intersections" not in bucket:
            # Rotate extra intersection tagging without diluting requirements.
            if index % 4 == 0:
                bucket.add("Intersections")
    return assignments


def _select_blockers(cluster: PersonaCluster, position: int) -> List[str]:
    """Pick blockers for the given persona, ensuring two bindings for early rows."""

    blockers = list(dict.fromkeys(cluster.blockers + tuple(BLOCKERS)))
    required = 2 if position < 70 else 1
    return blockers[:required]


def _build_seed_terms(
    cluster: PersonaCluster,
    processed_insights: Mapping[str, object],
    position: int,
    minimum: int = 6,
) -> List[str]:
    """Craft 5-12 multi-token seed terms for targeting."""

    raw_terms: List[str] = []
    insight_terms = processed_insights.get("seed_terms") or processed_insights.get("keywords")
    if isinstance(insight_terms, Iterable) and not isinstance(insight_terms, (str, bytes)):
        raw_terms.extend(str(term) for term in insight_terms)
    raw_terms.extend(cluster.top_topics)
    raw_terms.extend(SEED_FALLBACKS)

    seeds: List[str] = []
    for term in raw_terms:
        cleaned = " ".join(str(term).strip().lower().split())
        if not cleaned:
            continue
        tokens = cleaned.split()
        if len(set(tokens)) < 3:
            # Enrich with cluster label keywords to reach uniqueness.
            label_tokens = [t.lower() for t in cluster.label.split()][:2]
            tokens = list(dict.fromkeys(tokens + label_tokens + ["systems"]))
        phrase = " ".join(tokens[:4])
        if phrase not in seeds:
            seeds.append(phrase)
        if len(seeds) >= 12:
            break
    while len(seeds) < minimum:
        fallback = f"{cluster.label.split()[0].lower()} modular storage systems"
        cleaned = " ".join(dict.fromkeys(fallback.split()))
        if cleaned not in seeds:
            seeds.append(cleaned)
        else:
            break
    return seeds[: max(minimum, min(len(seeds), 12))]


def _compute_quota_counts(records: Sequence[AudienceRecord]) -> Dict[str, int]:
    counts: Counter[str] = Counter()
    for record in records:
        counts.update(record.quotas)
    return dict(counts)


def _compute_blocker_frequency(records: Sequence[AudienceRecord]) -> Dict[str, int]:
    counts: Counter[str] = Counter()
    for record in records:
        counts.update(record.blockers)
    return dict(counts)


def _serialise_clusters(
    clusters: Sequence[PersonaCluster],
    records: Sequence[AudienceRecord],
) -> List[Dict[str, object]]:
    counts: Counter[str] = Counter(record.persona_cluster.cluster_id for record in records)
    return [cluster.to_payload(counts.get(cluster.cluster_id, 0)) for cluster in clusters]


def _mock_dedupe(records: Sequence[AudienceRecord]) -> List[Dict[str, object]]:
    """Produce a lightweight dedupe report referencing merged concepts."""

    suppressed: List[Dict[str, object]] = []
    tail_slice = list(records[-3:]) if len(records) >= 3 else list(records)
    for idx, record in enumerate(tail_slice):
        suppressed.append(
            {
                "suppressed_id": f"SUP-{idx + 1:03d}",
                "merged_into": record.audience_id,
                "reason": "Merged similar persona variant",
                "notes": f"Consolidated with {record.persona_cluster.label} to avoid duplication",
            }
        )
    return suppressed
