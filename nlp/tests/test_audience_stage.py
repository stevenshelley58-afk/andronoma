from __future__ import annotations

import json
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nlp.pipeline import AudienceStage
from qa.validators import load_csv_records, validate_audience_quotas
from shared.models import (
    AssetRecord,
    Base,
    PipelineRun,
    RunStatus,
    StageState,
    StageStatus,
    User,
)
from shared.stages.base import StageContext


@pytest.fixture()
def session(tmp_path: Path) -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    with SessionLocal() as session:
        yield session
    engine.dispose()


def _seed_processing_payload(destination: Path) -> None:
    payload = {
        "artifacts": {
            "brand_position": {
                "category": {"statement": "Acme competes within modern wellness ops"},
                "promise": {"statement": "Delivers calmer mornings for busy operators"},
                "differentiators": [
                    {"statement": "Automates onboarding workflows with human support"},
                    {"statement": "Personalises recommendations using behaviour data"},
                ],
                "proof_pillars": [
                    {"statement": "Teams report a 30% lift in output within 60 days"},
                    {"statement": "Trusted by 5k revenue leaders and 200 agencies"},
                ],
                "value_framing": {
                    "statement": "Balanced value between premium support and efficient automation"
                },
            },
            "motivation_map": {
                "functional": {
                    "intensity": "High",
                    "insights": [
                        {
                            "statement": "Operators craving systems that remove manual spreadsheet work",
                            "type": "Direct",
                        }
                    ],
                },
                "emotional": {
                    "intensity": "Medium",
                    "insights": [
                        {
                            "statement": "Leaders who want confidence their team stays proactive",
                            "type": "Direct",
                        }
                    ],
                },
                "aspirational": {
                    "intensity": "Medium",
                    "insights": [
                        {
                            "statement": "Visionaries scaling toward category leadership",
                            "type": "Direct",
                        }
                    ],
                },
                "social": {
                    "intensity": "Low",
                    "insights": [
                        {
                            "statement": "Community builders amplifying peer wins",
                            "type": "Direct",
                        }
                    ],
                },
            },
            "blockers_ranking": [
                {"blocker": "cost concerns"},
                {"blocker": "integration risk"},
                {"blocker": "time to value"},
                {"blocker": "proof gaps"},
                {"blocker": "stakeholder buy-in"},
            ],
            "market_summary": {
                "whitespace_opportunities": [
                    {"statement": "Own the affordability plus premium support narrative"}
                ],
                "cultural_signals": [
                    {"statement": "Community-led adoption is accelerating"}
                ],
            },
        }
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload), encoding="utf-8")


@pytest.fixture()
def prepared_run(session: Session, tmp_path: Path) -> PipelineRun:
    owner = User(id=uuid4(), email="owner@example.com", password_hash="hash")
    run = PipelineRun(
        id=uuid4(),
        owner_id=owner.id,
        status=RunStatus.RUNNING,
        input_payload={"config": {"target_markets": ["Ops leads", "Growth marketers"]}},
        budgets={"audiences": 60.0},
        telemetry={},
    )
    session.add_all([owner, run])
    session.commit()
    session.refresh(run)

    processed_root = tmp_path / "processed"
    output_path = tmp_path / "outputs" / "audiences_master.csv"
    original_processed_root = AudienceStage.PROCESSED_ROOT
    original_output_path = AudienceStage.OUTPUT_PATH
    AudienceStage.PROCESSED_ROOT = processed_root
    AudienceStage.OUTPUT_PATH = output_path

    payload_path = processed_root / str(run.id) / "processing.json"
    _seed_processing_payload(payload_path)

    try:
        yield run
    finally:
        AudienceStage.PROCESSED_ROOT = original_processed_root
        AudienceStage.OUTPUT_PATH = original_output_path


def test_audience_stage_generates_quota_compliant_csv(
    session: Session, prepared_run: PipelineRun
) -> None:
    context = StageContext(session=session, run=prepared_run)
    stage = AudienceStage(context)

    stage_state = StageState(
        id=uuid4(),
        run_id=prepared_run.id,
        name=stage.name,
        status=StageStatus.PENDING,
    )
    session.add(stage_state)
    session.commit()

    telemetry = stage.execute()

    csv_path = Path(telemetry["csv_path"])
    assert csv_path.exists()

    records = load_csv_records(csv_path)
    assert len(records) >= 100
    qa_result = validate_audience_quotas(records)
    assert not qa_result.is_blocker()

    stored_assets = (
        session.query(AssetRecord)
        .filter(AssetRecord.run_id == prepared_run.id, AssetRecord.stage == stage.name)
        .all()
    )
    assert stored_assets, "Audience CSV asset record should be created"

    assert telemetry["row_count"] == len(records)
    assert telemetry["records"], "Structured records should be present in telemetry"
    assert telemetry["qa"]["severity"] == "pass"
