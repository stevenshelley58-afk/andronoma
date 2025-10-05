from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.config import settings_dict
from shared.pipeline import PIPELINE_ORDER

from .routes import auth, logs, runs


description = """
Andronoma Orchestration API.

This API drives the end-to-end campaign workflow:
1. **scrape** market intelligence.
2. **process** the ingested data with NLP enrichment.
3. Create actionable **audiences**.
4. Generate **creatives**.
5. Produce campaign **images**.
6. Run automated **qa** gates before launch.
7. Package everything for **export**.
"""

app = FastAPI(
    title="Andronoma Platform API",
    description=description,
    version="0.1.0",
    openapi_tags=[
        {"name": "auth", "description": "Authentication"},
        {"name": "runs", "description": "Pipeline orchestration"},
        {"name": "logs", "description": "Streaming run logs"},
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(runs.router)
app.include_router(logs.router)


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/settings", tags=["meta"])
def settings() -> dict:
    return settings_dict()


@app.get("/pipeline", tags=["meta"])
def pipeline_flow() -> dict[str, list[str]]:
    return {"stages": PIPELINE_ORDER}
