"""Celery application configuration."""
from __future__ import annotations

import os

from celery import Celery
from celery.schedules import crontab

from shared.config import get_settings

settings = get_settings()

celery_app = Celery(
    "andronoma",
    broker=settings.broker_url,
    backend=settings.result_backend,
    include=["workers.tasks", "workers.codex_tasks"],
)

ENABLE_HARDENING = str(os.getenv("FEATURE_PLATFORM_HARDENING", "0")).lower() in {
    "1",
    "true",
    "yes",
    "on",
    "y",
}

if ENABLE_HARDENING:
    celery_app.conf.beat_schedule = {
        "platform-hardening-nightly": {
            "task": "codex.platform_hardening.nightly",
            "schedule": crontab(hour=2, minute=30),
        }
    }
else:
    celery_app.conf.beat_schedule = {}
celery_app.conf.update(task_serializer="json", accept_content=["json"], result_serializer="json")
