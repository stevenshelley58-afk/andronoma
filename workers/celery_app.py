"""Celery application configuration."""
from __future__ import annotations

from celery import Celery

from shared.config import get_settings

settings = get_settings()

celery_app = Celery(
    "andronoma",
    broker=settings.broker_url,
    backend=settings.result_backend,
    include=["workers.tasks"],
)

celery_app.conf.beat_schedule = {}
celery_app.conf.update(task_serializer="json", accept_content=["json"], result_serializer="json")
