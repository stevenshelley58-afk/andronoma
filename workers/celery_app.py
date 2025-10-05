"""Celery application configuration."""
from __future__ import annotations

import os

from celery import Celery
from celery.schedules import crontab
from kombu import Queue

from shared.config import get_settings
from .constants import PLATFORM_HARDENING_QUEUE

settings = get_settings()

celery_app = Celery(
    "andronoma",
    broker=settings.broker_url,
    backend=settings.result_backend,
    include=["workers.tasks", "workers.codex_tasks"],
)

celery_app.conf.task_default_queue = "andronoma"
celery_app.conf.task_queues = (
    Queue("andronoma", routing_key="andronoma"),
    Queue(PLATFORM_HARDENING_QUEUE, routing_key=PLATFORM_HARDENING_QUEUE),
)
celery_app.conf.task_routes = {
    "codex.*": {
        "queue": PLATFORM_HARDENING_QUEUE,
        "routing_key": PLATFORM_HARDENING_QUEUE,
    }
}

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
