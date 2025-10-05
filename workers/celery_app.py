"""Celery application configuration."""
from __future__ import annotations

from celery import Celery
from celery.schedules import crontab
from kombu import Queue

from shared.config import get_settings

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
)
celery_app.conf.task_routes = {}

celery_app.conf.beat_schedule = {
    "codex-nightly": {
        "task": "codex.standard.nightly",
        "schedule": crontab(hour=2, minute=30),
    }
}
celery_app.conf.update(task_serializer="json", accept_content=["json"], result_serializer="json")
