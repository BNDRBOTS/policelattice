from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.pipeline.runner import run_all_due


logger = logging.getLogger(__name__)


def build_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone="America/Phoenix")
    # Run a general check every 15 minutes. The precise source windows are
    # managed by the source catalog schedule fields. This avoids duplicate cron
    # registration for dozens of sources while preserving temporal sequencing.
    scheduler.add_job(
        run_all_due,
        CronTrigger(minute="*/15", timezone="America/Phoenix"),
        id="pipeline_runner",
        replace_existing=True,
    )
    return scheduler
