"""Background scheduling.

Jobs:
1. ``pipeline_runner``  — every 15 minutes, runs sources whose cron schedule
   is due (six-phase orchestrator in due_only mode).
2. ``monthly_refresh``  — on the 1st of each month at 02:00 Arizona time,
   finalizes the chron-archive for the month that just ended and re-archives
   the current month (automated monthly refresh protocol).
"""

from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import get_settings
from app.pipeline.runner import run_all_due

logger = logging.getLogger(__name__)


def run_monthly_refresh() -> dict:
    """Finalize the previous month's archive and refresh the current month."""
    from datetime import UTC, datetime

    from app.analytics.archive import MonthlyArchiver
    from app.db import SessionLocal

    now = datetime.now(UTC)
    current_month = now.strftime("%Y-%m")

    with SessionLocal() as session:
        archiver = MonthlyArchiver(session)
        stats: dict = {"current_month": current_month}
        try:
            stats["current"] = archiver.archive_month(current_month)
            prev = archiver.previous_month_key(current_month)
            if not archiver.is_finalized(prev):
                stats["finalized_previous"] = archiver.archive_month(prev, finalize=True)
            session.commit()
            logger.info("Monthly refresh completed: %s", stats)
        except Exception:
            session.rollback()
            logger.exception("Monthly refresh failed")
            raise
    return stats


def build_scheduler() -> BackgroundScheduler:
    settings = get_settings()
    scheduler = BackgroundScheduler(timezone="America/Phoenix")
    scheduler.add_job(
        run_all_due,
        CronTrigger(minute="*/15", timezone="America/Phoenix"),
        id="pipeline_runner",
        replace_existing=True,
    )
    scheduler.add_job(
        run_monthly_refresh,
        CronTrigger(
            day=settings.monthly_refresh_cron_day,
            hour=settings.monthly_refresh_cron_hour,
            minute=0,
            timezone="America/Phoenix",
        ),
        id="monthly_refresh",
        replace_existing=True,
    )
    return scheduler
