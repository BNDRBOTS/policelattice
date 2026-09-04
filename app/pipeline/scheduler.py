"""Automated refresh protocol.

Two cron jobs:

* **acquisition tick** — the full Search->...->Synthesize sequence, so new
  rows published by any source land in the lattice without intervention.
* **monthly refresh** — on the configured day, the sequence runs, per-officer
  anomaly detection is recomputed for every month present, and every month is
  sealed into the immutable chron-archive.
"""

from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import get_settings
from app.db import SessionLocal
from app.pipeline.anomalies import AnomalyDetector
from app.pipeline.archive import refresh_archive
from app.pipeline.orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)
settings = get_settings()


def run_acquisition_cycle(trigger: str = "scheduled") -> dict:
    with SessionLocal() as session:
        report = PipelineOrchestrator(session).run(trigger=trigger)
    logger.info(
        "Acquisition cycle complete: %s rows received, %s new",
        report["phases"].get("gather", {}).get("rows_received"),
        report["phases"].get("gather", {}).get("rows_new"),
    )
    return report


def run_monthly_refresh() -> dict:
    """Full pipeline, then anomaly detection and archival for every month."""
    from app.analytics.engine import available_periods

    report = run_acquisition_cycle(trigger="monthly_refresh")

    with SessionLocal() as session:
        detector = AnomalyDetector(session)
        anomaly_reports = [
            detector.detect(period) for period in available_periods(session)
        ]
        session.commit()

        archive_report = refresh_archive(session)

    report["anomaly_detection"] = anomaly_reports
    report["archive"] = archive_report
    logger.info(
        "Monthly refresh complete: %s findings across %s periods; %s months sealed",
        sum(r.get("findings", 0) for r in anomaly_reports),
        len(anomaly_reports),
        archive_report.get("periods_sealed"),
    )
    return report


def build_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone="America/Phoenix")

    acquisition = settings.acquisition_cron.split()
    scheduler.add_job(
        run_acquisition_cycle,
        CronTrigger(
            minute=acquisition[0],
            hour=acquisition[1],
            day=acquisition[2],
            month=acquisition[3],
            day_of_week=acquisition[4],
        ),
        id="acquisition_cycle",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=900,
    )

    monthly = settings.monthly_refresh_cron.split()
    scheduler.add_job(
        run_monthly_refresh,
        CronTrigger(
            minute=monthly[0],
            hour=monthly[1],
            day=monthly[2],
            month=monthly[3],
            day_of_week=monthly[4],
        ),
        id="monthly_refresh",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    return scheduler
