"""Automated Asia/Kolkata Daily Scheduler for Groww Data Collection."""

from __future__ import annotations
import time
import logging
import threading
import gc
from datetime import datetime, date, timedelta

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ
from .calendar_service import NSECalendarService
from .instruments import InstrumentSyncService
from .fetcher import ConcurrencyFetchEngine
from .audit import HistoricalAuditService

logger = logging.getLogger(__name__)

class DailyScheduler:
    """Schedules 3:55 PM, 4:00 PM, 6:00 AM, and Audit tasks in Asia/Kolkata timezone."""

    def __init__(
        self,
        config: CollectorConfig = DEFAULT_CONFIG,
        calendar: NSECalendarService | None = None,
        sync_service: InstrumentSyncService | None = None,
        fetch_engine: ConcurrencyFetchEngine | None = None,
        audit_service: HistoricalAuditService | None = None,
    ):
        self.config = config
        self.calendar = calendar or NSECalendarService(config)
        self.sync_service = sync_service or InstrumentSyncService(config)
        self.fetch_engine = fetch_engine or ConcurrencyFetchEngine(config, calendar=self.calendar)
        self.audit_service = audit_service or HistoricalAuditService(config, calendar=self.calendar)
        self._running = False
        self._executed_tasks_today: set[str] = set()
        self._last_date: date | None = None

    def run_sync_instruments_task(self):
        """3:55 PM IST task."""
        now_dt = datetime.now(IST_TZ)
        today = now_dt.date()
        if not self.calendar.is_trading_day(today):
            logger.info(f"[{now_dt.strftime('%H:%M:%S')}] 3:55 PM: Skipping instrument sync (NSE Market Closed / Holiday: {today}).")
            return
        logger.info(f"[{now_dt.strftime('%H:%M:%S')}] 3:55 PM: Triggering NSE EQ Instrument Synchronization...")
        try:
            res = self.sync_service.synchronize()
            logger.info(f"3:55 PM Sync Finished: {res}")
        except Exception as e:
            logger.error(f"3:55 PM Sync failed: {e}", exc_info=True)

    def run_daily_fetch_task(self):
        """4:00 PM IST task."""
        now_dt = datetime.now(IST_TZ)
        today = now_dt.date()
        if not self.calendar.is_trading_day(today):
            logger.info(f"[{now_dt.strftime('%H:%M:%S')}] 4:00 PM: Skipping daily fetch (NSE Market Closed / Holiday: {today}).")
            return
        logger.info(f"[{now_dt.strftime('%H:%M:%S')}] 4:00 PM: Triggering Daily Candle Fetch...")
        try:
            rep = self.fetch_engine.fetch_daily_candles_4pm(today)
            logger.info(f"4:00 PM Fetch Completed. Run ID: {rep.run_id}, Summary: {rep.summary.overall_status}")
        except Exception as e:
            logger.error(f"4:00 PM Fetch failed: {e}", exc_info=True)

    def run_final_sync_task(self):
        """6:00 AM IST task (Final correction of previous completed session)."""
        now_dt = datetime.now(IST_TZ)
        if self.config.skip_weekends and now_dt.weekday() >= 5:
            logger.info(f"[{now_dt.strftime('%H:%M:%S')}] 6:00 AM: Skipping final sync (Weekend / Market Closed: {now_dt.strftime('%A')}).")
            return
        logger.info(f"[{now_dt.strftime('%H:%M:%S')}] 6:00 AM: Triggering Final Candle Synchronization...")
        try:
            prev_session = self.calendar.get_previous_trading_session(now_dt)
            logger.info(f"Identified previous completed session for 6:00 AM correction: {prev_session}")
            rep = self.fetch_engine.final_candle_sync_6am(prev_session)
            logger.info(f"6:00 AM Final Correction Completed. Run ID: {rep.run_id}, Status: {rep.summary.overall_status}")
        except Exception as e:
            logger.error(f"6:00 AM Final Sync failed: {e}", exc_info=True)

    def run_audit_task(self):
        """Daily completeness audit task."""
        now_dt = datetime.now(IST_TZ)
        if self.config.skip_weekends and now_dt.weekday() >= 5:
            logger.info(f"[{now_dt.strftime('%H:%M:%S')}] Skipping completeness audit (Weekend / Market Closed: {now_dt.strftime('%A')}).")
            return
        logger.info(f"[{now_dt.strftime('%H:%M:%S')}] Triggering Historical Completeness Audit...")
        try:
            rep = self.audit_service.run_completeness_audit(attempt_gap_recovery=True)
            logger.info(f"Completeness Audit Completed. Run ID: {rep.run_id}, Status: {rep.summary.overall_status}")
        except Exception as e:
            logger.error(f"Completeness Audit failed: {e}", exc_info=True)

    def start_daemon(self, poll_interval_seconds: int = 15):
        """Start daemon loop checking Asia/Kolkata clock and firing tasks at scheduled times."""
        self._running = True
        logger.info("=" * 70)
        logger.info("  GROWW DAILY COLLECTION SYSTEM SCHEDULER STARTED (Asia/Kolkata)")
        logger.info("  WEEKEND POLICY: Strict No-Run on Saturday & Sunday")
        logger.info(f"  RETENTION POLICY: Store last {self.config.log_retention_days} days of logs; auto-purge oldest")
        logger.info(f"  3:55 PM IST ({self.config.schedule_sync_instruments}) -> Instrument & Catalog Sync")
        logger.info(f"  4:00 PM IST ({self.config.schedule_daily_fetch}) -> Daily Candle Fetch")
        logger.info(f"  6:00 AM IST ({self.config.schedule_final_sync}) -> Previous Session Final Correction")
        logger.info(f"  6:30 AM IST ({self.config.schedule_audit}) -> Historical Completeness Audit")
        logger.info("=" * 70)

        while self._running:
            try:
                now_ist = datetime.now(IST_TZ)
                curr_date = now_ist.date()

                # Reset executed set and clean memory at midnight IST
                if self._last_date != curr_date:
                    self._executed_tasks_today.clear()
                    self._last_date = curr_date
                    # Trigger periodic garbage collection and refresh trading calendar
                    gc.collect()
                    try:
                        self.calendar.initialize(force=True)
                    except Exception as cal_err:
                        logger.warning(f"Midnight calendar refresh non-critical error: {cal_err}")
                    # Enforce strictly 21 days log & report retention (purging oldest files)
                    try:
                        from .cli import cleanup_old_logs_and_reports
                        cleanup_old_logs_and_reports(self.config.logs_dir, max_days=self.config.log_retention_days)
                    except Exception as purge_err:
                        logger.warning(f"Midnight log cleanup error: {purge_err}")
                    logger.info(f"--- New Day Begun: {curr_date} IST | Daily Tasks Reset & Garbage Collected ---")

                # Weekend Protection: Do NOT run any tasks on Saturday (5) or Sunday (6)
                if self.config.skip_weekends and curr_date.weekday() >= 5:
                    if "weekend_logged" not in self._executed_tasks_today:
                        self._executed_tasks_today.add("weekend_logged")
                        day_name = curr_date.strftime("%A")
                        logger.info(f"[{now_ist.strftime('%H:%M:%S')}] Weekend detected ({day_name}, {curr_date}): Markets closed. Zero tasks will run on Saturday & Sunday.")
                    time.sleep(poll_interval_seconds)
                    continue

                curr_time_str = now_ist.strftime("%H:%M")

                # 1. 3:55 PM: Sync instruments
                if curr_time_str == self.config.schedule_sync_instruments and "sync_instruments" not in self._executed_tasks_today:
                    self._executed_tasks_today.add("sync_instruments")
                    self.run_sync_instruments_task()

                # 2. 4:00 PM: Daily fetch
                if curr_time_str == self.config.schedule_daily_fetch and "daily_fetch" not in self._executed_tasks_today:
                    self._executed_tasks_today.add("daily_fetch")
                    self.run_daily_fetch_task()

                # 3. 6:00 AM: Final sync
                if curr_time_str == self.config.schedule_final_sync and "final_sync" not in self._executed_tasks_today:
                    self._executed_tasks_today.add("final_sync")
                    self.run_final_sync_task()

                # 4. 6:30 AM: Daily completeness audit
                if curr_time_str == self.config.schedule_audit and "audit" not in self._executed_tasks_today:
                    self._executed_tasks_today.add("audit")
                    self.run_audit_task()

            except Exception as e:
                logger.error(f"Error in scheduler tick: {e}", exc_info=True)

            time.sleep(poll_interval_seconds)

    def stop(self):
        """Stop scheduler loop."""
        self._running = False
        logger.info("Scheduler stopped.")
