"""Unified Command-Line Interface for the Groww Daily Candle Collection System."""

from __future__ import annotations
import sys
import argparse
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from datetime import datetime, date

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ
from .calendar_service import NSECalendarService
from .parquet_store import ParquetStore
from .instruments import InstrumentSyncService
from .fetcher import ConcurrencyFetchEngine
from .audit import HistoricalAuditService
from .scheduler import DailyScheduler
from .migration_tracker import MigrationTracker
from .verification import DataVerificationService
from .desktop_detector import DEFAULT_FOLDER_NAME

def setup_logging(logs_dir: Path, verbose: bool = False):
    """Configure comprehensive structured logging with automatic midnight rotation for 1000+ days."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now(IST_TZ).strftime("%Y%m%d")
    log_file = logs_dir / f"groww_collector.log"

    level = logging.DEBUG if verbose else logging.INFO

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s:%(lineno)d]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Midnight rotating file handler (keeps strictly last 21 days)
    rfh = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=21,
        encoding="utf-8",
    )
    rfh.suffix = "%Y%m%d"
    rfh.setLevel(level)
    rfh.setFormatter(formatter)
    root_logger.addHandler(rfh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)

    logging.info(f"Logging initialized. Rotating at midnight with 21-day retention. Active log: {log_file}")
    
    # Run initial cleanup of anything older than 21 days
    cleanup_old_logs_and_reports(logs_dir, max_days=21)

def cleanup_old_logs_and_reports(logs_dir: Path, max_days: int = 21):
    """Store strictly the last `max_days` days of logs and reports. Oldest files are automatically removed."""
    if not logs_dir.exists():
        return
    import time
    cutoff_time = time.time() - (max_days * 86400)
    purged_count = 0

    # 1. Purge log files older than max_days
    for f in logs_dir.glob("*.log*"):
        try:
            if f.is_file() and f.stat().st_mtime < cutoff_time:
                f.unlink(missing_ok=True)
                purged_count += 1
        except Exception:
            pass

    # 2. Purge reports older than max_days
    reports_dir = logs_dir / "reports"
    if reports_dir.exists():
        for f in reports_dir.glob("*.*"):
            try:
                if f.is_file() and f.stat().st_mtime < cutoff_time:
                    f.unlink(missing_ok=True)
                    purged_count += 1
            except Exception:
                pass

    if purged_count > 0:
        logging.info(f"Retention Enforcer: Cleaned {purged_count} files older than {max_days} days.")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Groww Daily Candle Data Collection System (300 Workers, Desktop Auto-Storage, Segment Migrations)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--task",
        choices=[
            "sync_instruments", "daily_fetch", "final_sync", 
            "audit", "verify_existing", "detect_migrations", 
            "run_all", "benchmark"
        ],
        help="Task to execute immediately",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run continuous background scheduler in Asia/Kolkata time (3:55 PM, 4:00 PM, 6:00 AM, Daily Audit)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=300,
        help="Number of concurrent worker threads (default: 300)",
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=4,
        help="Maximum total attempts per failed request (default: 4 = 1 initial + 3 immediate retries)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date for fetch/sync in YYYY-MM-DD format (defaults to current trading date / previous session)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Primary directory for {company_name}_Day.parquet files (defaults to '<Desktop>/NSE Equity Companies')",
    )
    parser.add_argument(
        "--folder-name",
        type=str,
        default=DEFAULT_FOLDER_NAME,
        help="Custom folder name on Desktop if --data-dir is not explicitly passed",
    )
    parser.add_argument(
        "--secondary-dir",
        type=str,
        default=None,
        help="Optional secondary sync directory (if present, kept in sync)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable detailed debug logging",
    )
    return parser.parse_args()

def main():
    args = parse_args()

    # Build config
    config = CollectorConfig(
        data_dir=Path(args.data_dir) if args.data_dir else None,
        folder_name=args.folder_name,
        secondary_data_dir=Path(args.secondary_dir) if args.secondary_dir else None,
        max_workers=args.workers,
        max_attempts=args.attempts,
    )

    setup_logging(config.logs_dir, verbose=args.verbose)
    logger = logging.getLogger("CLI")

    logger.info("=" * 80)
    logger.info("   GROWW DAILY CANDLE DATA COLLECTION SYSTEM")
    logger.info(f"   Workers: {config.max_workers} | Max Attempts: {config.max_attempts} | Timeout: {config.request_timeout}s")
    logger.info(f"   Detected Desktop:   {config.desktop_dir}")
    logger.info(f"   Primary Data Dir:   {config.data_dir} (Already Existed: {config.folder_already_existed})")
    logger.info(f"   Metadata Subdir:    {config.metadata_dir}")
    if config.secondary_data_dir:
        logger.info(f"   Secondary Data Dir: {config.secondary_data_dir}")
    logger.info("=" * 80)

    # Initialize services
    calendar = NSECalendarService(config)
    calendar.initialize()
    migration_tracker = MigrationTracker(config)
    store = ParquetStore(config)
    verification_service = DataVerificationService(config, store=store, calendar=calendar)
    sync_service = InstrumentSyncService(
        config,
        store=store,
        migration_tracker=migration_tracker,
        verification_service=verification_service,
    )
    fetch_engine = ConcurrencyFetchEngine(config, store=store, calendar=calendar, migration_tracker=migration_tracker)
    audit_service = HistoricalAuditService(config, store=store, calendar=calendar)

    # Parse date if provided
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Expected YYYY-MM-DD.")
            sys.exit(1)

    if args.daemon:
        scheduler = DailyScheduler(
            config=config,
            calendar=calendar,
            sync_service=sync_service,
            fetch_engine=fetch_engine,
            audit_service=audit_service,
        )
        try:
            scheduler.start_daemon()
        except KeyboardInterrupt:
            scheduler.stop()
            logger.info("Daemon interrupted by user.")
        return

    if args.task in ("sync_instruments", "verify_existing"):
        rep_mgr = sync_service.synchronize()
        print("\n" + rep_mgr.compile_full_text_report())

    elif args.task == "detect_migrations":
        mig_data = migration_tracker.detect_migrations(target_date)
        from .reports import NSEEQMigrationReport
        mig_rep = NSEEQMigrationReport(
            comparison_date=mig_data["comparison_date"],
            current_date=mig_data["current_date"],
            is_baseline=mig_data["is_baseline"],
            migrations=mig_data["migrations"],
            total_moved_out_of_eq=mig_data["summary"]["moved_out_of_eq"],
            total_entered_eq=mig_data["summary"]["entered_eq"],
            total_newly_listed=mig_data["summary"]["newly_listed"],
            total_migrated_into_eq=mig_data["summary"]["migrated_into_eq"],
            total_removed_from_master=mig_data["summary"]["removed_from_master"],
            total_unverified_status=mig_data["summary"]["unverified_status"],
        )
        print("\n" + mig_rep.format_text())

    elif args.task == "daily_fetch":
        rep = fetch_engine.fetch_daily_candles_4pm(target_date)
        print("\n" + rep.compile_full_text_report())

    elif args.task == "final_sync":
        rep = fetch_engine.final_candle_sync_6am(target_date)
        print("\n" + rep.compile_full_text_report())

    elif args.task == "audit":
        rep = audit_service.run_completeness_audit(attempt_gap_recovery=True)
        print("\n" + rep.compile_full_text_report())

    elif args.task == "benchmark":
        print("\n=== RUNNING BENCHMARK ACROSS FULL NSE EQ UNIVERSE (300 WORKERS) ===")
        import time
        t0 = time.time()
        test_session = calendar.get_previous_trading_session()
        print(f"Testing full universe fetch for completed session: {test_session}...")
        rep = fetch_engine.final_candle_sync_6am(test_session)
        elapsed = time.time() - t0
        print("\n" + rep.compile_full_text_report())
        print(f"\n>>> BENCHMARK RESULT: Completed in {elapsed:.2f} seconds ({elapsed/60:.2f} minutes).")
        if elapsed <= 300.0:
            print(">>> PERFORMANCE TARGET ACHIEVED: Well under 5-minute requirement!")
        else:
            print(">>> PERFORMANCE WARNING: Exceeded 5-minute target.")

    elif args.task == "run_all":
        print("\n--- EXECUTING COMPLETE PIPELINE (ALL TASKS) ---")
        # 1. Sync instruments & migrations & verify
        sync_rep = sync_service.synchronize()
        print(f"1. Instrument Sync & Verification Finished.")

        # 2. Final sync / correction
        final_rep = fetch_engine.final_candle_sync_6am(target_date)
        print("\n" + final_rep.compile_full_text_report())

        # 3. Completeness audit & gap recovery
        audit_rep = audit_service.run_completeness_audit(attempt_gap_recovery=True)
        print("\n" + audit_rep.compile_full_text_report())

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
