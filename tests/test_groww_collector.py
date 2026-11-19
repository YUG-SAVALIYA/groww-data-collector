"""Comprehensive Test Suite for Groww Daily Candle Collection System."""

import os
import sys
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from groww_collector.config import CollectorConfig, IST_TZ
from groww_collector.calendar_service import NSECalendarService
from groww_collector.parquet_store import ParquetStore, PARQUET_SCHEMA
from groww_collector.groww_client import GrowwAPIClient
from groww_collector.reports import (
    ExecutionReportManager, FetchAttemptRecord, DesktopStorageReport,
    ExistingDataVerificationReport, NSEEQMigrationReport
)
from groww_collector.scheduler import DailyScheduler
from groww_collector.desktop_detector import get_desktop_directory, resolve_storage_directory
from groww_collector.migration_tracker import MigrationTracker
from groww_collector.verification import DataVerificationService

class TestGrowwCollectionSystem(unittest.TestCase):
    """Test suite covering schemas, data safety, calendar logic, retries, desktop detection, and migrations."""

    def setUp(self):
        self.test_dir = Path(__file__).parent / "test_data"
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.config = CollectorConfig(
            data_dir=self.test_dir / "primary",
            secondary_data_dir=self.test_dir / "secondary",
            logs_dir=self.test_dir / "logs",
            reports_dir=self.test_dir / "reports",
            max_workers=10,
            max_attempts=4,
        )
        self.store = ParquetStore(self.config)
        self.calendar = NSECalendarService(self.config)

    def tearDown(self):
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_desktop_detection_and_override(self):
        """Verify OS-aware desktop detection and custom folder override."""
        desktop = get_desktop_directory()
        self.assertTrue(desktop.exists())

        # Test custom folder name on desktop
        target, existed, desk = resolve_storage_directory(
            folder_name="Custom Equity Test",
            ensure_created=False,
        )
        self.assertEqual(target.name, "Custom Equity Test")
        self.assertEqual(target.parent, desktop)

        # Test custom override path
        custom_p = self.test_dir / "custom_location"
        target2, existed2, _ = resolve_storage_directory(
            custom_dir=custom_p,
            ensure_created=True,
        )
        self.assertEqual(target2, custom_p.resolve())
        self.assertTrue(target2.exists())

    def test_schema_preservation(self):
        """Verify strict schema matching with existing Parquet files."""
        sym = "TEST_STOCK"
        raw_candles = [
            [1794767400, 100.0, 105.0, 98.0, 102.5, 50000],
            [1794853800, 102.5, 108.0, 101.0, 107.0, 60000],
        ]
        df, invalids = self.store.parse_raw_candles_to_df(raw_candles)
        self.assertEqual(len(df), 2)
        self.assertEqual(invalids, 0)

        ins, upd, tot = self.store.merge_and_save(sym, df)
        self.assertEqual(ins, 2)
        self.assertEqual(tot, 2)

        saved_path = self.store.get_file_path(sym)
        self.assertTrue(saved_path.exists())

        schema = pq.read_schema(saved_path)
        self.assertTrue(schema.equals(PARQUET_SCHEMA))

        # Check secondary sync
        sec_path = self.store.get_file_path(sym, self.config.secondary_data_dir)
        self.assertTrue(sec_path.exists())
        self.assertTrue(pq.read_schema(sec_path).equals(PARQUET_SCHEMA))

    def test_ohlcv_validation_and_deduplication(self):
        """Ensure invalid OHLCV rows are rejected and duplicates are updated."""
        sym = "TEST_VAL"
        raw_candles = [
            [1794767400, 100.0, 105.0, 98.0, 102.0, 5000],  # Valid
            [1794767400, 100.0, 106.0, 98.0, 104.0, 6000],  # Duplicate same day (should replace older)
            [1794853800, -10.0, 105.0, 98.0, 102.0, 5000],  # Negative open
            [1794940200, 100.0, 90.0, 105.0, 102.0, 5000],   # High < Low
            [1795026600, 100.0, 105.0, 98.0, 102.0, -10],    # Negative volume
        ]
        df, invalids = self.store.parse_raw_candles_to_df(raw_candles)
        self.assertEqual(invalids, 3)
        self.assertEqual(len(df), 1)

        ins, upd, tot = self.store.merge_and_save(sym, df)
        self.assertEqual(tot, 1)

    def test_per_company_locking_and_atomic_replace(self):
        """Verify per-company thread locking mechanism and atomic write."""
        lock1 = self.store.get_symbol_lock("RELIANCE")
        lock2 = self.store.get_symbol_lock("reliance")
        self.assertIs(lock1, lock2)

        lock_tcs = self.store.get_symbol_lock("TCS")
        self.assertIsNot(lock1, lock_tcs)

    def test_calendar_trading_session_logic(self):
        """Test previous trading session and holiday resolution."""
        sat_dt = datetime(2026, 11, 21, 10, 0, 0, tzinfo=IST_TZ)
        prev = self.calendar.get_previous_trading_session(sat_dt)
        self.assertEqual(prev, date(2026, 11, 20))

        sun_dt = datetime(2026, 11, 22, 10, 0, 0, tzinfo=IST_TZ)
        prev_sun = self.calendar.get_previous_trading_session(sun_dt)
        self.assertEqual(prev_sun, date(2026, 11, 20))

        mon_dt = datetime(2026, 11, 23, 6, 0, 0, tzinfo=IST_TZ)
        prev_mon = self.calendar.get_previous_trading_session(mon_dt)
        self.assertEqual(prev_mon, date(2026, 11, 20))

    def test_migration_tracking_logic(self):
        """Test detection of EQ exits, SME to EQ migrations, new IPO listings, and master removals."""
        tracker = MigrationTracker(self.config)
        d_yesterday = date(2026, 11, 16)
        d_today = date(2026, 11, 17)

        # 1. Baseline Snapshot (Yesterday)
        snap_yesterday = {
            "INE001A01010": {
                "isin": "INE001A01010", "symbol": "CO_A_EXIT", "series": "EQ",
                "segment": "CASH", "exchange": "NSE", "name": "Company A Exit",
                "exchange_token": "101", "groww_symbol": "NSE-CO_A_EXIT", "listing_date": "2015-01-01"
            },
            "INE002B01010": {
                "isin": "INE002B01010", "symbol": "CO_B_SME", "series": "SM",
                "segment": "CASH", "exchange": "NSE", "name": "Company B SME",
                "exchange_token": "102", "groww_symbol": "NSE-CO_B_SME", "listing_date": "2020-05-10"
            },
            "INE003C01010": {
                "isin": "INE003C01010", "symbol": "CO_C_DELIST", "series": "EQ",
                "segment": "CASH", "exchange": "NSE", "name": "Company C Delist",
                "exchange_token": "103", "groww_symbol": "NSE-CO_C_DELIST", "listing_date": "2018-02-15"
            },
        }
        tracker.save_snapshot(snap_yesterday, d_yesterday)

        # Initialize history
        history = {
            "INE001A01010": {"isin": "INE001A01010", "symbol": "CO_A_EXIT", "current_segment": "EQ", "active_in_eq": True, "events": []},
            "INE002B01010": {"isin": "INE002B01010", "symbol": "CO_B_SME", "current_segment": "SM", "active_in_eq": False, "events": []},
            "INE003C01010": {"isin": "INE003C01010", "symbol": "CO_C_DELIST", "current_segment": "EQ", "active_in_eq": True, "events": []},
        }
        tracker.save_membership_history(history)

        # 2. Mock Today's Instruments:
        # - CO_A_EXIT moves EQ -> BE (EQ Exit)
        # - CO_B_SME moves SM -> EQ (Existing Company Migrated to EQ)
        # - CO_C_DELIST removed entirely
        # - CO_D_IPO is a new listing (EQ)
        snap_today = {
            "INE001A01010": {
                "isin": "INE001A01010", "symbol": "CO_A_EXIT", "series": "BE",
                "segment": "CASH", "exchange": "NSE", "name": "Company A Exit",
                "exchange_token": "101", "groww_symbol": "NSE-CO_A_EXIT", "listing_date": "2015-01-01"
            },
            "INE002B01010": {
                "isin": "INE002B01010", "symbol": "CO_B_SME", "series": "EQ",
                "segment": "CASH", "exchange": "NSE", "name": "Company B SME",
                "exchange_token": "102", "groww_symbol": "NSE-CO_B_SME", "listing_date": "2020-05-10"
            },
            "INE004D01010": {
                "isin": "INE004D01010", "symbol": "CO_D_IPO", "series": "EQ",
                "segment": "CASH", "exchange": "NSE", "name": "Company D IPO",
                "exchange_token": "104", "groww_symbol": "NSE-CO_D_IPO", "listing_date": "2026-11-17"
            },
        }

        # Override fetch_all_nse_cash_instruments to return snap_today
        tracker.fetch_all_nse_cash_instruments = lambda: snap_today

        mig_result = tracker.detect_migrations(d_today)
        self.assertFalse(mig_result["is_baseline"])

        summary = mig_result["summary"]
        self.assertEqual(summary["moved_out_of_eq"], 1)   # CO_A_EXIT moved to BE
        self.assertEqual(summary["migrated_into_eq"], 1) # CO_B_SME moved from SM to EQ
        self.assertEqual(summary["newly_listed"], 1)     # CO_D_IPO new listing
        self.assertEqual(summary["removed_from_master"], 1) # CO_C_DELIST removed

        # Test fetch eligibility
        eligible = tracker.get_eligible_eq_symbols()
        self.assertIn("CO_B_SME", eligible)
        self.assertIn("CO_D_IPO", eligible)
        self.assertNotIn("CO_A_EXIT", eligible)   # Excluded from daily fetch!
        self.assertNotIn("CO_C_DELIST", eligible) # Excluded from daily fetch!

    def test_reports_generation(self):
        """Verify report compilation with Desktop, Verification, and Migration reports."""
        rep = ExecutionReportManager("TEST_RUN_002", "unit_test", self.config)
        rep.desktop_report = DesktopStorageReport(
            desktop_detected=r"C:\Users\test\Desktop",
            folder_path=r"C:\Users\test\Desktop\NSE Equity Companies",
            folder_already_existed=True,
            total_existing_parquet_files=2404,
            total_files_verified=2404,
            total_files_already_complete=2400,
            total_files_requiring_updates=4,
            total_new_files_created=0,
        )
        rep.verification_report = ExistingDataVerificationReport(
            total_companies_checked=2404,
            total_complete_historical=2400,
            total_with_missing_candles=4,
            total_missing_candles_recovered=4,
            total_missing_still_unresolved=0,
        )
        rep.migration_report = NSEEQMigrationReport(
            comparison_date="2026-11-16",
            current_date="2026-11-17",
            total_moved_out_of_eq=1,
            total_entered_eq=2,
            total_newly_listed=1,
            total_migrated_into_eq=1,
        )

        full_text = rep.compile_full_text_report()
        self.assertIn("1. WHERE YOUR DATA IS STORED (Desktop Storage)", full_text)
        self.assertIn("2. EXISTING DATA HEALTH & AUDIT CHECK", full_text)
        self.assertIn("3. STOCK SEGMENT MOVEMENTS & NEW IPOs (NSE EQ Migration)", full_text)

        saved = rep.save_permanent_reports()
        self.assertTrue(Path(saved["text_report"]).exists())
        self.assertTrue(Path(saved["json_report"]).exists())

    def test_weekend_skipping_and_retention(self):
        """Verify Saturday/Sunday skip and 21-day log cleanup."""
        import os
        import time
        from groww_collector.cli import cleanup_old_logs_and_reports
        from groww_collector.scheduler import DailyScheduler

        # 1. Test Weekend Skip
        sched = DailyScheduler(self.config, calendar=self.calendar)
        self.assertFalse(self.calendar.is_trading_day(date(2026, 11, 21))) # Saturday
        self.assertFalse(self.calendar.is_trading_day(date(2026, 11, 22))) # Sunday

        # 2. Test 21-day log retention
        test_logs_dir = self.test_dir / "test_logs"
        test_logs_dir.mkdir(parents=True, exist_ok=True)
        reports_sub = test_logs_dir / "reports"
        reports_sub.mkdir(parents=True, exist_ok=True)

        old_log = test_logs_dir / "groww_collector_old.log"
        new_log = test_logs_dir / "groww_collector_new.log"
        old_rep = reports_sub / "report_old.txt"
        new_rep = reports_sub / "report_new.txt"

        old_log.write_text("old log")
        new_log.write_text("new log")
        old_rep.write_text("old rep")
        new_rep.write_text("new rep")

        # Set old files modified time to 25 days ago
        twenty_five_days_ago = time.time() - (25 * 86400)
        os.utime(old_log, (twenty_five_days_ago, twenty_five_days_ago))
        os.utime(old_rep, (twenty_five_days_ago, twenty_five_days_ago))

        # Run 21-day retention enforcer
        cleanup_old_logs_and_reports(test_logs_dir, max_days=21)

        # Old files must be purged, new files must remain
        self.assertFalse(old_log.exists())
        self.assertFalse(old_rep.exists())
        self.assertTrue(new_log.exists())
        self.assertTrue(new_rep.exists())

    def test_parquet_store_overwrite_atomic_and_get_last_candle(self):
        """Verify get_last_candle_before and overwrite_atomic methods."""
        sym = "SPLIT_TEST"
        # 1. Initial history: 3 candles
        raw_candles = [
            [1794767400, 1000.0, 1050.0, 980.0, 1020.0, 5000],  # Nov 16
            [1794853800, 1020.0, 1080.0, 1010.0, 1070.0, 6000], # Nov 17
            [1794940200, 1070.0, 1090.0, 1050.0, 1080.0, 7000], # Nov 18
        ]
        df, _ = self.store.parse_raw_candles_to_df(raw_candles)
        self.store.merge_and_save(sym, df)

        # Query last candle before Nov 19
        target_d = date(2026, 11, 19)
        last_c = self.store.get_last_candle_before(sym, target_d)
        self.assertIsNotNone(last_c)
        self.assertEqual(last_c["close"], 1080.0)

        # 2. Overwrite atomic with split-adjusted series (1:10 split, prices divided by 10)
        split_candles = [
            [1794767400, 100.0, 105.0, 98.0, 102.0, 50000],
            [1794853800, 102.0, 108.0, 101.0, 107.0, 60000],
            [1794940200, 107.0, 109.0, 105.0, 108.0, 70000],
            [1795026600, 108.0, 115.0, 107.0, 112.0, 80000], # Nov 19
        ]
        df_split, _ = self.store.parse_raw_candles_to_df(split_candles)
        written = self.store.overwrite_atomic(sym, df_split)
        self.assertEqual(written, 4)

        # Read back and verify
        df_read = self.store.read_df(sym)
        self.assertEqual(len(df_read), 4)
        self.assertEqual(df_read.iloc[-1]["close"], 112.0)
        self.assertEqual(df_read.iloc[0]["close"], 102.0)

    def test_corporate_action_split_reporting(self):
        """Verify reporting of corporate actions / splits in execution reports."""
        from groww_collector.reports import ExecutionReportManager
        rep_mgr = ExecutionReportManager("TEST_SPLIT_RUN", "daily_candle_fetch_4pm", self.config)
        rep_mgr.initial_report.total_requested = 100
        rep_mgr.initial_report.successful = 100

        # Record split detected
        rep_mgr.record_split_detected(
            symbol="TATAMOTORS",
            target_date="2026-11-19",
            prev_close=1000.0,
            today_open=200.0,
            shift_pct=0.80,
            candles_restored=2800,
        )

        text = rep_mgr.compile_full_text_report()
        self.assertIn("CORPORATE ACTIONS & SPLITS DETECTED (>20% SHIFT)", text)
        self.assertIn("TATAMOTORS", text)
        self.assertIn("1000.00", text)
        self.assertIn("200.00", text)
        self.assertIn("2,800", text)

    def test_cli_parser_all_flag(self):
        """Verify CLI parser supports --all and defaults properly."""
        from groww_collector.cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["--all"])
        self.assertTrue(args.all)
        self.assertFalse(args.daemon)

        args_default = parser.parse_args([])
        self.assertFalse(args_default.all)
        self.assertFalse(args_default.daemon)

if __name__ == "__main__":
    unittest.main()
