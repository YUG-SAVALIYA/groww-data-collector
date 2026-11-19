"""High-Concurrency 300-Worker Fetch Engine with Immediate 4-Attempt Retries."""

from __future__ import annotations
import time
import uuid
import logging
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Any
from pathlib import Path
import pandas as pd

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ
from .groww_client import GrowwAPIClient
from .calendar_service import NSECalendarService
from .parquet_store import ParquetStore
from .reports import ExecutionReportManager, FetchAttemptRecord

from .migration_tracker import MigrationTracker

logger = logging.getLogger(__name__)

class ConcurrencyFetchEngine:
    """Orchestrates high-concurrency fetching across 300 workers with immediate 4-stage retry."""

    def __init__(
        self,
        config: CollectorConfig = DEFAULT_CONFIG,
        client: GrowwAPIClient | None = None,
        store: ParquetStore | None = None,
        calendar: NSECalendarService | None = None,
        migration_tracker: MigrationTracker | None = None,
    ):
        self.config = config
        self.client = client or GrowwAPIClient(config)
        self.store = store or ParquetStore(config)
        self.calendar = calendar or NSECalendarService(config)
        self.migration_tracker = migration_tracker or MigrationTracker(config)

    def load_universe_symbols(self) -> list[str]:
        """Load target universe of companies from bundled universe (strictly 2,404 NSE EQ active companies)."""
        # 1. Guaranteed baseline: Bundled 2,404 active NSE Series EQ universe
        if self.config.bundled_universe_path.exists():
            try:
                b_df = pd.read_csv(self.config.bundled_universe_path)
                syms = sorted(b_df["Symbol"].dropna().astype(str).str.strip().str.upper().unique().tolist())
                if len(syms) == 2404:
                    return syms
            except Exception as e:
                logger.warning(f"Could not read bundled universe: {e}")

        syms = set()
        try:
            eligible = self.migration_tracker.get_eligible_eq_symbols()
            if eligible:
                syms.update(eligible)
        except Exception as e:
            logger.debug(f"Could not load eligible symbols from migration tracker: {e}")

        if self.config.catalog_path.exists():
            try:
                cat = pd.read_csv(self.config.catalog_path)
                cat_syms = cat["Symbol"].dropna().astype(str).str.strip().str.upper().unique()
                syms.update(cat_syms)
            except Exception as e:
                logger.warning(f"Could not read catalog: {e}")

        if not syms:
            syms.update([
                f.name.replace("_Day.parquet", "").strip().upper()
                for f in self.config.data_dir.glob("*_Day.parquet")
            ])
        return sorted(list(syms))

    def execute_with_immediate_retries(
        self,
        items: list[Any],
        process_func: Callable[[Any, int, str], tuple[bool, int, int, int, str | None]],
        report_manager: ExecutionReportManager,
        item_to_symbol: Callable[[Any], str],
        item_to_target_info: Callable[[Any], str],
    ) -> dict[str, Any]:
        """Execute processing on items with immediate 4-attempt retry architecture (no sleep).
        
        process_func signature:
            func(item, attempt_number, run_id) -> (is_success, candle_count, inserted_count, updated_count, error_msg)
        """
        run_id = report_manager.run_id
        max_attempts = self.config.max_attempts
        workers = self.config.max_workers

        total_requested = len(items)
        report_manager.initial_report.total_requested = total_requested

        current_items = list(items)
        failed_items = []

        total_candles_received = 0
        total_inserted = 0
        total_updated = 0

        # Attempt 1 through 4
        for attempt_no in range(1, max_attempts + 1):
            is_initial = (attempt_no == 1)
            retry_index = attempt_no - 1  # 1 for Retry 1, 2 for Retry 2, 3 for Retry 3

            stage_name = "Initial Fetch" if is_initial else f"Immediate Retry {retry_index}"
            logger.info(f"[{run_id}] Starting {stage_name} across {len(current_items)} items with {workers} workers...")

            stage_t0 = time.time()
            stage_successes = []
            stage_failures = []

            # Concurrent execution
            with ThreadPoolExecutor(max_workers=min(workers, max(1, len(current_items)))) as executor:
                future_to_item = {
                    executor.submit(process_func, it, attempt_no, run_id): it
                    for it in current_items
                }
                for future in as_completed(future_to_item):
                    it = future_to_item[future]
                    sym = item_to_symbol(it)
                    target_info = item_to_target_info(it)
                    t_call = time.time()
                    try:
                        ok, candles_cnt, ins_cnt, upd_cnt, err = future.result()
                        call_duration = time.time() - t_call
                    except Exception as exc:
                        ok = False
                        candles_cnt = 0
                        ins_cnt = 0
                        upd_cnt = 0
                        err = f"Worker Exception: {str(exc)[:50]}"
                        call_duration = time.time() - t_call

                    record = FetchAttemptRecord(
                        run_id=run_id,
                        symbol=sym,
                        target_info=target_info,
                        attempt_number=attempt_no,
                        status_code=200 if ok else 500,
                        status_str="SUCCESS" if ok else ("EMPTY" if "empty" in str(err).lower() else "ERROR"),
                        error=err,
                        duration_seconds=call_duration,
                        candle_count=candles_cnt,
                        inserted_count=ins_cnt,
                        updated_count=upd_cnt,
                    )
                    report_manager.log_attempt(record)

                    if ok:
                        stage_successes.append((it, candles_cnt, ins_cnt, upd_cnt))
                        total_candles_received += candles_cnt
                        total_inserted += ins_cnt
                        total_updated += upd_cnt
                    else:
                        stage_failures.append((it, err))

            stage_duration = time.time() - stage_t0
            logger.info(
                f"[{run_id}] {stage_name} finished in {stage_duration:.2f}s: "
                f"{len(stage_successes)} succeeded, {len(stage_failures)} failed."
            )

            # Record stats
            if is_initial:
                report_manager.initial_report.successful = len(stage_successes)
                empty_cnt = sum(1 for _, err in stage_failures if "empty" in str(err).lower())
                report_manager.initial_report.empty_responses = empty_cnt
                report_manager.initial_report.api_network_errors = len(stage_failures) - empty_cnt
                report_manager.initial_report.total_candles_received = total_candles_received
                report_manager.initial_report.total_new_inserted = total_inserted
                report_manager.initial_report.total_existing_updated = total_updated

                if stage_failures:
                    report_manager.retry_report.total_requiring_retry = len(stage_failures)
            else:
                # Retry stages
                recovered_cnt = len(stage_successes)
                recovered_candles = sum(c[1] for c in stage_successes)
                report_manager.retry_report.total_candles_recovered += recovered_candles

                if retry_index == 1:
                    report_manager.retry_report.recovered_retry_1 = recovered_cnt
                elif retry_index == 2:
                    report_manager.retry_report.recovered_retry_2 = recovered_cnt
                elif retry_index == 3:
                    report_manager.retry_report.recovered_retry_3 = recovered_cnt

            # If all succeeded, break early
            if not stage_failures:
                logger.info(f"[{run_id}] 100% of items completed successfully at stage {stage_name}!")
                break

            # Prepare items for next immediate retry
            current_items = [it for it, _ in stage_failures]
            if attempt_no == max_attempts:
                # All 4 attempts exhausted
                report_manager.retry_report.still_unsuccessful = len(stage_failures)
                for it, err in stage_failures:
                    sym = item_to_symbol(it)
                    target = item_to_target_info(it)
                    report_manager.add_unresolved(
                        company=sym,
                        target=target,
                        attempts=max_attempts,
                        status="UNRESOLVED",
                        error=err or "Max attempts exceeded",
                    )

        # Populate summary
        total_proc = total_requested
        tot_unresolved = len(report_manager.unresolved_records)
        report_manager.summary.total_companies_processed = total_proc
        report_manager.summary.total_successful_fetches = total_proc - tot_unresolved
        report_manager.summary.total_candles_fetched = total_candles_received
        report_manager.summary.total_candles_inserted_updated = total_inserted + total_updated
        report_manager.summary.total_successful_retries = (
            report_manager.retry_report.recovered_retry_1
            + report_manager.retry_report.recovered_retry_2
            + report_manager.retry_report.recovered_retry_3
        )
        report_manager.summary.total_unresolved_companies = tot_unresolved
        report_manager.summary.overall_status = "COMPLETED_WITH_UNRESOLVED" if tot_unresolved > 0 else "ALL_SUCCESSFUL"

        return {
            "total_requested": total_requested,
            "total_success": total_proc - tot_unresolved,
            "total_unresolved": tot_unresolved,
            "total_candles": total_candles_received,
        }

    def fetch_daily_candles_4pm(self, target_date: date | None = None) -> ExecutionReportManager:
        """Requirement 2: Daily Candle Fetch – 4:00 PM IST.
        
        Fetches the latest available daily candle for every company in the NSE EQ universe,
        inserts new trading-date records, updates existing dates with latest Groww values,
        and saves atomically.
        """
        run_id = f"DAILY_{datetime.now(IST_TZ).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        rep_mgr = ExecutionReportManager(run_id, "daily_candle_fetch_4pm", self.config)
        t_start = time.time()

        if target_date is None:
            # Check if today is a trading day
            today = datetime.now(IST_TZ).date()
            if not self.calendar.is_trading_day(today):
                logger.warning(f"Today ({today}) is not an NSE trading day (weekend/holiday). Skipping 4:00 PM fetch.")
                rep_mgr.summary.overall_status = f"SKIPPED_HOLIDAY_{today}"
                rep_mgr.summary.total_execution_time_seconds = time.time() - t_start
                rep_mgr.save_permanent_reports()
                return rep_mgr
            target_date = today

        symbols = self.load_universe_symbols()
        rep_mgr.initial_report.total_universe = len(symbols)
        logger.info(f"[{run_id}] Executing 4:00 PM Daily Candle Fetch for date {target_date} across {len(symbols)} companies...")

        def worker_task(sym: str, attempt: int, r_id: str):
            # Fetch using Groww Live Quote API (tr_live_prices) to get the accurate closing LTP price!
            # Falls back seamlessly to charting service if live quote is unavailable.
            candles, status, err, sym_used = self.client.fetch_live_quote_candle(sym, target_date)
            if not candles:
                return False, 0, 0, 0, err or "Empty candle response"
            
            today_df, invalids = self.store.parse_raw_candles_to_df(candles)
            if today_df.empty:
                return False, 0, 0, 0, "No valid candles after OHLCV validation"

            today_row = today_df.iloc[-1]
            today_open = float(today_row["open"])

            # Case A: File does not exist yet on disk (new stock or fresh setup)
            if not self.store.exists(sym):
                start_year = self.config.history_start_year
                start_dt = datetime(start_year, 1, 1, 0, 0, 0, tzinfo=IST_TZ)
                start_ms = int(start_dt.timestamp() * 1000)
                yesterday_dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=IST_TZ)
                yesterday_ms = int(yesterday_dt.timestamp() * 1000)

                full_df = today_df.copy()
                for _ in range(self.config.max_attempts):
                    c_hist, _, _, _ = self.client.fetch_candles_single_attempt(sym, start_ms, yesterday_ms)
                    if c_hist:
                        df_hist, _ = self.store.parse_raw_candles_to_df(c_hist)
                        if not df_hist.empty:
                            df_hist = df_hist[df_hist["datetime"].dt.date < target_date].copy()
                            full_df = pd.concat([df_hist, today_df], ignore_index=True)
                            full_df = full_df.drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime").reset_index(drop=True)
                            break

                self.store.overwrite_atomic(sym, full_df)
                rep_mgr.record_candle_change(
                    sym, str(target_date), "ADDED",
                    float(today_row["open"]), float(today_row["high"]),
                    float(today_row["low"]), float(today_row["close"]),
                    int(today_row["volume"]),
                )
                return True, len(full_df), 1, 0, None

            # Case B: File exists on disk -> Check for Stock Split / Consolidation (>20% price shift)
            prev_candle = self.store.get_last_candle_before(sym, target_date)
            if prev_candle is not None:
                prev_close = float(prev_candle["close"])
                if prev_close > 0 and today_open > 0:
                    shift_pct = abs(today_open - prev_close) / prev_close
                    if shift_pct > 0.20:
                        # Corporate action detected! Auto-heal entire 2015-today history with split-adjusted series
                        logger.warning(
                            f"[CORPORATE ACTION / SPLIT DETECTED] {sym}: Yesterday Close={prev_close:.2f}, "
                            f"Today Open={today_open:.2f} (Shift: {shift_pct*100:.2f}% > 20%). "
                            f"Re-fetching adjusted historical series from {self.config.history_start_year} to today..."
                        )
                        start_year = self.config.history_start_year
                        start_dt = datetime(start_year, 1, 1, 0, 0, 0, tzinfo=IST_TZ)
                        start_ms = int(start_dt.timestamp() * 1000)
                        yesterday_dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=IST_TZ)
                        yesterday_ms = int(yesterday_dt.timestamp() * 1000)

                        split_healed = False
                        for h_attempt in range(1, self.config.max_attempts + 1):
                            c_hist, st, err_h, _ = self.client.fetch_candles_single_attempt(sym, start_ms, yesterday_ms)
                            if c_hist:
                                df_hist, _ = self.store.parse_raw_candles_to_df(c_hist)
                                if not df_hist.empty:
                                    df_hist = df_hist[df_hist["datetime"].dt.date < target_date].copy()
                                    full_df = pd.concat([df_hist, today_df], ignore_index=True)
                                    full_df = full_df.drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime").reset_index(drop=True)
                                    tot_written = self.store.overwrite_atomic(sym, full_df)
                                    rep_mgr.record_split_detected(
                                        symbol=sym,
                                        target_date=str(target_date),
                                        prev_close=prev_close,
                                        today_open=today_open,
                                        shift_pct=shift_pct,
                                        candles_restored=tot_written,
                                    )
                                    split_healed = True
                                    break
                            if st == 404:
                                break

                        if split_healed:
                            return True, len(full_df), 0, 1, None
                        else:
                            logger.error(f"Failed to fetch split-adjusted history for {sym}. Proceeding with standard candle update.")

            # Standard Daily Merge & Save
            ins_cnt, upd_cnt, tot = self.store.merge_and_save(sym, today_df)
            if ins_cnt > 0 or upd_cnt > 0:
                o = float(today_row["open"])
                h = float(today_row["high"])
                l = float(today_row["low"])
                c = float(today_row["close"])
                v = int(today_row["volume"])
                d_str = str(today_row["datetime"])[:10]
                if ins_cnt > 0:
                    rep_mgr.record_candle_change(sym, d_str, "ADDED", o, h, l, c, v)
                elif upd_cnt > 0:
                    rep_mgr.record_candle_change(sym, d_str, "UPDATED", o, h, l, c, v)

            return True, len(today_df), ins_cnt, upd_cnt, None

        self.execute_with_immediate_retries(
            items=symbols,
            process_func=worker_task,
            report_manager=rep_mgr,
            item_to_symbol=lambda s: s,
            item_to_target_info=lambda s: str(target_date),
        )

        rep_mgr.summary.total_execution_time_seconds = time.time() - t_start
        rep_mgr.save_permanent_reports()
        return rep_mgr

    def final_candle_sync_6am(self, target_date: date | None = None) -> ExecutionReportManager:
        """Requirement 3: Final Candle Synchronization – 6:00 AM IST.
        
        Fetches the previous completed NSE trading session's daily candle for every company,
        updates the existing candle in the corresponding Parquet file (final correction),
        replaces existing values without duplicating records.
        """
        run_id = f"FINAL6AM_{datetime.now(IST_TZ).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        rep_mgr = ExecutionReportManager(run_id, "final_candle_sync_6am", self.config)
        t_start = time.time()

        if target_date is None:
            target_date = self.calendar.get_previous_trading_session()

        symbols = self.load_universe_symbols()
        rep_mgr.initial_report.total_universe = len(symbols)
        logger.info(f"[{run_id}] Executing 6:00 AM Final Correction for session {target_date} across {len(symbols)} companies...")

        start_date = target_date - pd.Timedelta(days=5)
        start_ms, end_ms = self.client.build_time_range_ms(start_date, target_date)

        def worker_task(sym: str, attempt: int, r_id: str):
            candles, status, err, _ = self.client.fetch_candles_single_attempt(sym, start_ms, end_ms)
            if not candles:
                return False, 0, 0, 0, err or "Empty candle response"
            
            df, _ = self.store.parse_raw_candles_to_df(candles)
            if df.empty:
                return False, 0, 0, 0, "No valid candles parsed"

            df_session = df[df["datetime"].dt.date == target_date]
            if df_session.empty:
                latest_d = df["datetime"].iloc[-1].date()
                if latest_d == target_date:
                    df_session = df.iloc[-1:].copy()
                else:
                    return False, 0, 0, 0, f"Candle for completed session {target_date} missing (got {latest_d})"

            # Replaces / updates values for target_date
            ins_cnt, upd_cnt, tot = self.store.merge_and_save(sym, df_session)
            return True, len(df_session), ins_cnt, upd_cnt, None

        self.execute_with_immediate_retries(
            items=symbols,
            process_func=worker_task,
            report_manager=rep_mgr,
            item_to_symbol=lambda s: s,
            item_to_target_info=lambda s: str(target_date),
        )

        rep_mgr.summary.total_execution_time_seconds = time.time() - t_start
        rep_mgr.save_permanent_reports()
        return rep_mgr
