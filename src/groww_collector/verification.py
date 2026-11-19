"""Existing Folder and Data Verification Service.

Verifies existing files, checks schemas and duplicates, compares data against
the official NSE trading calendar from listing date, heals missing gaps in batch,
and provisions new company files.
"""

from __future__ import annotations
import time
import logging
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
import pandas as pd
import pyarrow.parquet as pq

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ
from .groww_client import GrowwAPIClient
from .calendar_service import NSECalendarService
from .parquet_store import ParquetStore, PARQUET_SCHEMA
from .reports import DesktopStorageReport, ExistingDataVerificationReport, CandleChangeRecord

logger = logging.getLogger(__name__)

class DataVerificationService:
    """Inspects, verifies, and repairs Parquet data for all NSE EQ companies."""

    def __init__(
        self,
        config: CollectorConfig = DEFAULT_CONFIG,
        client: GrowwAPIClient | None = None,
        store: ParquetStore | None = None,
        calendar: NSECalendarService | None = None,
    ):
        self.config = config
        self.client = client or GrowwAPIClient(config)
        self.store = store or ParquetStore(config)
        self.calendar = calendar or NSECalendarService(config)

    def verify_and_synchronize_universe(
        self,
        universe_symbols: list[str],
        company_metadata: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[DesktopStorageReport, ExistingDataVerificationReport]:
        """Execute full verification routine on existing folder.
        
        Returns:
            (DesktopStorageReport, ExistingDataVerificationReport)
        """
        t0 = time.time()
        logger.info(f"Starting Existing Folder and Data Verification on: {self.config.data_dir}...")

        # 1. Desktop Storage Report metrics
        desktop_rep = DesktopStorageReport(
            desktop_detected=str(self.config.desktop_dir),
            folder_path=str(self.config.data_dir),
            folder_already_existed=self.config.folder_already_existed,
        )

        existing_files = {
            f.name.replace("_Day.parquet", "").strip().upper(): f
            for f in self.config.data_dir.glob("*_Day.parquet")
        }
        desktop_rep.total_existing_parquet_files = len(existing_files)

        verif_rep = ExistingDataVerificationReport()

        latest_session = self.calendar.get_latest_completed_session()
        min_start_date = date(self.config.history_start_year, 1, 1)

        case_a_symbols = []  # File exists
        case_b_symbols = []  # File does not exist

        for sym in universe_symbols:
            if sym in existing_files:
                case_a_symbols.append(sym)
            else:
                case_b_symbols.append(sym)

        logger.info(
            f"Verification breakdown: {len(case_a_symbols)} files exist (Case A), "
            f"{len(case_b_symbols)} files missing (Case B)."
        )

        # ----------------------------------------------------
        # Process Case A: Inspect existing files concurrently
        # ----------------------------------------------------
        companies_complete = []
        companies_needing_gap_fill = []

        def inspect_existing_file(sym: str):
            fpath = existing_files[sym]
            meta = (company_metadata or {}).get(sym, {})
            l_dt_raw = meta.get("listing_date") or meta.get("Listing_Date") or ""
            l_dt = None
            if l_dt_raw:
                try:
                    l_dt = pd.to_datetime(l_dt_raw, format="%d-%b-%Y").date()
                except Exception:
                    try:
                        l_dt = pd.to_datetime(l_dt_raw).date()
                    except Exception:
                        pass

            # Always check from 2015 so migrated/re-listed stocks (like NESTLEIND, UEL) recover full Groww history
            exp_start = min_start_date
            expected_dates = set(self.calendar.get_expected_trading_sessions(exp_start, latest_session))

            dates_present = set()
            dup_cnt = 0
            inv_cnt = 0
            schema_ok = False

            try:
                # Check schema
                s = pq.read_schema(fpath)
                schema_ok = s.equals(PARQUET_SCHEMA)
            except Exception:
                schema_ok = False

            try:
                df = pd.read_parquet(fpath)
                if not df.empty:
                    dt_series = pd.to_datetime(df["datetime"]).dt.tz_convert(IST_TZ).dt.floor("D")
                    dates_list = dt_series.dt.date.tolist()
                    dup_cnt = len(dates_list) - len(set(dates_list))

                    v_df, inv = self.store.parse_raw_candles_to_df(
                        [[t.timestamp(), o, h, l, c, v] for t, o, h, l, c, v in zip(
                            dt_series, df["open"], df["high"], df["low"], df["close"], df["volume"]
                        )]
                    )
                    inv_cnt = inv

                    if dup_cnt > 0 or not schema_ok or inv_cnt > 0:
                        # Clean duplicates, corrupt rows, or normalize schema atomically
                        self.store.merge_and_save(sym, v_df)

                    dates_present = set(v_df["datetime"].dt.tz_convert(IST_TZ).dt.date.tolist())
            except Exception as e:
                logger.warning(f"Error inspecting {sym}: {e}")

            missing = sorted(list(expected_dates - dates_present))
            return {
                "symbol": sym,
                "dates_present": len(dates_present),
                "expected_dates": len(expected_dates),
                "missing": missing,
                "duplicates": dup_cnt,
                "invalids": inv_cnt,
                "schema_ok": schema_ok,
            }

        with ThreadPoolExecutor(max_workers=min(32, max(1, len(case_a_symbols)))) as ex:
            inspections = list(ex.map(inspect_existing_file, case_a_symbols))

        desktop_rep.total_files_verified = len(inspections)
        verif_rep.total_companies_checked = len(universe_symbols)

        for res in inspections:
            verif_rep.total_duplicates_corrected += res["duplicates"]
            verif_rep.total_invalids_corrected += res["invalids"]
            if not res["missing"]:
                companies_complete.append(res["symbol"])
            else:
                companies_needing_gap_fill.append((res["symbol"], res["missing"]))

        desktop_rep.total_files_already_complete = len(companies_complete)
        desktop_rep.total_files_requiring_updates = len(companies_needing_gap_fill)

        logger.info(
            f"Inspection complete: {len(companies_complete)} files already complete (no re-download needed). "
            f"{len(companies_needing_gap_fill)} files have missing dates to heal."
        )

        # ----------------------------------------------------
        # Process Case B: Create new Parquet files for missing symbols
        # ----------------------------------------------------
        newly_created = 0
        now_ms = int(datetime.now(IST_TZ).timestamp() * 1000)
        today_d = datetime.now(IST_TZ).date()
        yesterday_dt = datetime(today_d.year, today_d.month, today_d.day, 0, 0, 0, tzinfo=IST_TZ)
        yesterday_ms = int(yesterday_dt.timestamp() * 1000)

        def create_new_company_file(sym: str):
            # Fetch complete available history from 2015 to yesterday via Charting Service
            start_year = self.config.history_start_year
            start_dt = datetime(start_year, 1, 1, 0, 0, 0, tzinfo=IST_TZ)
            start_ms = int(start_dt.timestamp() * 1000)

            # 1. Historical candles strictly up to yesterday using Charting Service
            for attempt in range(1, self.config.max_attempts + 1):
                candles, status, err, _ = self.client.fetch_candles_single_attempt(sym, start_ms, yesterday_ms)
                if candles:
                    df, _ = self.store.parse_raw_candles_to_df(candles)
                    if not df.empty:
                        # Exclude any today candle from charting service
                        df_hist = df[df["datetime"].dt.date < today_d].copy()
                        if not df_hist.empty:
                            self.store.merge_and_save(sym, df_hist)
                        break
                if status == 404:
                    break

            # 2. For today's candle, strictly use Live Quote API (tr_live_prices)
            if self.calendar.is_trading_day(today_d):
                c_live, _, _, _ = self.client.fetch_live_quote_candle(sym, today_d)
                if c_live:
                    df_live, _ = self.store.parse_raw_candles_to_df(c_live)
                    if not df_live.empty:
                        self.store.merge_and_save(sym, df_live)

            fpath = self.store.get_file_path(sym)
            if fpath.exists():
                try:
                    cnt = len(pd.read_parquet(fpath, columns=["datetime"]))
                    return sym, True, cnt
                except Exception:
                    pass
            return sym, False, 0

        if case_b_symbols:
            logger.info(f"Creating {len(case_b_symbols)} missing company files across {self.config.max_workers} workers...")
            pending = list(case_b_symbols)
            for wave in range(1, 3):
                if not pending:
                    break
                failed_in_wave = []
                with ThreadPoolExecutor(max_workers=min(self.config.max_workers, len(pending))) as ex:
                    futures = {ex.submit(create_new_company_file, s): s for s in pending}
                    for fut in as_completed(futures):
                        s, ok, cnt = fut.result()
                        if ok:
                            newly_created += 1
                        else:
                            failed_in_wave.append(s)
                pending = failed_in_wave
                if pending:
                    logger.warning(f"Wave {wave}: {len(pending)} companies need retry. Retrying...")

        desktop_rep.total_new_files_created = newly_created

        # ----------------------------------------------------
        # Heal Case A missing gaps in batch ranges
        # ----------------------------------------------------
        recovered_cnt = 0
        unresolved_cnt = 0
        unresolved_companies_cnt = 0

        if companies_needing_gap_fill:
            logger.info(f"Healing missing gaps across {len(companies_needing_gap_fill)} companies...")

            def heal_company_gaps(item):
                sym, missing_list = item
                if not missing_list:
                    return sym, 0, 0, []

                today_d = datetime.now(IST_TZ).date()
                historical_missing = [d for d in missing_list if d < today_d]
                needs_today = (today_d in missing_list)

                rec = 0
                unres = len(missing_list)
                healed_items = []

                # 1. Historical missing dates (strictly date < today) fetched via Charting Service API
                if historical_missing:
                    d_min = min(historical_missing)
                    d_max = max(historical_missing)
                    start_ms, end_ms = self.client.build_time_range_ms(d_min, d_max)
                    missing_hist_set = set(historical_missing)

                    for attempt in range(1, self.config.max_attempts + 1):
                        candles, status, err, _ = self.client.fetch_candles_single_attempt(sym, start_ms, end_ms)
                        if candles:
                            df, _ = self.store.parse_raw_candles_to_df(candles)
                            if not df.empty:
                                # Strictly filter to historical missing dates (EXCLUDING today)
                                df_to_add = df[(df["datetime"].dt.date.isin(missing_hist_set)) & (df["datetime"].dt.date < today_d)].copy()
                                if not df_to_add.empty:
                                    self.store.merge_and_save(sym, df_to_add)
                                    rec += len(df_to_add)
                                    unres -= len(df_to_add)
                                    for _, row in df_to_add.iterrows():
                                        healed_items.append(CandleChangeRecord(
                                            symbol=sym,
                                            target_date=str(row["datetime"])[:10],
                                            change_type="HEALED_HIST",
                                            open=float(row["open"]),
                                            high=float(row["high"]),
                                            low=float(row["low"]),
                                            close=float(row["close"]),
                                            volume=int(row["volume"]),
                                        ))
                                    break
                        if status == 404:
                            break

                # 2. For today's candle: ALWAYS strictly use Live Quote API (tr_live_prices) - NEVER Charting Service!
                if needs_today and self.calendar.is_trading_day(today_d):
                    c_live, _, _, _ = self.client.fetch_live_quote_candle(sym, today_d)
                    if c_live:
                        df_live, _ = self.store.parse_raw_candles_to_df(c_live)
                        if not df_live.empty:
                            self.store.merge_and_save(sym, df_live)
                            rec += 1
                            unres = max(0, unres - 1)
                            for _, row in df_live.iterrows():
                                healed_items.append(CandleChangeRecord(
                                    symbol=sym,
                                    target_date=str(row["datetime"])[:10],
                                    change_type="HEALED_LIVE",
                                    open=float(row["open"]),
                                    high=float(row["high"]),
                                    low=float(row["low"]),
                                    close=float(row["close"]),
                                    volume=int(row["volume"]),
                                ))

                return sym, rec, unres, healed_items

            with ThreadPoolExecutor(max_workers=min(self.config.max_workers, len(companies_needing_gap_fill))) as ex:
                heal_results = list(ex.map(heal_company_gaps, companies_needing_gap_fill))

            for _, rec, unres, h_items in heal_results:
                recovered_cnt += rec
                unresolved_cnt += unres
                if unres > 0:
                    unresolved_companies_cnt += 1
                verif_rep.healed_records.extend(h_items)

        verif_rep.total_complete_historical = len(companies_complete) + (len(companies_needing_gap_fill) - unresolved_companies_cnt if companies_needing_gap_fill else 0)
        verif_rep.total_with_missing_candles = len(companies_needing_gap_fill)
        verif_rep.total_missing_candles_recovered = recovered_cnt
        verif_rep.total_missing_still_unresolved = unresolved_cnt

        logger.info(
            f"Verification & synchronization finished in {time.time()-t0:.2f}s: "
            f"Complete: {verif_rep.total_complete_historical}, Recovered: {recovered_cnt}, "
            f"New files: {newly_created}."
        )

        return desktop_rep, verif_rep
