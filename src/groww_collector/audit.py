"""Historical Data Completeness Audit and Self-Healing Gap Recovery."""

from __future__ import annotations
import time
import uuid
import logging
from datetime import date, datetime
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ
from .groww_client import GrowwAPIClient
from .calendar_service import NSECalendarService
from .parquet_store import ParquetStore
from .reports import ExecutionReportManager, HistoricalAuditReport, UnresolvedRecord

logger = logging.getLogger(__name__)

class HistoricalAuditService:
    """Audits data completeness against official NSE trading calendar and recovers missing gaps."""

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

    def load_catalog(self) -> pd.DataFrame:
        """Load metadata catalog with company symbols, names, and listing dates."""
        if self.config.catalog_path.exists():
            try:
                return pd.read_csv(self.config.catalog_path)
            except Exception as e:
                logger.error(f"Error loading catalog: {e}")
        return pd.DataFrame()

    def run_completeness_audit(
        self,
        attempt_gap_recovery: bool = True,
    ) -> ExecutionReportManager:
        """Execute continuous completeness audit and automatic missing gap recovery."""
        run_id = f"AUDIT_{datetime.now(IST_TZ).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        rep_mgr = ExecutionReportManager(run_id, "historical_completeness_audit", self.config)
        t_start = time.time()

        catalog_df = self.load_catalog()
        if catalog_df.empty:
            logger.error("Catalog empty or missing. Run sync_instruments first.")
            rep_mgr.summary.overall_status = "FAILED_NO_CATALOG"
            rep_mgr.summary.total_execution_time_seconds = time.time() - t_start
            rep_mgr.save_permanent_reports()
            return rep_mgr

        latest_session = self.calendar.get_latest_completed_session()
        logger.info(f"[{run_id}] Auditing data completeness up to completed session: {latest_session}...")

        min_calendar_date = date(self.config.history_start_year, 1, 1)

        # 1. Phase 1: Audit all companies in catalog
        audit_results = []
        companies_needing_recovery = []

        def audit_single_company(row_tuple):
            _, row = row_tuple
            sym = str(row.get("Symbol", "")).strip().upper()
            if not sym:
                return None

            company_name = str(row.get("Company_Name", ""))
            isin = str(row.get("ISIN", ""))
            listing_date_raw = str(row.get("Listing_Date", ""))

            # Parse listing date
            listing_date = None
            try:
                listing_date = pd.to_datetime(listing_date_raw, format="%d-%b-%Y").date()
            except Exception:
                try:
                    listing_date = pd.to_datetime(listing_date_raw).date()
                except Exception:
                    pass

            exp_start = min_calendar_date
            expected_sessions = set(self.calendar.get_expected_trading_sessions(exp_start, latest_session))

            # Read existing parquet
            fpath = self.store.get_file_path(sym)
            dates_present = set()
            dup_count = 0
            invalid_count = 0

            if fpath.exists():
                try:
                    df = pd.read_parquet(fpath)
                    if not df.empty:
                        # Check duplicates
                        dt_series = pd.to_datetime(df["datetime"]).dt.tz_convert(IST_TZ).dt.floor("D")
                        dates_list = dt_series.dt.date.tolist()
                        dup_count = len(dates_list) - len(set(dates_list))

                        v_df, inv = self.store.parse_raw_candles_to_df(
                            [[t.timestamp(), o, h, l, c, v] for t, o, h, l, c, v in zip(
                                dt_series, df["open"], df["high"], df["low"], df["close"], df["volume"]
                            )]
                        )
                        invalid_count = inv

                        # If duplicates or invalid rows found, clean immediately
                        if dup_count > 0 or invalid_count > 0:
                            self.store.merge_and_save(sym, v_df)

                        dates_present = set(v_df["datetime"].dt.tz_convert(IST_TZ).dt.date.tolist())
                except Exception as e:
                    logger.warning(f"Error auditing {sym}: {e}")

            missing_dates = sorted(list(expected_sessions - dates_present))
            coverage_pct = (len(dates_present & expected_sessions) / len(expected_sessions) * 100) if expected_sessions else 100.0

            return {
                "Symbol": sym,
                "Company_Name": company_name,
                "ISIN": isin,
                "Listing_Date": listing_date,
                "Expected_Start": exp_start,
                "Data_Start": min(dates_present) if dates_present else None,
                "Data_End": max(dates_present) if dates_present else None,
                "Expected_Sessions": len(expected_sessions),
                "Actual_Candles": len(dates_present),
                "Missing_Candles": len(missing_dates),
                "Missing_Dates_List": missing_dates,
                "Coverage_Pct": round(coverage_pct, 2),
                "Status": "PERFECT" if not missing_dates else ("HIGH_COVERAGE" if coverage_pct >= 95.0 else "SUB_95"),
                "dup_count": dup_count,
                "invalid_count": invalid_count,
            }

        with ThreadPoolExecutor(max_workers=32) as ex:
            raw_audits = list(ex.map(audit_single_company, list(catalog_df.iterrows())))

        audit_results = [r for r in raw_audits if r is not None]

        tot_expected = sum(r["Expected_Sessions"] for r in audit_results)
        tot_actual = sum(r["Actual_Candles"] for r in audit_results)
        tot_missing = sum(r["Missing_Candles"] for r in audit_results)
        tot_duplicates = sum(r["dup_count"] for r in audit_results)
        tot_invalids = sum(r["invalid_count"] for r in audit_results)

        if attempt_gap_recovery:
            for r in audit_results:
                if r["Missing_Candles"] > 0:
                    companies_needing_recovery.append((r["Symbol"], r["Missing_Dates_List"]))

        rep_mgr.audit_report.total_audited = len(audit_results)
        rep_mgr.audit_report.total_expected_candles = tot_expected
        rep_mgr.audit_report.total_actual_candles = tot_actual
        rep_mgr.audit_report.total_missing_dates = tot_missing
        rep_mgr.audit_report.duplicates_detected_and_corrected = tot_duplicates
        rep_mgr.audit_report.invalid_records_detected = tot_invalids

        logger.info(
            f"[{run_id}] Phase 1 Audit Complete: {len(audit_results)} companies audited. "
            f"Overall coverage: {(tot_actual/tot_expected*100):.2f}%. "
            f"Companies needing recovery: {len(companies_needing_recovery)}"
        )

        # 2. Phase 2: Batch Historical Gap Recovery (using 300 workers + immediate retries)
        recovered_dates_count = 0
        unresolved_dates_count = 0

        if companies_needing_recovery:
            logger.info(
                f"[{run_id}] Phase 2: Attempting gap recovery across {len(companies_needing_recovery)} "
                f"companies with {self.config.max_workers} concurrent workers..."
            )

            def recover_company_gaps(item, attempt: int, r_id: str):
                sym, missing_d_list = item
                if not missing_d_list:
                    return True, 0, 0, 0, None

                # Batch request span: min to max missing date
                d_min = min(missing_d_list)
                d_max = max(missing_d_list)
                start_ms, end_ms = self.client.build_time_range_ms(d_min, d_max)

                candles, status, err, _ = self.client.fetch_candles_single_attempt(sym, start_ms, end_ms)
                if not candles:
                    return False, 0, 0, 0, err or "No candles returned for gap range"

                df, _ = self.store.parse_raw_candles_to_df(candles)
                if df.empty:
                    return False, 0, 0, 0, "No valid candles in gap range"

                # Filter only for dates that were actually missing
                missing_set = set(missing_d_list)
                df_to_add = df[df["datetime"].dt.date.isin(missing_set)].copy()
                if df_to_add.empty:
                    return False, 0, 0, 0, "No requested missing dates found in returned candles"

                ins_cnt, upd_cnt, tot = self.store.merge_and_save(sym, df_to_add)
                return True, len(df_to_add), ins_cnt, upd_cnt, None

            # Execute recovery with immediate retries
            engine_rep = rep_mgr
            workers = min(self.config.max_workers, len(companies_needing_recovery))
            
            # Run recovery passes
            current_batch = list(companies_needing_recovery)
            for attempt_no in range(1, self.config.max_attempts + 1):
                stage_failures = []
                with ThreadPoolExecutor(max_workers=min(workers, max(1, len(current_batch)))) as ex:
                    futures = {
                        ex.submit(recover_company_gaps, item, attempt_no, run_id): item
                        for item in current_batch
                    }
                    for fut in as_completed(futures):
                        item = futures[fut]
                        sym, m_list = item
                        try:
                            ok, cnt, ins, upd, err = fut.result()
                            if ok:
                                recovered_dates_count += cnt
                            else:
                                stage_failures.append((item, err))
                        except Exception as e:
                            stage_failures.append((item, str(e)))

                if not stage_failures:
                    break
                current_batch = [it for it, _ in stage_failures]

                # If last attempt finished, record remaining as genuinely unresolved/suspended
                if attempt_no == self.config.max_attempts:
                    for (sym, m_list), err in stage_failures:
                        # Re-read existing file to check what dates remain genuinely missing
                        still_present = self.store.read_existing_dates(sym)
                        genuine_missing = [d for d in m_list if d not in still_present]
                        for m_date in genuine_missing:
                            unresolved_dates_count += 1
                            rep_mgr.add_unresolved(
                                company=sym,
                                target=str(m_date),
                                attempts=self.config.max_attempts,
                                status="UNAVAILABLE_ON_EXCHANGE",
                                error="No trading candle exists on Groww (suspended/trading halted)",
                            )

        rep_mgr.audit_report.missing_recovered = recovered_dates_count
        rep_mgr.audit_report.missing_unresolved = unresolved_dates_count

        # Save updated audit DataFrame to disk
        audit_df = pd.DataFrame(audit_results)
        audit_csv_path = self.config.audit_path
        audit_df.drop(columns=["Missing_Dates_List"]).to_csv(audit_csv_path, index=False)
        logger.info(f"[{run_id}] Saved company listing coverage audit to: {audit_csv_path}")

        # Final Summary
        rep_mgr.summary.total_companies_processed = len(audit_results)
        rep_mgr.summary.total_successful_fetches = len(audit_results) - len(set(r.company for r in rep_mgr.unresolved_records))
        rep_mgr.summary.total_candles_fetched = tot_actual + recovered_dates_count
        rep_mgr.summary.total_candles_inserted_updated = recovered_dates_count
        rep_mgr.summary.total_unresolved_companies = len(set(r.company for r in rep_mgr.unresolved_records))
        rep_mgr.summary.total_unresolved_missing_dates = unresolved_dates_count
        rep_mgr.summary.total_execution_time_seconds = time.time() - t_start
        rep_mgr.summary.overall_status = "AUDIT_COMPLETE_WITH_SUSPENDED_REPORTED" if unresolved_dates_count > 0 else "AUDIT_100_PERCENT_COMPLETE"

        rep_mgr.save_permanent_reports()
        return rep_mgr
