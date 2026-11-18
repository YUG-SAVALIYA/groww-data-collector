"""NSE EQ Instrument Synchronization & Segment Migration Service (3:55 PM IST).

Tracks multi-segment instrument snapshots, detects EQ exits and entries (new IPO listings
vs migrations from BE/SM/BZ), enforces fetch eligibility, verifies existing Parquet data,
and maintains segment membership history.
"""

from __future__ import annotations
import io
import time
import uuid
import logging
from datetime import date, datetime
from typing import Any
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import requests
import pandas as pd

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ
from .groww_client import GrowwAPIClient
from .parquet_store import ParquetStore
from .migration_tracker import MigrationTracker
from .verification import DataVerificationService
from .reports import ExecutionReportManager, NSEEQMigrationReport

logger = logging.getLogger(__name__)

class InstrumentSyncService:
    """Synchronizes NSE EQ instruments, tracks segment migrations, and manages company catalog."""

    def __init__(
        self,
        config: CollectorConfig = DEFAULT_CONFIG,
        client: GrowwAPIClient | None = None,
        store: ParquetStore | None = None,
        migration_tracker: MigrationTracker | None = None,
        verification_service: DataVerificationService | None = None,
    ):
        self.config = config
        self.client = client or GrowwAPIClient(config)
        self.store = store or ParquetStore(config)
        self.migration_tracker = migration_tracker or MigrationTracker(config)
        self.verification_service = verification_service or DataVerificationService(config, client=self.client, store=self.store)

    def fetch_groww_instruments(self) -> pd.DataFrame:
        """Download and filter Groww instrument master for NSE EQ companies."""
        headers = {"User-Agent": self.config.user_agent}
        resp = requests.get(self.config.groww_instruments_url, headers=headers, timeout=30)
        resp.raise_for_status()

        df = pd.read_csv(io.BytesIO(resp.content), low_memory=False)
        nse_eq = df[(df["exchange"] == "NSE") & (df["series"] == "EQ")].copy()
        
        if "isin" in nse_eq.columns:
            nse_eq = nse_eq[~nse_eq["isin"].astype(str).str.startswith("INF")].copy()

        for col in ["trading_symbol", "groww_symbol", "name", "isin"]:
            if col in nse_eq.columns:
                nse_eq[col] = nse_eq[col].astype(str).str.strip()

        return nse_eq

    def fetch_nse_equity_l(self) -> pd.DataFrame:
        """Download official EQUITY_L.csv from NSE."""
        headers = {"User-Agent": self.config.user_agent}
        resp = requests.get(self.config.nse_equity_l_url, headers=headers, timeout=20)
        resp.raise_for_status()

        df = pd.read_csv(io.BytesIO(resp.content), low_memory=False)
        df.columns = df.columns.str.strip()

        for col in df.columns:
            if df[col].dtype == object or str(df[col].dtype).startswith("str"):
                df[col] = df[col].astype(str).str.strip()

        eq_df = df[df["SERIES"] == "EQ"].copy()
        return eq_df

    def get_unified_universe(self) -> pd.DataFrame:
        """Merge Groww instruments and NSE EQUITY_L into unified metadata DataFrame."""
        groww_eq = pd.DataFrame()
        try:
            groww_eq = self.fetch_groww_instruments()
            logger.info(f"Fetched {len(groww_eq)} NSE EQ instruments from Groww master.")
        except Exception as e:
            logger.warning(f"Could not fetch Groww instrument master: {e}")

        nse_eq = pd.DataFrame()
        try:
            nse_eq = self.fetch_nse_equity_l()
            logger.info(f"Fetched {len(nse_eq)} official NSE Series EQ companies from NSE.")
        except Exception as e:
            logger.warning(f"Could not fetch NSE EQUITY_L: {e}")

        existing_catalog = pd.DataFrame()
        if self.config.catalog_path.exists():
            try:
                existing_catalog = pd.read_csv(self.config.catalog_path)
            except Exception as e:
                logger.warning(f"Could not read existing catalog: {e}")

        records = {}

        # 1. Load bundled official active NSE EQ list (Guaranteed 2,404 active companies baseline)
        if self.config.bundled_universe_path.exists():
            try:
                b_df = pd.read_csv(self.config.bundled_universe_path)
                for _, row in b_df.iterrows():
                    sym = str(row.get("Symbol", "")).strip().upper()
                    if sym:
                        records[sym] = {
                            "Symbol": sym,
                            "Company_Name": str(row.get("Company_Name", "")),
                            "Series": "EQ",
                            "ISIN": str(row.get("ISIN", "")),
                            "Listing_Date": str(row.get("Listing_Date", "")),
                        }
            except Exception as e:
                logger.warning(f"Could not load bundled active list: {e}")

        if not existing_catalog.empty:
            for _, row in existing_catalog.iterrows():
                sym = str(row.get("Symbol", "")).strip().upper()
                if sym and sym in records:
                    records[sym].update({
                        "Company_Name": str(row.get("Company_Name", records[sym]["Company_Name"])),
                        "Listing_Date": str(row.get("Listing_Date", records[sym]["Listing_Date"])),
                    })

        if not nse_eq.empty:
            for _, row in nse_eq.iterrows():
                sym = str(row.get("SYMBOL", "")).strip().upper()
                if sym and sym in records:
                    records[sym].update({
                        "Company_Name": str(row.get("NAME OF COMPANY", "")),
                        "ISIN": str(row.get("ISIN NUMBER", "")),
                        "Listing_Date": str(row.get("DATE OF LISTING", "")),
                    })

        if not groww_eq.empty:
            for _, row in groww_eq.iterrows():
                sym = str(row.get("trading_symbol", "")).strip().upper()
                if sym and sym in records:
                    records[sym]["ISIN"] = records[sym]["ISIN"] or str(row.get("isin", ""))

        universe_df = pd.DataFrame(list(records.values()))
        universe_df = universe_df.sort_values("Symbol").reset_index(drop=True)
        return universe_df

    def synchronize(self) -> ExecutionReportManager:
        """Execute the 3:55 PM synchronization, segment migration, and data verification workflow.
        
        Returns:
            ExecutionReportManager with full Desktop, Verification, and Migration reports.
        """
        t0 = time.time()
        run_id = f"SYNC_{datetime.now(IST_TZ).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        rep_mgr = ExecutionReportManager(run_id, "sync_instruments_and_migrations", self.config)

        logger.info(f"[{run_id}] Starting NSE EQ Instrument & Segment Migration Sync (3:55 PM Task)...")

        # ----------------------------------------------------
        # Step 1: Detect Segment Migrations across all NSE Cash segments
        # ----------------------------------------------------
        mig_data = self.migration_tracker.detect_migrations()
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
        rep_mgr.migration_report = mig_rep

        # Handle specific migration entry cases
        now_dt = datetime.now(IST_TZ)
        now_ms = int(now_dt.timestamp() * 1000)

        for m in mig_data.get("migrations", []):
            sym = m["Symbol"]
            m_type = m["Migration_Type"]

            if m_type == "New Listing":
                # Case B1: Newly listed company -> fetch today's candle only
                logger.info(f"[{run_id}] Handling New Listing for {sym}: Fetching today's candle...")
                start_today_ms = int(datetime(now_dt.year, now_dt.month, now_dt.day, 0, 0, 0, tzinfo=IST_TZ).timestamp() * 1000)
                candles, status, err, _ = self.client.fetch_candles_single_attempt(sym, start_today_ms, now_ms)
                if candles:
                    df, _ = self.store.parse_raw_candles_to_df(candles)
                    if not df.empty:
                        self.store.merge_and_save(sym, df)

            elif m_type == "Existing Company":
                # Case B2: Existing company migrated into EQ from another segment -> fetch full history
                logger.info(f"[{run_id}] Handling Migrated Existing Company for {sym}: Fetching full history...")
                start_year = self.config.history_start_year
                start_ms = int(datetime(start_year, 1, 1, 0, 0, 0, tzinfo=IST_TZ).timestamp() * 1000)
                candles, status, err, _ = self.client.fetch_candles_single_attempt(sym, start_ms, now_ms)
                if candles:
                    df, _ = self.store.parse_raw_candles_to_df(candles)
                    if not df.empty:
                        self.store.merge_and_save(sym, df)

        # ----------------------------------------------------
        # Step 2: Determine currently eligible NSE EQ universe
        # ----------------------------------------------------
        universe_df = self.get_unified_universe()
        eligible_set = set(self.migration_tracker.get_eligible_eq_symbols())
        if not universe_df.empty:
            eligible_set.update(universe_df["Symbol"].str.upper().unique())
        eligible_syms = sorted(list(eligible_set))

        metadata_dict = {
            row["Symbol"]: row.to_dict()
            for _, row in universe_df.iterrows()
        }

        # ----------------------------------------------------
        # Step 3: Run Existing Folder and Data Verification (Requirement 2)
        # ----------------------------------------------------
        desktop_rep, verif_rep = self.verification_service.verify_and_synchronize_universe(
            universe_symbols=eligible_syms,
            company_metadata=metadata_dict,
        )
        rep_mgr.desktop_report = desktop_rep
        rep_mgr.verification_report = verif_rep

        # ----------------------------------------------------
        # Step 4: Update companies_catalog.csv in metadata directory
        # ----------------------------------------------------
        def read_company_meta(sym: str):
            fpath = self.store.get_file_path(sym)
            candle_count = 0
            d_start = ""
            d_end = ""
            if fpath.exists():
                try:
                    cdf = pd.read_parquet(fpath, columns=["datetime"])
                    candle_count = len(cdf)
                    if candle_count > 0:
                        d_start = str(cdf["datetime"].iloc[0])[:10]
                        d_end = str(cdf["datetime"].iloc[-1])[:10]
                except Exception:
                    pass

            row = metadata_dict.get(sym, {})
            status = "COMPLETE" if candle_count > 0 else "PENDING_FETCH"
            return {
                "Symbol": sym,
                "Company_Name": row.get("Company_Name", ""),
                "Series": "EQ",
                "ISIN": row.get("ISIN", ""),
                "Listing_Date": row.get("Listing_Date", ""),
                "Total_Candles": candle_count,
                "Data_Start": d_start,
                "Data_End": d_end,
                "Status": status,
            }

        with ThreadPoolExecutor(max_workers=32) as ex:
            catalog_rows = list(ex.map(read_company_meta, eligible_syms))

        catalog_df = pd.DataFrame(catalog_rows)
        catalog_df.to_csv(self.config.catalog_path, index=False)
        logger.info(f"Saved updated company catalog ({len(catalog_df)} companies) to {self.config.catalog_path}")

        # Summary
        rep_mgr.summary.total_companies_processed = len(eligible_syms)
        rep_mgr.summary.total_successful_fetches = desktop_rep.total_files_already_complete + verif_rep.total_missing_candles_recovered
        rep_mgr.summary.total_candles_inserted_updated = verif_rep.total_missing_candles_recovered
        rep_mgr.summary.total_execution_time_seconds = time.time() - t0
        rep_mgr.summary.overall_status = "SYNC_AND_VERIFICATION_COMPLETE"

        rep_mgr.save_permanent_reports()
        return rep_mgr
