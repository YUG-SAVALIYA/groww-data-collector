"""Clean, Human-Readable Logging, Audit, and Execution Reports Generator.

Formats complex data collection metrics into clear, plain human language with
intuitive visual sections and transparent executive summaries.
"""

from __future__ import annotations
import json
import logging
import uuid
import threading
from datetime import datetime, date
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Any
import pandas as pd

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ

logger = logging.getLogger(__name__)

@dataclass
class CandleChangeRecord:
    """Record of an individual candle being added or updated."""
    symbol: str
    target_date: str
    change_type: str  # "ADDED" or "UPDATED" or "HEALED"
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: int = 0

@dataclass
class CorporateActionRecord:
    """Record of a stock split / consolidation / major corporate action (>20% price shift)."""
    symbol: str
    target_date: str
    prev_close: float
    today_open: float
    shift_pct: float
    candles_restored: int

@dataclass
class FetchAttemptRecord:
    """Individual attempt record for a company fetch."""
    run_id: str
    symbol: str
    target_info: str
    attempt_number: int
    status_code: int
    status_str: str  # SUCCESS, EMPTY, ERROR, RECOVERED, UNRESOLVED
    error: str | None = None
    duration_seconds: float = 0.0
    candle_count: int = 0
    inserted_count: int = 0
    updated_count: int = 0

@dataclass
class DesktopStorageReport:
    """Section 1: Desktop Storage Overview in Plain English."""
    desktop_detected: str = ""
    folder_path: str = ""
    folder_already_existed: bool = False
    total_existing_parquet_files: int = 0
    total_files_verified: int = 0
    total_files_already_complete: int = 0
    total_files_requiring_updates: int = 0
    total_new_files_created: int = 0

    def format_text(self) -> str:
        folder_state = "Existing folder safely reused (No files lost)" if self.folder_already_existed else "Brand new folder created on Desktop"
        return (
            "--------------------------------------------------------------------------------\n"
            "  1. WHERE YOUR DATA IS STORED (Desktop Storage)\n"
            "--------------------------------------------------------------------------------\n"
            f"  * Detected Desktop Location : {self.desktop_detected}\n"
            f"  * Active Data Folder        : {self.folder_path}\n"
            f"  * Folder Status             : {folder_state}\n"
            f"  * Total Parquet Files Found : {self.total_existing_parquet_files:,} files on disk\n"
            f"  * Files Verified            : {self.total_files_verified:,} files inspected\n"
            f"  * Up-to-Date Files Skipped  : {self.total_files_already_complete:,} (Already 100% complete - zero network wasted!)\n"
            f"  * Files Needing Updates     : {self.total_files_requiring_updates:,} files\n"
            f"  * New Company Files Created : {self.total_new_files_created:,} new files\n\n"
        )

@dataclass
class ExistingDataVerificationReport:
    """Section 2: Data Health & Completeness in Plain English."""
    total_companies_checked: int = 0
    total_complete_historical: int = 0
    total_with_missing_candles: int = 0
    total_missing_candles_recovered: int = 0
    total_missing_still_unresolved: int = 0
    total_existing_candles_updated: int = 0
    total_duplicates_corrected: int = 0
    total_invalids_corrected: int = 0
    healed_records: list[CandleChangeRecord] = field(default_factory=list)

    def format_text(self) -> str:
        pct = (self.total_complete_historical / self.total_companies_checked * 100) if self.total_companies_checked else 100.0
        lines = [
            "--------------------------------------------------------------------------------\n",
            "  2. EXISTING DATA HEALTH & AUDIT CHECK\n",
            "--------------------------------------------------------------------------------\n",
            f"  * Total Companies Audited   : {self.total_companies_checked:,} companies\n",
            f"  * 100% Complete History     : {self.total_complete_historical:,} companies ({pct:.1f}% of entire universe)\n",
            f"  * Companies with Missing Day: {self.total_with_missing_candles:,} companies\n",
            f"  * Missing Candles Recovered : {self.total_missing_candles_recovered:,} candles restored from Groww\n",
            f"  * Genuinely Halted / Unavail: {self.total_missing_still_unresolved:,} (No trades on exchange - no fake candles made)\n",
            f"  * Duplicate Dates Cleaned   : {self.total_duplicates_corrected:,} duplicate records removed\n",
            f"  * Corrupt / Incomplete Rows Filtered: {self.total_invalids_corrected:,} rows cleaned (e.g. NaN volume, null values, or broken prices)\n\n",
        ]

        if self.healed_records:
            lines.append(f"  [HEALED / RESTORED CANDLES - {len(self.healed_records)} CANDLES]\n")
            lines.append(f"  | {'Symbol':<12} | {'Date':<12} | {'Open':<10} | {'High':<10} | {'Low':<10} | {'Close':<10} | {'Volume':<12} |\n")
            lines.append(f"  |{'-'*14}|{'-'*14}|{'-'*12}|{'-'*12}|{'-'*12}|{'-'*12}|{'-'*14}|\n")
            for r in self.healed_records[:40]:
                v_str = f"{r.volume:,}"
                lines.append(f"  | {r.symbol:<12} | {r.target_date:<12} | {r.open:<10.2f} | {r.high:<10.2f} | {r.low:<10.2f} | {r.close:<10.2f} | {v_str:<12} |\n")
            if len(self.healed_records) > 40:
                lines.append(f"  ... and {len(self.healed_records) - 40} more healed candles.\n")
            lines.append("\n")

        return "".join(lines)

@dataclass
class NSEEQMigrationReport:
    """Section 3: Segment Migration & IPO Tracking in Plain English."""
    comparison_date: str | None = None
    current_date: str = ""
    is_baseline: bool = False
    migrations: list[dict[str, Any]] = field(default_factory=list)
    total_moved_out_of_eq: int = 0
    total_entered_eq: int = 0
    total_newly_listed: int = 0
    total_migrated_into_eq: int = 0
    total_removed_from_master: int = 0
    total_unverified_status: int = 0

    def format_text(self) -> str:
        lines = [
            "--------------------------------------------------------------------------------\n"
            "  3. STOCK SEGMENT MOVEMENTS & NEW IPOs (NSE EQ Migration)\n"
            "--------------------------------------------------------------------------------\n"
        ]
        if self.is_baseline:
            lines.append(
                f"  * Initial Baseline Created on {self.current_date}.\n"
                "    All NSE Cash instruments have been snapshotted into metadata/snapshots/.\n"
                "    Future runs will automatically detect any stock moving between segments.\n\n"
            )
            return "".join(lines)

        lines.append(f"  * Compared Dates: {self.comparison_date} vs {self.current_date}\n")

        if not self.migrations:
            lines.append("  * Good news: No companies changed segments today. All stay active in NSE EQ.\n")
        else:
            lines.append(
                f"\n  | {'Company Symbol':<16} | {'Previous':<10} | {'Current':<10} | {'What Happened':<20} | {'Action Taken':<22} |\n"
                f"  |{'-'*18}|{'-'*12}|{'-'*12}|{'-'*22}|{'-'*24}|\n"
            )
            for m in self.migrations:
                comp = str(m.get("Symbol", ""))[:16]
                prev = str(m.get("Previous_Segment", ""))[:10]
                curr = str(m.get("Current_Segment", ""))[:10]
                m_type = str(m.get("Migration_Type", ""))[:20]
                act = str(m.get("Action", ""))[:22]
                lines.append(f"  | {comp:<16} | {prev:<10} | {curr:<10} | {m_type:<20} | {act:<22} |\n")

        lines.append(
            f"\n  * Summary of Movements:\n"
            f"    - Companies that Left EQ (e.g. moved to BE/SM) : {self.total_moved_out_of_eq} (Daily EQ fetch stopped; historical files kept)\n"
            f"    - Brand New IPOs Listed Today                  : {self.total_newly_listed} (Initial candle downloaded)\n"
            f"    - Companies Migrated Into EQ (from BE/SM)      : {self.total_migrated_into_eq} (Full historical data downloaded)\n"
            f"    - Delisted / Removed from Exchange Master      : {self.total_removed_from_master} (Files preserved safely)\n\n"
        )
        return "".join(lines)

@dataclass
class InitialFetchReport:
    """Section 4: Daily Candle Collection Results in Plain English."""
    total_universe: int = 0
    total_requested: int = 0
    successful: int = 0
    empty_responses: int = 0
    api_network_errors: int = 0
    total_candles_received: int = 0
    total_new_inserted: int = 0
    total_existing_updated: int = 0
    total_splits_detected: int = 0
    added_records: list[CandleChangeRecord] = field(default_factory=list)
    updated_records: list[CandleChangeRecord] = field(default_factory=list)
    split_records: list[CorporateActionRecord] = field(default_factory=list)

    def format_text(self) -> str:
        pct = (self.successful / self.total_requested * 100) if self.total_requested else 0.0
        lines = [
            "--------------------------------------------------------------------------------\n",
            "  DAILY CANDLE COLLECTION: ADDED, UPDATED & SPLIT CANDLES\n",
            "--------------------------------------------------------------------------------\n",
            f"  * Total Companies Requested : {self.total_requested:,} companies\n",
            f"  * Succeeded on First Attempt: {self.successful:,} companies ({pct:.2f}% first-try success!)\n",
            f"  * Temporary API Glitches    : {self.api_network_errors:,} companies (rescued by immediate retry)\n",
            f"  * Total Candles Downloaded  : {self.total_candles_received:,} daily candles\n",
            f"  * Newly Added Candles       : {self.total_new_inserted:,} candles added\n",
            f"  * Revised / Updated Candles : {self.total_existing_updated:,} candles revised with official live figures\n",
        ]
        if self.total_splits_detected > 0:
            lines.append(f"  * Corporate Actions / Splits: {self.total_splits_detected:,} companies detected (>20% price shift) and 2015-today history adjusted\n")
        lines.append("\n")

        if self.split_records:
            lines.append(f"  [CORPORATE ACTIONS & SPLITS DETECTED (>20% SHIFT) - {len(self.split_records)} COMPANIES]\n")
            lines.append("  * Automatic Full History Re-Download (2015-Today) executed to adjust all historical candles.\n")
            lines.append(f"  | {'Symbol':<12} | {'Date':<12} | {'Prev Close':<12} | {'Today Open':<12} | {'Price Shift':<14} | {'Candles Restored':<18} |\n")
            lines.append(f"  |{'-'*14}|{'-'*14}|{'-'*14}|{'-'*14}|{'-'*16}|{'-'*20}|\n")
            for sr in self.split_records:
                shift_str = f"{sr.shift_pct * 100:+.2f}%"
                rst_str = f"{sr.candles_restored:,}"
                lines.append(f"  | {sr.symbol:<12} | {sr.target_date:<12} | {sr.prev_close:<12.2f} | {sr.today_open:<12.2f} | {shift_str:<14} | {rst_str:<18} |\n")
            lines.append("\n")

        if self.added_records:
            lines.append(f"  [NEWLY ADDED CANDLES - {len(self.added_records)} COMPANIES]\n")
            lines.append(f"  | {'Symbol':<12} | {'Date':<12} | {'Open':<10} | {'High':<10} | {'Low':<10} | {'Close':<10} | {'Volume':<12} |\n")
            lines.append(f"  |{'-'*14}|{'-'*14}|{'-'*12}|{'-'*12}|{'-'*12}|{'-'*12}|{'-'*14}|\n")
            for r in self.added_records[:40]:
                v_str = f"{r.volume:,}"
                lines.append(f"  | {r.symbol:<12} | {r.target_date:<12} | {r.open:<10.2f} | {r.high:<10.2f} | {r.low:<10.2f} | {r.close:<10.2f} | {v_str:<12} |\n")
            if len(self.added_records) > 40:
                lines.append(f"  ... and {len(self.added_records) - 40} more newly added candles.\n")
            lines.append("\n")

        if self.updated_records:
            lines.append(f"  [REVISED / UPDATED CANDLES - {len(self.updated_records)} COMPANIES]\n")
            lines.append(f"  | {'Symbol':<12} | {'Date':<12} | {'Open':<10} | {'High':<10} | {'Low':<10} | {'Close':<10} | {'Volume':<12} |\n")
            lines.append(f"  |{'-'*14}|{'-'*14}|{'-'*12}|{'-'*12}|{'-'*12}|{'-'*12}|{'-'*14}|\n")
            for r in self.updated_records[:40]:
                v_str = f"{r.volume:,}"
                lines.append(f"  | {r.symbol:<12} | {r.target_date:<12} | {r.open:<10.2f} | {r.high:<10.2f} | {r.low:<10.2f} | {r.close:<10.2f} | {v_str:<12} |\n")
            if len(self.updated_records) > 40:
                lines.append(f"  ... and {len(self.updated_records) - 40} more revised candles.\n")
            lines.append("\n")

        return "".join(lines)

@dataclass
class RetryReport:
    """Section 5: Immediate Retries in Plain English."""
    total_requiring_retry: int = 0
    recovered_retry_1: int = 0
    recovered_retry_2: int = 0
    recovered_retry_3: int = 0
    still_unsuccessful: int = 0
    total_candles_recovered: int = 0

    def format_text(self) -> str:
        tot_recovered = self.recovered_retry_1 + self.recovered_retry_2 + self.recovered_retry_3
        return (
            "--------------------------------------------------------------------------------\n"
            "  5. IMMEDIATE RETRY RESULTS (No Sleep / Fast Auto-Healing)\n"
            "--------------------------------------------------------------------------------\n"
            f"  * Companies Needing Retry    : {self.total_requiring_retry:,}\n"
            f"  * Recovered on Retry 1       : {self.recovered_retry_1:,}\n"
            f"  * Recovered on Retry 2       : {self.recovered_retry_2:,}\n"
            f"  * Recovered on Retry 3       : {self.recovered_retry_3:,}\n"
            f"  * Total Rescued by Retries   : {tot_recovered:,} companies successfully saved\n"
            f"  * Remaining Unresolved       : {self.still_unsuccessful:,} (Recorded in Unresolved Report below)\n\n"
        )

@dataclass
class HistoricalAuditReport:
    """Section 6: Historical Coverage in Plain English."""
    total_audited: int = 0
    total_expected_candles: int = 0
    total_actual_candles: int = 0
    total_missing_dates: int = 0
    missing_recovered: int = 0
    missing_unresolved: int = 0
    duplicates_detected_and_corrected: int = 0
    invalid_records_detected: int = 0

    def format_text(self) -> str:
        cov = (self.total_actual_candles / self.total_expected_candles * 100) if self.total_expected_candles else 100.0
        return (
            "--------------------------------------------------------------------------------\n"
            "  6. COMPLETE HISTORICAL MARKET DATA QUALITY\n"
            "--------------------------------------------------------------------------------\n"
            f"  * Total Companies Audited    : {self.total_audited:,}\n"
            f"  * Total Expected Market Days : {self.total_expected_candles:,} days across entire history\n"
            f"  * Actual Daily Candles Held  : {self.total_actual_candles:,} valid daily candles on disk\n"
            f"  * Overall Market Coverage    : {cov:.2f}% (Complete coverage from listing dates to today)\n"
            f"  * Candles Restored from Gaps : {self.missing_recovered:,}\n\n"
        )

@dataclass
class UnresolvedRecord:
    """Row in Section 7: Unresolved Data Report."""
    company: str
    missing_date_or_range: str
    attempts: int
    final_status: str
    error: str

@dataclass
class FinalDailySummary:
    """Section 8: Executive Summary in Plain English."""
    run_id: str
    task_name: str
    total_execution_time_seconds: float = 0.0
    total_companies_processed: int = 0
    total_successful_fetches: int = 0
    total_candles_fetched: int = 0
    total_candles_inserted_updated: int = 0
    total_successful_retries: int = 0
    total_unresolved_companies: int = 0
    total_unresolved_missing_dates: int = 0
    overall_status: str = "COMPLETED"

    def format_text(self) -> str:
        mins = self.total_execution_time_seconds / 60.0
        target_eval = "YES - Fast! (< 5.0 min target met)" if mins <= 5.0 else "NO (> 5.0 min target)"
        return (
            "================================================================================\n"
            "                         FINAL EXECUTIVE SUMMARY                                \n"
            "================================================================================\n"
            f"  * What Ran                  : {self.task_name}\n"
            f"  * Run ID Reference          : {self.run_id}\n"
            f"  * Total Time Taken          : {self.total_execution_time_seconds:.1f} seconds ({mins:.2f} minutes)\n"
            f"  * 5-Minute Performance Goal : {target_eval}\n"
            f"  * Total Companies Processed : {self.total_companies_processed:,} companies\n"
            f"  * Successfully Synchronized : {self.total_successful_fetches:,} companies\n"
            f"  * Total Daily Candles Handled: {self.total_candles_inserted_updated:,} candles\n"
            f"  * Overall Final Status      : {self.overall_status}\n"
            "================================================================================\n"
        )

class ExecutionReportManager:
    """Manages recording of attempts, generation of reports, and permanent file storage."""

    def __init__(self, run_id: str, task_name: str, config: CollectorConfig = DEFAULT_CONFIG):
        self.run_id = run_id
        self.task_name = task_name
        self.config = config
        self.timestamp_str = datetime.now(IST_TZ).strftime("%Y%m%d_%H%M%S")
        
        self.desktop_report: DesktopStorageReport | None = None
        self.verification_report: ExistingDataVerificationReport | None = None
        self.migration_report: NSEEQMigrationReport | None = None
        self.initial_report = InitialFetchReport()
        self.retry_report = RetryReport()
        self.audit_report = HistoricalAuditReport()
        self.unresolved_records: list[UnresolvedRecord] = []
        self.summary = FinalDailySummary(run_id=run_id, task_name=task_name)
        self.all_attempts: list[FetchAttemptRecord] = []
        self._records_lock = threading.Lock()

    def record_candle_change(
        self,
        symbol: str,
        target_date: str,
        change_type: str,
        open_val: float,
        high_val: float,
        low_val: float,
        close_val: float,
        volume_val: int,
    ):
        rec = CandleChangeRecord(
            symbol=symbol,
            target_date=target_date,
            change_type=change_type,
            open=open_val,
            high=high_val,
            low=low_val,
            close=close_val,
            volume=volume_val,
        )
        with self._records_lock:
            if change_type == "ADDED":
                self.initial_report.added_records.append(rec)
            elif change_type == "UPDATED":
                self.initial_report.updated_records.append(rec)

    def record_split_detected(
        self,
        symbol: str,
        target_date: str,
        prev_close: float,
        today_open: float,
        shift_pct: float,
        candles_restored: int,
    ):
        rec = CorporateActionRecord(
            symbol=symbol,
            target_date=target_date,
            prev_close=prev_close,
            today_open=today_open,
            shift_pct=shift_pct,
            candles_restored=candles_restored,
        )
        with self._records_lock:
            self.initial_report.split_records.append(rec)
            self.initial_report.total_splits_detected += 1

    def log_attempt(self, attempt: FetchAttemptRecord):
        self.all_attempts.append(attempt)

    def add_unresolved(self, company: str, target: str, attempts: int, status: str, error: str):
        # Translate error into plain English
        plain_err = error
        if "completed session" in str(error).lower() or "missing" in str(error).lower():
            plain_err = "No trades on exchange (Market halt or illiquid session)"
        elif "empty" in str(error).lower():
            plain_err = "No candle returned by exchange"
        elif "timeout" in str(error).lower():
            plain_err = "Connection timed out"

        self.unresolved_records.append(
            UnresolvedRecord(
                company=company,
                missing_date_or_range=target,
                attempts=attempts,
                final_status="NOT_TRADED / SUSPENDED" if "no trade" in plain_err.lower() else status,
                error=plain_err,
            )
        )

    def generate_unresolved_markdown_table(self, limit: int = 50) -> str:
        lines = [
            "--------------------------------------------------------------------------------\n",
            "  DATA INTEGRITY: MISSING CANDLES & UNRESOLVED COMPANIES\n",
            "--------------------------------------------------------------------------------\n",
        ]
        if not self.unresolved_records:
            lines.append("  * Missing Candles: NONE! 100% of companies have full continuous daily data.\n\n")
            return "".join(lines)

        lines.extend([
            f"  * Detected {len(self.unresolved_records)} company session(s) requiring attention:\n\n",
            f"  | {'Symbol':<12} | {'Missing Date(s)':<18} | {'Status':<20} | {'Action / Detail':<30} |\n",
            f"  |{'-'*14}|{'-'*20}|{'-'*22}|{'-'*32}|\n",
        ])

        for r in self.unresolved_records[:limit]:
            lines.append(
                f"  | {r.company:<12} | {r.missing_date_or_range:<18} | {r.final_status:<20} | {r.error[:30]:<30} |\n"
            )

        if len(self.unresolved_records) > limit:
            lines.append(f"\n  ... and {len(self.unresolved_records) - limit} more records (saved in permanent report).\n\n")
        else:
            lines.append("\n")

        return "".join(lines)

    def compile_full_text_report(self) -> str:
        """Combine all sections into a clean, human-understandable report."""
        now_ist = datetime.now(IST_TZ).strftime("%Y-%m-%d %I:%M %p IST")
        
        banner = (
            "================================================================================\n"
            "                    GROWW COLLECTOR — SYSTEM STATUS REPORT                      \n"
            f"           Time: {now_ist} | Task: {self.task_name}                     \n"
            "================================================================================\n"
            f"  * Storage Location          : {self.config.data_dir}\n"
            f"  * Target Universe           : 2,404 Active NSE Equity (Series EQ) Companies\n"
            f"  * Companies Processed       : {self.summary.total_companies_processed:,}\n"
            f"  * Successful Companies      : {self.summary.total_successful_fetches:,}\n"
            f"  * Total Daily Candles Added : {self.summary.total_candles_inserted_updated:,}\n"
            f"  * Overall Status            : {self.summary.overall_status}\n"
            "================================================================================\n\n"
        )
        parts = [
            banner,
            self.desktop_report.format_text() if self.desktop_report else "",
            self.verification_report.format_text() if self.verification_report else "",
            self.migration_report.format_text() if self.migration_report else "",
            self.initial_report.format_text() if self.initial_report.total_requested > 0 else "",
            self.retry_report.format_text() if self.retry_report.total_requiring_retry > 0 else "",
            self.audit_report.format_text() if self.audit_report.total_audited > 0 else "",
            self.generate_unresolved_markdown_table(),
            self.summary.format_text(),
        ]
        return "".join(p for p in parts if p)

    def save_permanent_reports(self) -> dict[str, str]:
        """Save text report, json summary, and unresolved CSV to disk."""
        base_name = f"report_{self.task_name}_{self.run_id}_{self.timestamp_str}"
        
        # 1. Human-Readable Text Report
        txt_path = self.config.reports_dir / f"{base_name}.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(self.compile_full_text_report())

        # 2. Machine-Readable JSON Summary
        json_data = {
            "run_id": self.run_id,
            "task_name": self.task_name,
            "timestamp": datetime.now(IST_TZ).isoformat(),
            "desktop_storage": asdict(self.desktop_report) if self.desktop_report else None,
            "existing_verification": asdict(self.verification_report) if self.verification_report else None,
            "segment_migration": asdict(self.migration_report) if self.migration_report else None,
            "initial_fetch": asdict(self.initial_report),
            "retry_report": asdict(self.retry_report),
            "audit_report": asdict(self.audit_report),
            "summary": asdict(self.summary),
            "unresolved_count": len(self.unresolved_records),
        }
        json_path = self.config.reports_dir / f"{base_name}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2)

        # 3. Unresolved CSV Table
        unresolved_csv_path = self.config.reports_dir / f"unresolved_{self.task_name}_{self.run_id}.csv"
        if self.unresolved_records:
            unresolved_df = pd.DataFrame([asdict(r) for r in self.unresolved_records])
            unresolved_df.to_csv(unresolved_csv_path, index=False)
        else:
            pd.DataFrame(columns=["company", "missing_date_or_range", "attempts", "final_status", "error"]).to_csv(
                unresolved_csv_path, index=False
            )

        logger.info(f"Saved permanent reports to: {self.config.reports_dir}")
        return {
            "text_report": str(txt_path),
            "json_report": str(json_path),
            "unresolved_csv": str(unresolved_csv_path),
        }
