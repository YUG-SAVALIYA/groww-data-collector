"""Configuration settings for Groww Daily Candle Collection System."""

from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass, field
from datetime import timezone, timedelta
import zoneinfo

from .desktop_detector import get_desktop_directory, resolve_storage_directory, DEFAULT_FOLDER_NAME

IST_TZ = zoneinfo.ZoneInfo("Asia/Kolkata")
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

@dataclass
class CollectorConfig:
    """Master configuration for the data collection system."""

    # Data Storage Directories
    data_dir: Path | None = None
    folder_name: str = DEFAULT_FOLDER_NAME
    secondary_data_dir: Path | None = None
    logs_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent.parent / "logs")
    reports_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent.parent / "logs" / "reports")
    
    # Metadata Filenames
    metadata_dirname: str = "metadata"
    catalog_filename: str = "companies_catalog.csv"
    audit_filename: str = "company_listing_coverage_audit.csv"

    # Concurrency and Networking
    max_workers: int = 300
    connection_pool_size: int = 350
    request_timeout: float = 8.0
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    # Retry Policy (Total 4 attempts = 1 initial + 3 immediate retries)
    max_attempts: int = 4
    immediate_retry: bool = True  # No sleep between retries

    # Remote URLs
    groww_chart_url: str = (
        "https://groww.in/v1/api/charting_service/v2/chart/exchange/NSE/segment/CASH/{symbol}"
    )
    groww_live_price_url: str = (
        "https://groww.in/v1/api/stocks_data/v1/tr_live_prices/exchange/NSE/segment/CASH/{symbol}/latest"
    )
    groww_instruments_url: str = (
        "https://growwapi-assets.groww.in/instruments/instrument.csv"
    )
    nse_equity_l_url: str = (
        "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    )
    nse_holiday_api_url: str = (
        "https://www.nseindia.com/api/holiday-master?type=trading"
    )

    # Bundled Master Universe (Guaranteed 2,404 Active NSE Series EQ Companies)
    bundled_universe_path: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent / "data" / "nse_equity_active.csv"
    )

    # Benchmark symbol for master trading calendar
    benchmark_symbol: str = "RELIANCE"
    history_start_year: int = 2015

    # Schedule Times (Asia/Kolkata)
    schedule_sync_instruments: str = "15:55"
    schedule_daily_fetch: str = "16:00"
    schedule_audit: str = "06:30"

    # Retention Policy & Weekend Rules
    log_retention_days: int = 21
    skip_weekends: bool = True

    # Runtime computed attributes
    desktop_dir: Path = field(init=False)
    folder_already_existed: bool = field(init=False, default=False)
    metadata_dir: Path = field(init=False)
    snapshots_dir: Path = field(init=False)
    membership_history_file: Path = field(init=False)
    migrations_log_file: Path = field(init=False)

    def __post_init__(self):
        target_path, already_existed, desktop = resolve_storage_directory(
            custom_dir=self.data_dir,
            folder_name=self.folder_name,
            ensure_created=True,
        )
        self.data_dir = target_path
        self.folder_already_existed = already_existed
        self.desktop_dir = desktop

        # Isolated metadata directory inside data_dir
        self.metadata_dir = self.data_dir / self.metadata_dirname
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.snapshots_dir = self.metadata_dir / "snapshots"
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        self.membership_history_file = self.metadata_dir / "segment_membership_history.json"
        self.migrations_log_file = self.metadata_dir / "migrations_log.csv"

        if self.secondary_data_dir:
            self.secondary_data_dir = Path(self.secondary_data_dir)
            self.secondary_data_dir.mkdir(parents=True, exist_ok=True)

        self.logs_dir = Path(self.logs_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir = Path(self.reports_dir)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    @property
    def catalog_path(self) -> Path:
        p_meta = self.metadata_dir / self.catalog_filename
        p_root = self.data_dir / self.catalog_filename
        if p_meta.exists() or not p_root.exists():
            return p_meta
        return p_root

    @property
    def audit_path(self) -> Path:
        p_meta = self.metadata_dir / self.audit_filename
        p_root = self.data_dir / self.audit_filename
        if p_meta.exists() or not p_root.exists():
            return p_meta
        return p_root

# Global default config instance
DEFAULT_CONFIG = CollectorConfig()
