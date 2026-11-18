"""NSE Trading Calendar and Holiday Service for Asia/Kolkata."""

from __future__ import annotations
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
import requests
import pandas as pd

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ

logger = logging.getLogger(__name__)

class NSECalendarService:
    """Manages NSE trading calendar, exchange holidays, and trading session calculations."""

    def __init__(self, config: CollectorConfig = DEFAULT_CONFIG):
        self.config = config
        self._holiday_dates: set[date] = set()
        self._master_trading_dates: list[date] = []
        self._initialized = False
        self._last_holiday_fetch_date: date | None = None

    def initialize(self, force: bool = False):
        """Load holiday lists and benchmark historical calendar."""
        today = datetime.now(IST_TZ).date()
        # Auto-refresh holidays every Monday or if year changes or if forced
        needs_refresh = (
            force
            or not self._initialized
            or (self._last_holiday_fetch_date and (today - self._last_holiday_fetch_date).days >= 7)
        )
        if not needs_refresh:
            return
        self._load_holidays()
        self._load_master_calendar()
        self._initialized = True
        self._last_holiday_fetch_date = today

    def _load_holidays(self):
        """Fetch live NSE holidays or load cached/fallback holidays."""
        cache_file = self.config.logs_dir / "nse_holidays_cache.json"
        fetched = False

        try:
            headers = {"User-Agent": self.config.user_agent}
            resp = requests.get(self.config.nse_holiday_api_url, headers=headers, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                cm_holidays = data.get("CM", [])
                dates = set()
                for item in cm_holidays:
                    # tradingDate format: "26-Jan-2026"
                    raw_dt = item.get("tradingDate")
                    if raw_dt:
                        try:
                            dt = datetime.strptime(raw_dt.strip(), "%d-%b-%Y").date()
                            dates.add(dt)
                        except Exception:
                            pass
                if dates:
                    self._holiday_dates = dates
                    fetched = True
                    # Cache to disk
                    try:
                        with open(cache_file, "w", encoding="utf-8") as f:
                            json.dump([d.isoformat() for d in sorted(dates)], f)
                    except Exception as e:
                        logger.warning(f"Could not write holiday cache: {e}")
        except Exception as e:
            logger.warning(f"Failed to fetch live NSE holidays from API: {e}")

        # Fallback to cache file if live fetch didn't succeed
        if not fetched and cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                    self._holiday_dates = {date.fromisoformat(d) for d in cached}
                    fetched = True
            except Exception as e:
                logger.warning(f"Could not load cached holidays: {e}")

        # If still empty, add standard recurring Indian national/market holidays for recent years
        if not self._holiday_dates:
            self._holiday_dates = self._get_hardcoded_holidays()

    def _get_hardcoded_holidays(self) -> set[date]:
        """Known NSE Equity market holidays as safe fallback."""
        return {
            date(2026, 1, 15), # Maharashtra Municipal Election
            date(2026, 1, 26), # Republic Day
            date(2026, 2, 15), # Mahashivratri
            date(2026, 3, 20), # Holi
            date(2026, 3, 31), # Id-Ul-Fitr
            date(2026, 4, 3),  # Good Friday
            date(2026, 4, 14), # Dr. Baba Saheb Ambedkar Jayanti
            date(2026, 5, 1),  # Maharashtra Day
            date(2026, 6, 17), # Bakri Id
            date(2026, 8, 15), # Independence Day
            date(2026, 10, 2), # Mahatma Gandhi Jayanti
            date(2026, 10, 20),# Dussehra
            date(2026, 11, 8), # Diwali Laxmi Pujan
            date(2026, 11, 10),# Diwali Balipratipada
            date(2026, 11, 24),# Gurunanak Jayanti
            date(2026, 12, 25),# Christmas
        }

    def _load_master_calendar(self):
        """Load benchmark trading dates from benchmark stock (e.g. RELIANCE)."""
        benchmark_file = self.config.data_dir / f"{self.config.benchmark_symbol}_Day.parquet"
        if not benchmark_file.exists() and self.config.secondary_data_dir:
            benchmark_file = self.config.secondary_data_dir / f"{self.config.benchmark_symbol}_Day.parquet"

        dates = set()
        if not benchmark_file.exists():
            # On a brand-new device with zero initial data, auto-provision benchmark file from Groww
            try:
                from .groww_client import GrowwAPIClient
                from .parquet_store import ParquetStore
                client = GrowwAPIClient(self.config)
                store = ParquetStore(self.config)
                start_d = date(self.config.history_start_year, 1, 1)
                end_d = datetime.now(IST_TZ).date()
                s_ms, e_ms = client.build_time_range_ms(start_d, end_d)
                candles, st, err, _ = client.fetch_candles_single_attempt(self.config.benchmark_symbol, s_ms, e_ms)
                if candles:
                    b_df, _ = store.parse_raw_candles_to_df(candles)
                    if not b_df.empty:
                        store.merge_and_save(self.config.benchmark_symbol, b_df)
                        benchmark_file = self.config.data_dir / f"{self.config.benchmark_symbol}_Day.parquet"
                        logger.info(f"Auto-provisioned benchmark calendar ({len(b_df)} trading days) for fresh device.")
            except Exception as e:
                logger.debug(f"Could not auto-provision benchmark on fresh device: {e}")

        if benchmark_file.exists():
            try:
                df = pd.read_parquet(benchmark_file, columns=["datetime"])
                dt_series = pd.to_datetime(df["datetime"]).dt.tz_convert(IST_TZ).dt.date
                dates.update(dt_series.tolist())
            except Exception as e:
                logger.error(f"Error reading benchmark calendar from {benchmark_file}: {e}")

        # Ensure historical dates from 2015 to today are sorted
        self._master_trading_dates = sorted(list(dates))

    def is_trading_day(self, check_date: date) -> bool:
        """Return True if check_date is a standard NSE equity trading day (Mon-Fri, non-holiday)."""
        self.initialize()
        # Saturday = 5, Sunday = 6
        if check_date.weekday() >= 5:
            return False
        if check_date in self._holiday_dates:
            return False
        return True

    def get_latest_completed_session(self, ref_dt: datetime | None = None) -> date:
        """Identify the most recent completed NSE trading session.
        
        If ref_dt is after 15:30 on a trading day, today's session is completed.
        Otherwise (e.g. 6:00 AM, morning, weekend, or holiday), looks strictly backwards.
        """
        self.initialize()
        if ref_dt is None:
            ref_dt = datetime.now(IST_TZ)
        else:
            if ref_dt.tzinfo is None:
                ref_dt = ref_dt.replace(tzinfo=IST_TZ)
            else:
                ref_dt = ref_dt.astimezone(IST_TZ)

        curr_date = ref_dt.date()
        # If today is a trading day and market has concluded (after 15:30 IST)
        if self.is_trading_day(curr_date) and (ref_dt.hour > 15 or (ref_dt.hour == 15 and ref_dt.minute >= 30)):
            return curr_date

        # Otherwise look back starting from yesterday
        target_date = curr_date - timedelta(days=1)
        while not self.is_trading_day(target_date):
            target_date -= timedelta(days=1)
        return target_date

    def get_previous_trading_session(self, ref_dt: datetime | None = None) -> date:
        """Identifies the previous completed trading session prior to current day.
        
        Spec: Used at 6:00 AM IST to fetch/correct the previous completed session.
        Even if called at 4:00 PM, this returns the completed session prior to today.
        """
        self.initialize()
        if ref_dt is None:
            ref_dt = datetime.now(IST_TZ)
        else:
            if ref_dt.tzinfo is None:
                ref_dt = ref_dt.replace(tzinfo=IST_TZ)
            else:
                ref_dt = ref_dt.astimezone(IST_TZ)

        # Strictly look back from yesterday
        target_date = ref_dt.date() - timedelta(days=1)
        while not self.is_trading_day(target_date):
            target_date -= timedelta(days=1)
        return target_date

    def get_expected_trading_sessions(self, start_date: date, end_date: date) -> list[date]:
        """Return all valid trading dates from start_date to end_date (inclusive).
        
        The benchmark historical trading dates represent the definitive empirical NSE calendar
        (active every trading session since 2015). For any sessions beyond the benchmark,
        is_trading_day rules are applied.
        """
        self.initialize()
        if start_date > end_date:
            return []

        benchmark_dates = self._master_trading_dates
        if benchmark_dates:
            bench_start = benchmark_dates[0]
            bench_end = benchmark_dates[-1]

            pre_sessions = []
            if start_date < bench_start:
                curr = start_date
                end_pre = min(end_date, bench_start - timedelta(days=1))
                while curr <= end_pre:
                    if self.is_trading_day(curr):
                        pre_sessions.append(curr)
                    curr += timedelta(days=1)

            # In-benchmark range
            sessions = pre_sessions + [d for d in benchmark_dates if start_date <= d <= end_date]

            # If end_date extends beyond the benchmark data
            if end_date > bench_end:
                curr = max(start_date, bench_end + timedelta(days=1))
                while curr <= end_date:
                    if self.is_trading_day(curr):
                        sessions.append(curr)
                    curr += timedelta(days=1)

            return sessions
        else:
            # Safe fallback if benchmark calendar file not yet initialized
            sessions = []
            curr = start_date
            while curr <= end_date:
                if self.is_trading_day(curr):
                    sessions.append(curr)
                curr += timedelta(days=1)
            return sessions

        # Fallback if no benchmark file available
        sessions = []
        curr = start_date
        while curr <= end_date:
            if self.is_trading_day(curr):
                sessions.append(curr)
            curr += timedelta(days=1)
        return sessions
