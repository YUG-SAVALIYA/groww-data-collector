"""High-performance, thread-safe Groww API client with connection pooling."""

from __future__ import annotations
import threading
import time
from typing import Any
from datetime import date, datetime
import requests
from requests.adapters import HTTPAdapter
import urllib3

# Suppress insecure request warnings if any
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ

class GrowwAPIClient:
    """Thread-safe Groww charting client with per-thread sessions and connection pooling."""

    def __init__(self, config: CollectorConfig = DEFAULT_CONFIG):
        self.config = config
        self._local = threading.local()

    def get_session(self) -> requests.Session:
        """Get or initialize thread-local Session with connection pooling."""
        if not hasattr(self._local, "session"):
            session = requests.Session()
            session.trust_env = False  # Avoid environment proxy overhead
            adapter = HTTPAdapter(
                pool_connections=20,
                pool_maxsize=20,
                max_retries=0,  # Retries handled explicitly at application level
            )
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            session.headers.update({
                "User-Agent": self.config.user_agent,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
            })
            self._local.session = session
        return self._local.session

    def get_candidate_symbols(self, symbol: str) -> list[str]:
        """Return primary and fallback symbol representations for Groww."""
        sym = symbol.strip().upper()
        candidates = [sym]
        if "&" in sym:
            candidates.append(sym.replace("&", "_"))
        if "_" in sym:
            candidates.append(sym.replace("_", "&"))
        if sym.endswith("-BE"):
            candidates.append(sym.replace("-BE", ""))
        if sym.endswith("-SM"):
            candidates.append(sym.replace("-SM", ""))
        return candidates

    def fetch_candles_single_attempt(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
    ) -> tuple[list[list[Any]], int, str | None, str]:
        """Execute a single attempt to fetch candles from Groww API.
        
        Returns:
            (candles, http_status, error_message, candidate_used)
        """
        session = self.get_session()
        candidates = self.get_candidate_symbols(symbol)
        last_error = None
        last_status = 0

        for candidate in candidates:
            url = (
                f"https://groww.in/v1/api/charting_service/v2/chart/"
                f"exchange/NSE/segment/CASH/{candidate}"
                f"?intervalInMinutes=1440&minimal=false"
                f"&startTimeInMillis={start_ms}&endTimeInMillis={end_ms}"
            )
            try:
                resp = session.get(url, timeout=self.config.request_timeout)
                last_status = resp.status_code
                if resp.status_code == 200:
                    payload = resp.json()
                    candles = payload.get("candles", [])
                    if candles:
                        return candles, 200, None, candidate
                    # 200 with empty candles: try next candidate if any
                elif resp.status_code in (400, 404):
                    # Bad request or not found under candidate: try next
                    continue
                else:
                    last_error = f"HTTP {resp.status_code}"
            except requests.Timeout:
                last_error = "Timeout"
                last_status = 408
            except requests.RequestException as e:
                last_error = f"Network error: {type(e).__name__}"
                last_status = 500
            except Exception as e:
                last_error = f"Error: {str(e)[:60]}"
                last_status = 500

        # If loop completed without returning valid candles
        if last_status == 200:
            return [], 200, "Empty candle data returned", candidates[0]
        return [], last_status, last_error or "Unknown failure", candidates[0]

    def build_time_range_ms(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[int, int]:
        """Convert start and end dates to epoch milliseconds in IST."""
        # Start at 00:00:00 of start_date in IST
        start_dt = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=IST_TZ)
        # End at 23:59:59 of end_date in IST
        end_dt = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=IST_TZ)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        return start_ms, end_ms
