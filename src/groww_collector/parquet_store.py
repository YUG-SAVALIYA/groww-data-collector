"""Thread-safe Parquet Storage Layer with Atomic Writes, Strict Schema & Per-Company Locks."""

from __future__ import annotations
import os
import uuid
import logging
import threading
from typing import Any
from pathlib import Path
from datetime import date, datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ

logger = logging.getLogger(__name__)

# Exact existing PyArrow Schema
PARQUET_SCHEMA = pa.schema([
    ("datetime", pa.timestamp("ms", tz="Asia/Kolkata")),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.int64()),
])

EXPECTED_COLUMNS = ["datetime", "open", "high", "low", "close", "volume"]

class ParquetStore:
    """Thread-safe Parquet manager for {symbol}_Day.parquet files."""

    def __init__(self, config: CollectorConfig = DEFAULT_CONFIG):
        self.config = config
        self._symbol_locks: dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    def get_symbol_lock(self, symbol: str) -> threading.Lock:
        """Retrieve or create a thread lock for a specific company symbol."""
        sym = symbol.strip().upper()
        with self._global_lock:
            if sym not in self._symbol_locks:
                self._symbol_locks[sym] = threading.Lock()
            return self._symbol_locks[sym]

    def get_file_path(self, symbol: str, directory: Path | None = None) -> Path:
        """Return canonical {symbol}_Day.parquet path."""
        target_dir = directory or self.config.data_dir
        return target_dir / f"{symbol.strip().upper()}_Day.parquet"

    def exists(self, symbol: str) -> bool:
        """Check if file exists for company."""
        return self.get_file_path(symbol).exists()

    def read_df(self, symbol: str) -> pd.DataFrame:
        """Read company Parquet file under per-company lock."""
        path = self.get_file_path(symbol)
        lock = self.get_symbol_lock(symbol)
        with lock:
            if not path.exists():
                return pd.DataFrame(columns=EXPECTED_COLUMNS)
            try:
                df = pd.read_parquet(path)
                return df
            except Exception as e:
                logger.error(f"Error reading {path}: {e}")
                return pd.DataFrame(columns=EXPECTED_COLUMNS)

    def read_existing_dates(self, symbol: str) -> set[date]:
        """Fast extraction of existing trading dates in file."""
        path = self.get_file_path(symbol)
        if not path.exists():
            return set()
        try:
            df = pd.read_parquet(path, columns=["datetime"])
            if df.empty:
                return set()
            dt_series = pd.to_datetime(df["datetime"]).dt.tz_convert(IST_TZ).dt.date
            return set(dt_series.tolist())
        except Exception as e:
            logger.warning(f"Error reading dates from {path}: {e}")
            return set()

    def get_last_candle_before(self, symbol: str, target_date: date) -> dict[str, Any] | None:
        """Read the most recent candle strictly prior to target_date from {symbol}_Day.parquet."""
        path = self.get_file_path(symbol)
        if not path.exists():
            return None
        lock = self.get_symbol_lock(symbol)
        with lock:
            try:
                df = pd.read_parquet(path, columns=["datetime", "open", "high", "low", "close", "volume"])
                if df.empty:
                    return None
                dt_series = pd.to_datetime(df["datetime"]).dt.tz_convert(IST_TZ).dt.date
                hist = df[dt_series < target_date]
                if hist.empty:
                    return None
                last_row = hist.iloc[-1]
                return {
                    "datetime": last_row["datetime"],
                    "date": dt_series.iloc[len(hist) - 1],
                    "open": float(last_row["open"]),
                    "high": float(last_row["high"]),
                    "low": float(last_row["low"]),
                    "close": float(last_row["close"]),
                    "volume": int(last_row["volume"]),
                }
            except Exception as e:
                logger.warning(f"Error reading last candle for {symbol}: {e}")
                return None

    def parse_raw_candles_to_df(
        self,
        candles: list[list[Any]],
    ) -> tuple[pd.DataFrame, int]:
        """Convert Groww raw candles [[ts, open, high, low, close, volume], ...] to validated DataFrame.
        
        Returns:
            (validated_df, invalid_count)
        """
        if not candles:
            return pd.DataFrame(columns=EXPECTED_COLUMNS), 0

        try:
            df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
        except ValueError:
            # Handle possible extra/fewer columns safely
            df = pd.DataFrame(
                [c[:6] for c in candles if len(c) >= 6],
                columns=["ts", "open", "high", "low", "close", "volume"]
            )

        if df.empty:
            return pd.DataFrame(columns=EXPECTED_COLUMNS), 0

        total_input = len(df)

        # Normalize timestamps to midnight IST with timestamp[ms, tz=Asia/Kolkata]
        dt_series = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(IST_TZ).dt.floor("D")
        df["datetime"] = dt_series.astype("datetime64[ms, Asia/Kolkata]")
        df["date"] = dt_series.dt.date

        # Numeric conversions
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Validation rules:
        # 1. Non-null OHLCV
        # 2. Positive prices (> 0)
        # 3. Valid volume (>= 0)
        # 4. High >= Low, High >= Open, High >= Close, Low <= Open, Low <= Close
        valid_mask = (
            df["datetime"].notna()
            & df["open"].notna()
            & df["high"].notna()
            & df["low"].notna()
            & df["close"].notna()
            & (df["open"] > 0)
            & (df["high"] > 0)
            & (df["low"] > 0)
            & (df["close"] > 0)
            & (df["volume"].isna() | (df["volume"] >= 0))
            & (df["high"] >= df["low"])
            & (df["high"] >= df["open"] * 0.9999)
            & (df["high"] >= df["close"] * 0.9999)
            & (df["low"] <= df["open"] * 1.0001)
            & (df["low"] <= df["close"] * 1.0001)
        )

        valid_df = df[valid_mask].copy()
        invalid_count = total_input - len(valid_df)

        if valid_df.empty:
            return pd.DataFrame(columns=EXPECTED_COLUMNS), invalid_count

        # Ensure volume is int64 and prices are float64
        valid_df["volume"] = valid_df["volume"].fillna(0).round().astype("int64")
        for col in ["open", "high", "low", "close"]:
            valid_df[col] = valid_df[col].astype("float64")

        # Deduplicate incoming candles by date (keep last)
        valid_df = valid_df.drop_duplicates(subset=["date"], keep="last")
        valid_df = valid_df.sort_values("datetime", ascending=True).reset_index(drop=True)

        return valid_df[EXPECTED_COLUMNS].copy(), invalid_count

    def merge_and_save(
        self,
        symbol: str,
        new_candles_df: pd.DataFrame,
    ) -> tuple[int, int, int]:
        """Merge new candles into {symbol}_Day.parquet atomically with full safety guarantees.
        
        Returns:
            (inserted_count, updated_count, total_records)
        """
        if new_candles_df is None or new_candles_df.empty:
            return 0, 0, 0

        sym = symbol.strip().upper()
        lock = self.get_symbol_lock(sym)
        primary_path = self.get_file_path(sym, self.config.data_dir)

        with lock:
            existing_df = pd.DataFrame(columns=EXPECTED_COLUMNS)
            existing_dates = set()

            if primary_path.exists():
                try:
                    existing_df = pd.read_parquet(primary_path)
                    if not existing_df.empty:
                        # Normalize existing datetime to avoid mismatch
                        dt_s = pd.to_datetime(existing_df["datetime"]).dt.tz_convert(IST_TZ).dt.floor("D")
                        existing_df["datetime"] = dt_s.astype("datetime64[ms, Asia/Kolkata]")
                        existing_dates = set(dt_s.dt.date.tolist())
                except Exception as e:
                    logger.error(f"Error reading existing file {primary_path}: {e}")

            # Identify new incoming dates vs existing dates to update
            new_dates = set(pd.to_datetime(new_candles_df["datetime"]).dt.tz_convert(IST_TZ).dt.date.tolist())
            updated_dates = existing_dates & new_dates
            inserted_dates = new_dates - existing_dates

            inserted_count = len(inserted_dates)
            updated_count = len(updated_dates)

            # Combine existing and new records
            if not existing_df.empty:
                combined = pd.concat([existing_df, new_candles_df], ignore_index=True)
            else:
                combined = new_candles_df.copy()

            # Date column for exact unique-key deduplication
            combined["_temp_date"] = pd.to_datetime(combined["datetime"]).dt.tz_convert(IST_TZ).dt.date
            # Keep last to allow new/revised Groww values to update older values
            combined = combined.drop_duplicates(subset=["_temp_date"], keep="last")
            combined = combined.sort_values("datetime", ascending=True).reset_index(drop=True)
            combined = combined.drop(columns=["_temp_date"])

            # Safety check: Row count must not decrease
            if not existing_df.empty and len(combined) < len(existing_df):
                raise ValueError(
                    f"Safety violation: merged row count ({len(combined)}) is smaller than existing ({len(existing_df)})!"
                )

            # Ensure numeric precision and integer volume (handles index files where volume may be float/null)
            for col in ["open", "high", "low", "close"]:
                combined[col] = combined[col].astype("float64")
            combined["volume"] = combined["volume"].fillna(0).round().astype("int64")

            # Convert to PyArrow Table with exact schema
            table = pa.Table.from_pandas(combined[EXPECTED_COLUMNS], schema=PARQUET_SCHEMA, preserve_index=False)

            # Atomic write to primary data directory
            self._atomic_write(primary_path, table)

            # Atomic write to secondary directory if configured and directory exists
            if self.config.secondary_data_dir and self.config.secondary_data_dir.exists():
                sec_path = self.get_file_path(sym, self.config.secondary_data_dir)
                self._atomic_write(sec_path, table)

            return inserted_count, updated_count, len(combined)

    def overwrite_atomic(self, symbol: str, df: pd.DataFrame) -> int:
        """Atomically overwrite {symbol}_Day.parquet with a full historical DataFrame (e.g. after split/merge).
        
        Returns:
            total_records_written
        """
        if df is None or df.empty:
            return 0

        sym = symbol.strip().upper()
        lock = self.get_symbol_lock(sym)
        primary_path = self.get_file_path(sym, self.config.data_dir)

        with lock:
            clean_df = df.copy()
            # Normalize datetime
            dt_s = pd.to_datetime(clean_df["datetime"]).dt.tz_convert(IST_TZ).dt.floor("D")
            clean_df["datetime"] = dt_s.astype("datetime64[ms, Asia/Kolkata]")
            clean_df["_temp_date"] = dt_s.dt.date
            clean_df = clean_df.drop_duplicates(subset=["_temp_date"], keep="last")
            clean_df = clean_df.sort_values("datetime", ascending=True).reset_index(drop=True)
            clean_df = clean_df.drop(columns=["_temp_date"], errors="ignore")

            for col in ["open", "high", "low", "close"]:
                clean_df[col] = clean_df[col].astype("float64")
            clean_df["volume"] = clean_df["volume"].fillna(0).round().astype("int64")

            table = pa.Table.from_pandas(clean_df[EXPECTED_COLUMNS], schema=PARQUET_SCHEMA, preserve_index=False)
            self._atomic_write(primary_path, table)

            if self.config.secondary_data_dir and self.config.secondary_data_dir.exists():
                sec_path = self.get_file_path(sym, self.config.secondary_data_dir)
                self._atomic_write(sec_path, table)

            return len(clean_df)

    def _atomic_write(self, target_path: Path, table: pa.Table):
        """Write Table to temporary file and atomically rename/replace."""
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_name = f"{target_path.name}.tmp_{uuid.uuid4().hex}"
        tmp_path = target_path.parent / tmp_name

        try:
            pq.write_table(table, tmp_path, compression="snappy")
            # os.replace is atomic on both POSIX and Windows (Python 3.3+)
            os.replace(tmp_path, target_path)
        except Exception as e:
            if tmp_path.exists():
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            logger.error(f"Failed atomic write to {target_path}: {e}")
            raise
