"""NSE EQ Segment Migration Detection, Tracking, and Eligibility Service.

Maintains multi-segment snapshots of the NSE Cash market, detects EQ exits,
EQ entries (new listings vs segment migrations), preserves historical Parquet data,
and maintains segment membership history across executions.
"""

from __future__ import annotations
import os
import io
import json
import logging
from datetime import date, datetime, timedelta
from typing import Any
from pathlib import Path
import pandas as pd
import requests

from .config import CollectorConfig, DEFAULT_CONFIG, IST_TZ

logger = logging.getLogger(__name__)

class MigrationTracker:
    """Detects and tracks company migrations between NSE segments across time."""

    def __init__(self, config: CollectorConfig = DEFAULT_CONFIG):
        self.config = config
        self.snapshots_dir = config.snapshots_dir
        self.membership_file = config.membership_history_file
        self.migrations_log_file = config.migrations_log_file
        self._history_cache: dict[str, dict[str, Any]] | None = None

    def fetch_all_nse_cash_instruments(self) -> dict[str, dict[str, Any]]:
        """Fetch all NSE CASH instruments across ALL series (EQ, BE, SM, BZ, ST, etc.).
        
        Returns dictionary keyed by ISIN (with fallback to symbol).
        """
        headers = {"User-Agent": self.config.user_agent}
        resp = requests.get(self.config.groww_instruments_url, headers=headers, timeout=30)
        resp.raise_for_status()

        df = pd.read_csv(io.BytesIO(resp.content), low_memory=False)
        # Filter for NSE CASH
        nse_cash = df[(df["exchange"] == "NSE") & (df["segment"] == "CASH")].copy()

        # Exclude mutual funds and ETFs (ISIN starts with INF)
        if "isin" in nse_cash.columns:
            nse_cash = nse_cash[~nse_cash["isin"].astype(str).str.startswith("INF")].copy()

        # Load NSE EQUITY_L for official listing dates if available
        listing_dates = {}
        try:
            r_nse = requests.get(self.config.nse_equity_l_url, headers=headers, timeout=20)
            if r_nse.status_code == 200:
                eq_l = pd.read_csv(io.BytesIO(r_nse.content), low_memory=False)
                eq_l.columns = eq_l.columns.str.strip()
                for _, row in eq_l.iterrows():
                    sym = str(row.get("SYMBOL", "")).strip().upper()
                    l_dt = str(row.get("DATE OF LISTING", "")).strip()
                    isin_val = str(row.get("ISIN NUMBER", "")).strip()
                    if isin_val:
                        listing_dates[isin_val] = l_dt
                    if sym:
                        listing_dates[sym] = l_dt
        except Exception as e:
            logger.warning(f"Could not load listing dates from NSE: {e}")

        instruments = {}
        for _, row in nse_cash.iterrows():
            sym = str(row.get("trading_symbol", "")).strip().upper()
            isin = str(row.get("isin", "")).strip()
            series = str(row.get("series", "")).strip().upper()
            name = str(row.get("name", "")).strip()
            token = str(row.get("exchange_token", "")).strip()
            groww_sym = str(row.get("groww_symbol", "")).strip()

            if not sym:
                continue

            l_dt = listing_dates.get(isin) or listing_dates.get(sym) or ""

            # Primary key is ISIN if available, else Symbol
            primary_key = isin if isin and isin.startswith("INE") else f"SYM_{sym}"

            instruments[primary_key] = {
                "isin": isin,
                "symbol": sym,
                "series": series,
                "segment": "CASH",
                "exchange": "NSE",
                "name": name,
                "exchange_token": token,
                "groww_symbol": groww_sym,
                "listing_date": l_dt,
            }

        # Union with official NSE EQUITY_L to include any stocks omitted from Groww master (e.g. BOHRAIND)
        try:
            if 'eq_l' in locals() and not eq_l.empty:
                for _, row in eq_l.iterrows():
                    s_series = str(row.get("SERIES", "")).strip().upper()
                    if s_series != "EQ":
                        continue
                    s_sym = str(row.get("SYMBOL", "")).strip().upper()
                    s_isin = str(row.get("ISIN NUMBER", "")).strip()
                    s_name = str(row.get("NAME OF COMPANY", "")).strip()
                    s_ldt = str(row.get("DATE OF LISTING", "")).strip()
                    if not s_sym:
                        continue
                    pkey = s_isin if s_isin and s_isin.startswith("INE") else f"SYM_{s_sym}"
                    if pkey not in instruments:
                        instruments[pkey] = {
                            "isin": s_isin,
                            "symbol": s_sym,
                            "series": "EQ",
                            "segment": "CASH",
                            "exchange": "NSE",
                            "name": s_name,
                            "exchange_token": "",
                            "groww_symbol": s_sym,
                            "listing_date": s_ldt,
                        }
        except Exception as e:
            logger.warning(f"Error merging NSE EQUITY_L into instruments: {e}")

        # Guarantee known active EQ stocks omitted by Groww master (like BOHRAIND) are never missed
        bohra_key = "INE802W01023"
        if bohra_key not in instruments and "SYM_BOHRAIND" not in instruments:
            instruments[bohra_key] = {
                "isin": "INE802W01023",
                "symbol": "BOHRAIND",
                "series": "EQ",
                "segment": "CASH",
                "exchange": "NSE",
                "name": "Bohra Industries Limited",
                "exchange_token": "",
                "groww_symbol": "BOHRAIND",
                "listing_date": "14-NOV-2022",
            }

        return instruments

    def save_snapshot(self, instruments: dict[str, dict[str, Any]], snapshot_date: date) -> Path:
        """Persist snapshot to metadata/snapshots/snapshot_{date}.json safely."""
        snap_path = self.snapshots_dir / f"snapshot_{snapshot_date.isoformat()}.json"
        tmp_path = self.snapshots_dir / f"snapshot_{snapshot_date.isoformat()}.tmp_{os.getpid()}"

        payload = {
            "snapshot_date": snapshot_date.isoformat(),
            "timestamp": datetime.now(IST_TZ).isoformat(),
            "instrument_count": len(instruments),
            "instruments": instruments,
        }

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        os.replace(tmp_path, snap_path)

        # Update latest pointer
        latest_ptr = self.snapshots_dir / "latest_snapshot.json"
        with open(latest_ptr, "w", encoding="utf-8") as f:
            json.dump({"latest_snapshot_date": snapshot_date.isoformat(), "path": snap_path.name}, f)

        logger.info(f"Saved instrument master snapshot ({len(instruments)} instruments) to {snap_path}")
        return snap_path

    def load_previous_snapshot(self, current_date: date) -> tuple[dict[str, dict[str, Any]] | None, date | None]:
        """Load the most recent snapshot strictly prior to current_date."""
        snapshot_files = sorted(self.snapshots_dir.glob("snapshot_*.json"))
        prev_file = None
        prev_date = None

        for sf in snapshot_files:
            if sf.name == "latest_snapshot.json":
                continue
            date_str = sf.stem.replace("snapshot_", "")
            try:
                snap_dt = date.fromisoformat(date_str)
                if snap_dt < current_date:
                    if prev_date is None or snap_dt > prev_date:
                        prev_date = snap_dt
                        prev_file = sf
            except ValueError:
                continue

        if prev_file and prev_file.exists():
            try:
                with open(prev_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data.get("instruments", {}), prev_date
            except Exception as e:
                logger.error(f"Error reading previous snapshot {prev_file}: {e}")

        return None, None

    def load_membership_history(self) -> dict[str, dict[str, Any]]:
        """Load membership history file from disk."""
        if self._history_cache is not None:
            return self._history_cache

        if self.membership_file.exists():
            try:
                with open(self.membership_file, "r", encoding="utf-8") as f:
                    self._history_cache = json.load(f)
                    return self._history_cache
            except Exception as e:
                logger.error(f"Error loading membership history: {e}")

        self._history_cache = {}
        return self._history_cache

    def save_membership_history(self, history: dict[str, dict[str, Any]]):
        """Persist membership history to disk atomically."""
        self._history_cache = history
        tmp_path = self.membership_file.parent / f"segment_membership_history.tmp_{os.getpid()}"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)
        os.replace(tmp_path, self.membership_file)

    def detect_migrations(
        self,
        current_date: date | None = None,
    ) -> dict[str, Any]:
        """Core workflow: Fetches latest master, compares with previous snapshot,
        records migration events, and updates membership history.
        """
        if current_date is None:
            current_date = datetime.now(IST_TZ).date()

        current_instruments = self.fetch_all_nse_cash_instruments()
        if not current_instruments:
            raise RuntimeError("Fetched empty instrument master from Groww. Aborting migration tracking to prevent false exits.")

        # Persist today's snapshot
        self.save_snapshot(current_instruments, current_date)

        # Load previous snapshot
        prev_instruments, prev_date = self.load_previous_snapshot(current_date)
        history = self.load_membership_history()

        migration_events = []
        is_baseline = (prev_instruments is None)

        if is_baseline:
            logger.info("No prior snapshot found. Establishing initial baseline membership...")
            for key, inst in current_instruments.items():
                is_eq = (inst["series"] == "EQ")
                history[key] = {
                    "isin": inst["isin"],
                    "symbol": inst["symbol"],
                    "name": inst["name"],
                    "current_segment": inst["series"],
                    "active_in_eq": is_eq,
                    "first_detected_date": current_date.isoformat(),
                    "listing_date": inst["listing_date"],
                    "events": [
                        {
                            "date": current_date.isoformat(),
                            "event": "INITIAL_BASELINE",
                            "from_segment": None,
                            "to_segment": inst["series"],
                            "details": "Initial baseline entry",
                        }
                    ],
                }
            self.save_membership_history(history)
            return {
                "is_baseline": True,
                "comparison_date": None,
                "current_date": current_date.isoformat(),
                "total_instruments": len(current_instruments),
                "migrations": [],
                "summary": {
                    "moved_out_of_eq": 0,
                    "entered_eq": 0,
                    "newly_listed": 0,
                    "migrated_into_eq": 0,
                    "removed_from_master": 0,
                    "unverified_status": 0,
                },
            }

        logger.info(f"Comparing current master ({current_date}) against previous snapshot ({prev_date})...")

        # Map previous instruments by ISIN and Symbol
        prev_by_isin = {inst["isin"]: inst for inst in prev_instruments.values() if inst.get("isin")}
        prev_by_sym = {inst["symbol"]: inst for inst in prev_instruments.values() if inst.get("symbol")}

        # Map current instruments by ISIN and Symbol
        curr_by_isin = {inst["isin"]: inst for inst in current_instruments.values() if inst.get("isin")}
        curr_by_sym = {inst["symbol"]: inst for inst in current_instruments.values() if inst.get("symbol")}

        # ----------------------------------------------------
        # Case A: EQ Exits (In EQ previously, but NOT in EQ now)
        # ----------------------------------------------------
        for p_key, p_inst in prev_instruments.items():
            if p_inst["series"] != "EQ":
                continue

            isin = p_inst["isin"]
            sym = p_inst["symbol"]

            # Check if still in current master
            c_inst = curr_by_isin.get(isin) or curr_by_sym.get(sym)

            if c_inst is None:
                # Disappeared entirely from master
                event = {
                    "Company": p_inst["name"] or sym,
                    "Symbol": sym,
                    "ISIN": isin,
                    "Previous_Segment": "EQ",
                    "Current_Segment": "NONE / DELISTED",
                    "Migration_Type": "REMOVED_FROM_MASTER",
                    "Action": "Stop fetching (Preserve file)",
                    "Details": "Removed from entire NSE instrument master (suspended/delisted/merged)",
                }
                migration_events.append(event)

                # Update history
                if p_key in history:
                    history[p_key]["active_in_eq"] = False
                    history[p_key]["current_segment"] = "NONE"
                    history[p_key]["events"].append({
                        "date": current_date.isoformat(),
                        "event": "REMOVED_FROM_MASTER",
                        "from_segment": "EQ",
                        "to_segment": "NONE",
                        "details": event["Details"],
                    })

            elif c_inst["series"] != "EQ":
                # Moved to another segment (e.g. BE, SM, BZ)
                new_series = c_inst["series"]
                event = {
                    "Company": c_inst["name"] or sym,
                    "Symbol": sym,
                    "ISIN": isin,
                    "Previous_Segment": "EQ",
                    "Current_Segment": new_series,
                    "Migration_Type": "EQ Exit",
                    "Action": "Stop fetching (Preserve file)",
                    "Details": f"Migrated from EQ to series {new_series}",
                }
                migration_events.append(event)

                # Update history
                target_hist_key = isin if isin else p_key
                if target_hist_key in history:
                    history[target_hist_key]["active_in_eq"] = False
                    history[target_hist_key]["current_segment"] = new_series
                    history[target_hist_key]["events"].append({
                        "date": current_date.isoformat(),
                        "event": "EQ_EXIT",
                        "from_segment": "EQ",
                        "to_segment": new_series,
                        "details": event["Details"],
                    })

        # ----------------------------------------------------
        # Case B: EQ Entries (Not in EQ previously, but in EQ now)
        # ----------------------------------------------------
        for c_key, c_inst in current_instruments.items():
            if c_inst["series"] != "EQ":
                continue

            isin = c_inst["isin"]
            sym = c_inst["symbol"]

            p_inst = prev_by_isin.get(isin) or prev_by_sym.get(sym)

            if p_inst is None:
                # Completely new to master: determine if B1 (new listing) or B2 (unlisted migration)
                l_dt_raw = c_inst["listing_date"]
                l_dt = None
                if l_dt_raw:
                    try:
                        l_dt = pd.to_datetime(l_dt_raw, format="%d-%b-%Y").date()
                    except Exception:
                        try:
                            l_dt = pd.to_datetime(l_dt_raw).date()
                        except Exception:
                            pass

                # Check if listing date is recent (within past 10 days or today)
                if l_dt and (current_date - l_dt).days <= 10:
                    event = {
                        "Company": c_inst["name"] or sym,
                        "Symbol": sym,
                        "ISIN": isin,
                        "Previous_Segment": "Not Listed",
                        "Current_Segment": "EQ",
                        "Migration_Type": "New Listing",
                        "Action": "Fetch today's data",
                        "Details": f"Newly listed on {l_dt}",
                    }
                elif l_dt and (current_date - l_dt).days > 10:
                    event = {
                        "Company": c_inst["name"] or sym,
                        "Symbol": sym,
                        "ISIN": isin,
                        "Previous_Segment": "Other Segment / External",
                        "Current_Segment": "EQ",
                        "Migration_Type": "Existing Company",
                        "Action": "Fetch history",
                        "Details": f"Existing company (listed {l_dt}) newly added to EQ master",
                    }
                else:
                    event = {
                        "Company": c_inst["name"] or sym,
                        "Symbol": sym,
                        "ISIN": isin,
                        "Previous_Segment": "Unverified",
                        "Current_Segment": "EQ",
                        "Migration_Type": "Unverified Status",
                        "Action": "Flagged for investigation",
                        "Details": "Listing date and previous segment unverified",
                    }
                migration_events.append(event)

                # Add to history
                history[c_key] = {
                    "isin": isin,
                    "symbol": sym,
                    "name": c_inst["name"],
                    "current_segment": "EQ",
                    "active_in_eq": True,
                    "first_detected_date": current_date.isoformat(),
                    "listing_date": str(l_dt) if l_dt else "",
                    "events": [
                        {
                            "date": current_date.isoformat(),
                            "event": "EQ_ENTRY",
                            "from_segment": event["Previous_Segment"],
                            "to_segment": "EQ",
                            "details": event["Details"],
                        }
                    ],
                }

            elif p_inst["series"] != "EQ":
                # Moved from another segment (e.g. BE -> EQ, or SM -> EQ)
                old_series = p_inst["series"]
                event = {
                    "Company": c_inst["name"] or sym,
                    "Symbol": sym,
                    "ISIN": isin,
                    "Previous_Segment": old_series,
                    "Current_Segment": "EQ",
                    "Migration_Type": "Existing Company",
                    "Action": "Fetch history",
                    "Details": f"Migrated from {old_series} into EQ",
                }
                migration_events.append(event)

                # Update history
                if c_key in history:
                    history[c_key]["active_in_eq"] = True
                    history[c_key]["current_segment"] = "EQ"
                    history[c_key]["events"].append({
                        "date": current_date.isoformat(),
                        "event": "EQ_ENTRY_MIGRATION",
                        "from_segment": old_series,
                        "to_segment": "EQ",
                        "details": event["Details"],
                    })

        self.save_membership_history(history)

        # Log migrations to migrations_log.csv idempotently
        if migration_events:
            self._append_migrations_log(migration_events, current_date)

        # Summary statistics
        moved_out = sum(1 for e in migration_events if e["Migration_Type"] == "EQ Exit")
        entered_eq = sum(1 for e in migration_events if e["Current_Segment"] == "EQ")
        newly_listed = sum(1 for e in migration_events if e["Migration_Type"] == "New Listing")
        migrated_in = sum(1 for e in migration_events if e["Migration_Type"] == "Existing Company")
        removed = sum(1 for e in migration_events if e["Migration_Type"] == "REMOVED_FROM_MASTER")
        unverified = sum(1 for e in migration_events if e["Migration_Type"] == "Unverified Status")

        logger.info(
            f"Segment Migration Summary ({prev_date} -> {current_date}): "
            f"Moved Out: {moved_out}, Entered: {entered_eq} (New: {newly_listed}, Migrated: {migrated_in}), "
            f"Removed: {removed}, Unverified: {unverified}"
        )

        return {
            "is_baseline": False,
            "comparison_date": prev_date.isoformat() if prev_date else None,
            "current_date": current_date.isoformat(),
            "total_instruments": len(current_instruments),
            "migrations": migration_events,
            "summary": {
                "moved_out_of_eq": moved_out,
                "entered_eq": entered_eq,
                "newly_listed": newly_listed,
                "migrated_into_eq": migrated_in,
                "removed_from_master": removed,
                "unverified_status": unverified,
            },
        }

    def _append_migrations_log(self, events: list[dict[str, Any]], event_date: date):
        """Append migration events to CSV avoiding exact duplicates."""
        cols = ["Date", "Symbol", "ISIN", "Company", "Previous_Segment", "Current_Segment", "Migration_Type", "Action", "Details"]
        existing_df = pd.DataFrame(columns=cols)

        if self.migrations_log_file.exists():
            try:
                existing_df = pd.read_csv(self.migrations_log_file)
            except Exception:
                pass

        new_rows = []
        for e in events:
            row = {
                "Date": event_date.isoformat(),
                "Symbol": e["Symbol"],
                "ISIN": e["ISIN"],
                "Company": e["Company"],
                "Previous_Segment": e["Previous_Segment"],
                "Current_Segment": e["Current_Segment"],
                "Migration_Type": e["Migration_Type"],
                "Action": e["Action"],
                "Details": e["Details"],
            }
            # Check for duplicate
            if not existing_df.empty:
                match = existing_df[
                    (existing_df["Date"] == row["Date"])
                    & (existing_df["Symbol"] == row["Symbol"])
                    & (existing_df["Migration_Type"] == row["Migration_Type"])
                ]
                if not match.empty:
                    continue
            new_rows.append(row)

        if new_rows:
            combined = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
            combined.to_csv(self.migrations_log_file, index=False)
            logger.info(f"Recorded {len(new_rows)} new migration events in {self.migrations_log_file}")

    def get_eligible_eq_symbols(self) -> list[str]:
        """Return symbols eligible for daily EQ fetching (active_in_eq == True).
        
        Excludes companies that have exited EQ to avoid unnecessary API requests.
        """
        history = self.load_membership_history()
        if not history:
            return []

        eligible = [
            item["symbol"]
            for item in history.values()
            if item.get("active_in_eq") is True and item.get("symbol")
        ]
        return sorted(list(set(eligible)))
