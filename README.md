# Groww Daily Candle Data Collection System

A high-performance, fault-tolerant, automated data collection and synchronization system that fetches daily historical OHLCV candles from Groww for every NSE EQ-listed company and maintains complete, verified Parquet files.

---

## Key Features

- **High-Concurrency Engine:** 300 concurrent workers (`ThreadPoolExecutor`) processing all ~2,404 NSE EQ companies in **under 2.5 minutes** (easily beating the 5-minute requirement).
- **Automatic Desktop Storage:** Detects the current user's Desktop across Windows (including OneDrive redirection), macOS, and Linux, storing all company files in `NSE Equity Companies` by default, with support for custom folder overrides.
- **Existing Data Verification (Case A & Case B):**
  - **Case A (File Exists):** Inspects schema, dates, duplicates, and completeness. Complete files are skipped (zero bandwidth wasted). Files with missing dates have only missing ranges healed. Corrupted rows are auto-repaired.
  - **Case B (File Missing):** Determines listing date, creates `{company_name}_Day.parquet`, and backfills full historical candles.
- **NSE EQ Segment Migration Tracking:** Maintains dated snapshots across all NSE Cash series (`EQ`, `BE`, `SM`, `BZ`, `ST`).
  - Detects **EQ Exits**: Halts daily EQ fetching while safely preserving historical files.
  - Detects **EQ Entries**: Distinguishes brand new IPO listings (fetches initial candle) from existing migrated companies (backfills complete history).
- **Scheduled Automation (Asia/Kolkata Time):**
  - `3:55 PM IST`: Instrument master sync, segment migration detection, and verification.
  - `4:00 PM IST`: Daily candle fetch for all active NSE EQ companies.
  - `6:00 AM IST`: Final candle synchronization for the previous completed session (post-4 PM settlement revisions).
  - `6:30 AM IST`: Continuous completeness audit and self-healing gap recovery.
- **Weekend Protection:** Strict **no-run policy on Saturday and Sunday**. The system automatically sleeps on weekends and wakes up Monday morning.
- **21-Day Rolling Retention:** Daily logs rotate at midnight IST and automatically purge log and report files older than **21 days**.
- **Parquet Data Safety:** Atomic file replacement (`.tmp` $\rightarrow$ `os.replace`) protected by per-company thread locks. Parquet files can never be corrupted or half-written.
- **Human-Readable Reports:** Generates plain-English execution reports with summary metrics, status tables, and unresolved halt logs.

---

## Directory Structure

```text
groww-data-collector/
├── src/
│   └── groww_collector/
│       ├── __init__.py
│       ├── audit.py               # Historical completeness audit & gap healing
│       ├── calendar_service.py     # Live NSE holidays & empirical trading calendar
│       ├── cli.py                  # Command-line interface & logging
│       ├── config.py               # Central configuration & defaults
│       ├── desktop_detector.py     # OS-aware Desktop detection (Windows/OneDrive/Linux/Mac)
│       ├── fetcher.py              # 300-worker concurrency engine & 4-stage immediate retries
│       ├── groww_client.py         # Thread-safe Groww API client & connection pooling
│       ├── instruments.py          # 3:55 PM instrument sync workflow
│       ├── migration_tracker.py    # Segment migration detection (EQ exits, entries, IPOs)
│       ├── parquet_store.py        # Atomic Parquet storage, locks & schema enforcement
│       ├── reports.py              # Human-readable report compiler & exporters
│       ├── scheduler.py            # Automated Asia/Kolkata daemon scheduler
│       └── verification.py         # Case A / Case B folder verification service
├── groww_collector_service.py      # Root service launcher
├── requirements.txt                # Python dependencies
└── README.md
```

---

## Quick Start

### Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/YUG-SAVALIYA/groww-data-collector.git
cd groww-data-collector
pip install -r requirements.txt
```

### Running the System

#### 1. Continuous 24/7 Daemon Mode (Recommended)
Runs in the background on the automated schedule (3:55 PM, 4:00 PM, 6:00 AM, 6:30 AM):
```bash
python groww_collector_service.py --daemon
```
*(On Windows, you can simply double-click `run_on_any_device.bat`)*

#### 2. Verify Existing Folder and Sync
Inspects your Desktop folder, validates data integrity, skips complete files, and heals missing ranges:
```bash
python groww_collector_service.py --task verify_existing
```

#### 3. Immediate Daily Candle Fetch (4:00 PM Task)
Fetches today's daily candle for all active NSE EQ companies:
```bash
python groww_collector_service.py --task daily_fetch
```

#### 4. Final Previous Session Correction (6:00 AM Task)
Updates existing candles with final settlement prices from the previous trading day:
```bash
python groww_collector_service.py --task final_sync
```

#### 5. Historical Completeness Audit
Audits all companies against the master trading calendar and self-heals gaps:
```bash
python groww_collector_service.py --task audit
```

#### 6. Benchmark Concurrency Speed
Tests raw throughput across the universe with 300 workers:
```bash
python groww_collector_service.py --task benchmark
```

---

## Configuration Options

| Option | Description | Default |
| :--- | :--- | :--- |
| `--daemon` | Run continuous scheduler in Asia/Kolkata timezone | `False` |
| `--workers` | Number of concurrent worker threads | `300` |
| `--attempts` | Maximum attempts per failed request (1 initial + 3 retries) | `4` |
| `--data-dir` | Custom storage directory override | `<Desktop>/NSE Equity Companies` |
| `--folder-name`| Custom folder name on Desktop | `NSE Equity Companies` |

---

## Data Schema Reference

Every `{company_name}_Day.parquet` file adheres to this exact schema:

| Column | Type | Description |
| :--- | :--- | :--- |
| `datetime` | `timestamp[ms, tz=Asia/Kolkata]` | Trading session midnight timestamp in IST |
| `open` | `double` | Session opening price |
| `high` | `double` | Session highest price |
| `low` | `double` | Session lowest price |
| `close` | `double` | Session closing / settlement price |
| `volume` | `int64` | Total session shares traded |

Compression: **Snappy**
