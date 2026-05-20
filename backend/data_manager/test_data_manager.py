"""
Data Management Module — Integration Test Suite

Validates the full stack (DataStore → DataCatalog → DataManager) using
synthetic data. No MT5 connection required.

Run from project root:
    python test_data_manager.py
"""

import sys
import shutil
import traceback
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np

# Ensure data_manager is importable
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_manager.data_store import DataStore, ValidationResult
from data_manager.data_catalog import DataCatalog, CoverageResult
from data_manager.data_manager import DataManager

# ── Test infrastructure ──────────────────────────────────────────────────────
TEST_DIR = Path(__file__).resolve().parent / "_test_library"
PASS = 0
FAIL = 0


def setup():
    """Clean test directory."""
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)
    TEST_DIR.mkdir(parents=True)


def teardown():
    """Remove test directory."""
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)


def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} — {detail}")


def make_ohlcv(start: datetime, n_bars: int, freq_hours: float = 24.0) -> pd.DataFrame:
    """Generate synthetic OHLCV data."""
    dates = [start + timedelta(hours=freq_hours * i) for i in range(n_bars)]
    dates = [d.replace(tzinfo=timezone.utc) for d in dates]
    rng = np.random.default_rng(42)
    close = 100.0 + np.cumsum(rng.normal(0, 1, n_bars))
    df = pd.DataFrame({
        "open":        close + rng.normal(0, 0.5, n_bars),
        "high":        close + np.abs(rng.normal(0, 1, n_bars)),
        "low":         close - np.abs(rng.normal(0, 1, n_bars)),
        "close":       close,
        "tick_volume":  rng.integers(100, 10000, n_bars),
        "spread":       rng.integers(1, 20, n_bars),
        "real_volume":  np.zeros(n_bars, dtype=int),
    }, index=pd.DatetimeIndex(dates, name="time"))
    return df


# ═══════════════════════════════════════════════════════════════════════════════
#  TEST 1 — DataStore basics
# ═══════════════════════════════════════════════════════════════════════════════
def test_datastore():
    print("\n─── TEST 1: DataStore (Parquet read/write) ───")
    store = DataStore(TEST_DIR / "store_test")

    # Save
    df = make_ohlcv(datetime(2024, 1, 1), 500)
    store.save("EURUSD", "D1", df)
    check("Save creates file", store.exists("EURUSD", "D1"))

    # Load full
    loaded = store.load("EURUSD", "D1")
    check("Load returns correct rows", loaded is not None and len(loaded) == 500,
          f"got {len(loaded) if loaded is not None else 'None'}")

    # Load with date filter
    mid = datetime(2024, 6, 1, tzinfo=timezone.utc)
    filtered = store.load("EURUSD", "D1", date_from=mid)
    check("Date filter works", filtered is not None and filtered.index.min() >= mid,
          f"min={filtered.index.min() if filtered is not None else 'None'}")

    # Append + dedup
    overlap_start = df.index[-50]
    new_data = make_ohlcv(overlap_start.replace(tzinfo=None), 100, freq_hours=24.0)
    store.append("EURUSD", "D1", new_data)
    appended = store.load("EURUSD", "D1")
    check("Append deduplicates overlap", appended is not None and len(appended) < 600,
          f"expected <600, got {len(appended) if appended is not None else 'None'}")
    check("Append is sorted", appended.index.is_monotonic_increasing if appended is not None else False)

    # Validate
    result = store.validate("EURUSD", "D1")
    check("Validation passes", result.valid, f"issues: {result.issues}")

    # Checksum
    cs = store.compute_checksum("EURUSD", "D1")
    check("Checksum computed", cs is not None and len(cs) == 32)

    # Non-existent
    check("Non-existent returns None", store.load("FAKE", "D1") is None)

    # Column filter
    cols_only = store.load("EURUSD", "D1", columns=["close"])
    check("Column filter works", cols_only is not None and list(cols_only.columns) == ["close"])


# ═══════════════════════════════════════════════════════════════════════════════
#  TEST 2 — DataCatalog
# ═══════════════════════════════════════════════════════════════════════════════
def test_datacatalog():
    print("\n─── TEST 2: DataCatalog (SQLite metadata) ───")
    db_path = TEST_DIR / "catalog_test" / "catalog.db"
    cat = DataCatalog(db_path)

    # Register
    d_from = datetime(2024, 1, 1, tzinfo=timezone.utc)
    d_to   = datetime(2024, 6, 1, tzinfo=timezone.utc)
    cat.register_dataset("EURUSD", "D1", "ohlcv", "mt5", d_from, d_to, 120,
                         "EURUSD/D1/ohlcv.parquet")
    summary = cat.get_summary()
    check("Register creates record", len(summary) == 1)
    check("Symbol stored correctly", summary[0]["symbol"] == "EURUSD")

    # Coverage — full hit
    cov = cat.check_coverage("EURUSD", "D1", d_from, d_to)
    check("Full coverage detected", cov.covered and len(cov.gaps) == 0,
          f"gaps={cov.gaps}")

    # Coverage — trailing gap
    future = datetime(2024, 9, 1, tzinfo=timezone.utc)
    cov2 = cat.check_coverage("EURUSD", "D1", d_from, future)
    check("Trailing gap detected", cov2.needs_fetch and len(cov2.gaps) == 1,
          f"gaps={cov2.gaps}")

    # Coverage — leading gap
    earlier = datetime(2023, 6, 1, tzinfo=timezone.utc)
    cov3 = cat.check_coverage("EURUSD", "D1", earlier, d_to)
    check("Leading gap detected", cov3.needs_fetch and len(cov3.gaps) == 1)

    # Coverage — both gaps
    cov4 = cat.check_coverage("EURUSD", "D1", earlier, future)
    check("Both gaps detected", len(cov4.gaps) == 2, f"gaps={len(cov4.gaps)}")

    # Coverage — unknown symbol
    cov5 = cat.check_coverage("FAKE", "D1", d_from, d_to)
    check("Unknown symbol = needs fetch", cov5.needs_fetch)

    # Fetch log
    cat.log_fetch("EURUSD", "D1", "mt5", d_from, d_to, 120, 350, "success")
    history = cat.get_fetch_history("EURUSD")
    check("Fetch log recorded", len(history) == 1)

    # Upsert (update existing record)
    d_to_new = datetime(2024, 9, 1, tzinfo=timezone.utc)
    cat.register_dataset("EURUSD", "D1", "ohlcv", "mt5", d_from, d_to_new, 200,
                         "EURUSD/D1/ohlcv.parquet")
    summary2 = cat.get_summary()
    check("Upsert updates (not duplicates)", len(summary2) == 1 and summary2[0]["row_count"] == 200)

    cat.close()


# ═══════════════════════════════════════════════════════════════════════════════
#  TEST 3 — DataManager (full orchestration, mock fetcher)
# ═══════════════════════════════════════════════════════════════════════════════
def test_data_manager():
    print("\n─── TEST 3: DataManager (orchestration with mock fetcher) ───")

    dm = DataManager(library_root=TEST_DIR / "dm_test", cache_enabled=True)

    # Inject a mock fetcher that returns synthetic data
    class MockFetcher:
        name = "mock"
        call_count = 0
        def fetch_bars(self, symbol, timeframe, date_from, date_to):
            self.call_count += 1
            # +1 to ensure we cover date_to inclusively
            n_bars = max(1, (date_to - date_from).days + 1)
            return make_ohlcv(date_from.replace(tzinfo=None), n_bars)
        def fetch_bars_n(self, symbol, timeframe, n_bars):
            self.call_count += 1
            return make_ohlcv(datetime(2024, 1, 1), n_bars)
        def is_available(self):
            return True

    mock = MockFetcher()
    dm._fetcher = mock

    # First call — should hit fetcher
    d_from = datetime(2024, 1, 1, tzinfo=timezone.utc)
    d_to   = datetime(2024, 6, 1, tzinfo=timezone.utc)
    df1 = dm.get_bars("EURUSD", "D1", date_from=d_from, date_to=d_to)
    check("First call returns data", df1 is not None and len(df1) > 0)
    first_calls = mock.call_count

    # Second identical call — cache covers bulk of range; may do small trailing fill
    mock.call_count = 0
    df2 = dm.get_bars("EURUSD", "D1", date_from=d_from, date_to=d_to)
    check("Second call: at most 1 trailing gap fill", mock.call_count <= 1,
          f"fetcher called {mock.call_count} times")
    check("Cached data matches or grows", df2 is not None and len(df2) >= len(df1),
          f"df2={len(df2) if df2 is not None else 0}, df1={len(df1)}")

    # Third call — now cache fully covers range, zero fetches expected
    mock.call_count = 0
    df2b = dm.get_bars("EURUSD", "D1", date_from=d_from, date_to=d_to)
    check("Third call uses cache (zero fetches)", mock.call_count == 0,
          f"fetcher called {mock.call_count} times")

    # Extend range — should only fetch the gap
    mock.call_count = 0
    d_to_ext = datetime(2024, 9, 1, tzinfo=timezone.utc)
    df3 = dm.get_bars("EURUSD", "D1", date_from=d_from, date_to=d_to_ext)
    check("Extended range triggers gap fetch", mock.call_count == 1,
          f"expected 1 fetch, got {mock.call_count}")
    check("Extended result is longer", df3 is not None and len(df3) > len(df1),
          f"df3={len(df3) if df3 is not None else 0}, df1={len(df1)}")

    # n_bars mode
    mock.call_count = 0
    df4 = dm.get_bars("XAUUSD", "M15", n_bars=500)
    check("n_bars mode works", df4 is not None and len(df4) == 500)

    # Catalog summary
    summary = dm.get_catalog_summary()
    check("Catalog has 2 datasets", len(summary) == 2,
          f"got {len(summary)}")

    # Forecaster-compatible output
    df_fc = dm.get_bars_as_forecaster("EURUSD", "D1", date_from=d_from, date_to=d_to)
    check("Forecaster compat: has 'Close'", df_fc is not None and "Close" in df_fc.columns)
    check("Forecaster compat: tz-naive index",
          df_fc is not None and df_fc.index.tz is None)

    # Purge
    dm.purge("XAUUSD", "M15")
    check("Purge removes dataset", len(dm.get_catalog_summary()) == 1)

    # Fetch history
    history = dm.get_fetch_history()
    check("Fetch history recorded", len(history) > 0)


# ═══════════════════════════════════════════════════════════════════════════════
#  TEST 4 — Path portability
# ═══════════════════════════════════════════════════════════════════════════════
def test_portability():
    print("\n─── TEST 4: Path portability ───")
    lib_root = TEST_DIR / "portable_test"
    dm = DataManager(library_root=lib_root)

    # All paths should be under lib_root
    check("Library root exists", lib_root.exists())
    check("Catalog in library root", (lib_root / "catalog.db").exists())
    check("Temp dir in library root", (lib_root / "_temp").exists())

    # No absolute paths in catalog file_path entries
    class MockFetcher:
        name = "mock"
        def fetch_bars(self, s, tf, df, dt):
            return make_ohlcv(df.replace(tzinfo=None), 10)
        def fetch_bars_n(self, s, tf, n):
            return make_ohlcv(datetime(2024, 1, 1), n)
        def is_available(self):
            return True

    dm._fetcher = MockFetcher()
    dm.get_bars("TEST", "D1", n_bars=10)

    summary = dm.get_catalog_summary()
    if summary:
        fp = summary[0].get("file_path", "")
        check("Catalog stores relative path", not fp.startswith("/") and not fp.startswith("C:"),
              f"got: {fp}")
    else:
        check("Catalog stores relative path", False, "no summary entries")


# ═══════════════════════════════════════════════════════════════════════════════
#  RUN ALL
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  DATA MANAGEMENT MODULE — INTEGRATION TESTS")
    print("=" * 60)

    setup()
    try:
        test_datastore()
        test_datacatalog()
        test_data_manager()
        test_portability()
    except Exception as e:
        print(f"\n  ✗ UNEXPECTED ERROR: {e}")
        traceback.print_exc()
        FAIL += 1
    finally:
        teardown()

    print("\n" + "=" * 60)
    print(f"  RESULTS: {PASS} passed, {FAIL} failed")
    print("=" * 60)
    sys.exit(1 if FAIL > 0 else 0)
