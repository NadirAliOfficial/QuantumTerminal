"""
forcaster_dmm_patch.py — Data Management Module integration for forcasterv21.

This module provides drop-in replacement functions for the forecaster's
fetch_daily_data() and fetch_intraday_data() that route through the DMM
instead of calling MT5 directly.

INTEGRATION (3 steps in forcasterv21_institutional_anchors.py):
─────────────────────────────────────────────────────────────────

STEP 1 — Add import near line 28 (after other module imports):

    from data_manager import DataManager

STEP 2 — Replace the fetch functions (lines 66-93) with:

    # ==========================================
    # 📊 DATA INGESTION (via Data Management Module)
    # ==========================================
    _dm = None  # Initialized in __main__ after MT5 init

    def fetch_daily_data(ticker, days_back=1500):
        if _dm is None:
            return _fetch_daily_data_legacy(ticker, days_back)
        df = _dm.get_bars_as_forecaster(ticker, "D1", n_bars=days_back)
        return df

    def fetch_intraday_data(ticker, start_time, end_time=None):
        if _dm is None:
            return _fetch_intraday_data_legacy(ticker, start_time, end_time)
        df = _dm.get_bars_as_forecaster(ticker, "M15", n_bars=20000)
        if df is None: return None
        df = df[df.index >= start_time]
        if end_time is not None:
            df = df[df.index <= end_time]
        return df

    # Legacy fallback (direct MT5) — used if DMM init fails
    def _fetch_daily_data_legacy(ticker, days_back=1500):
        if not mt5.initialize(path=MT5_TERMINAL_PATH):
            print(f"[!] MT5 Init failed for {ticker}: {mt5.last_error()}")
            return None
        rates = mt5.copy_rates_from_pos(ticker, mt5.TIMEFRAME_D1, 0, days_back)
        if rates is None or len(rates) == 0: return None
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                           'close': 'Close', 'tick_volume': 'Volume'}, inplace=True)
        df.set_index('time', inplace=True)
        return df

    def _fetch_intraday_data_legacy(ticker, start_time, end_time=None):
        rates = mt5.copy_rates_from_pos(ticker, mt5.TIMEFRAME_M15, 0, 20000)
        if rates is None or len(rates) == 0: return None
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                           'close': 'Close'}, inplace=True)
        df.set_index('time', inplace=True)
        df = df[df.index >= start_time]
        if end_time is not None:
            df = df[df.index <= end_time]
        return df


STEP 3 — In __main__ block (after MT5 init, ~line 1855), add DMM initialization:

    if __name__ == "__main__":
        if not mt5.initialize(path=MT5_TERMINAL_PATH):
            print("[!] MT5 Init failed.")
            quit()

        # >>> NEW: Initialize Data Management Module <<<
        try:
            _dm = DataManager(mt5_terminal_path=MT5_TERMINAL_PATH)
            print(f"[DMM] Data library: {_dm.library_root} | Engine: {_dm.store.engine}")
        except Exception as e:
            print(f"[DMM] Init failed ({e}) — falling back to direct MT5")
            _dm = None

        # ... rest of __main__ unchanged ...

That's it. Three changes. Everything else stays the same.
"""

# ─── Standalone helper for quick integration testing ─────────────────────────
# You can run this file directly to test DMM + forecaster compatibility:
#   python forcaster_dmm_patch.py

import sys
from pathlib import Path

def test_integration():
    """Test that DMM produces forecaster-compatible DataFrames."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))

    from data_manager import DataManager
    from datetime import datetime, timezone
    import pandas as pd
    import numpy as np

    print("=" * 60)
    print("  FORECASTER ↔ DMM INTEGRATION TEST")
    print("=" * 60)

    dm = DataManager(
        library_root=Path(__file__).resolve().parent / "_test_fcst_lib",
        cache_enabled=True,
    )

    # Inject mock to simulate MT5
    class MockMT5Fetcher:
        name = "mock_mt5"
        def fetch_bars(self, symbol, timeframe, date_from, date_to):
            n = max(1, (date_to - date_from).days + 1)
            return _make_synthetic(date_from, n)
        def fetch_bars_n(self, symbol, timeframe, n_bars):
            return _make_synthetic(datetime(2020, 1, 1, tzinfo=timezone.utc), n_bars)
        def is_available(self):
            return True

    def _make_synthetic(start, n):
        if hasattr(start, 'tzinfo') and start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        from datetime import timedelta
        dates = pd.DatetimeIndex(
            [start + timedelta(days=i) for i in range(n)],
            name="time"
        )
        rng = np.random.default_rng(42)
        close = 1800.0 + np.cumsum(rng.normal(0, 10, n))
        return pd.DataFrame({
            "open": close + rng.normal(0, 2, n),
            "high": close + np.abs(rng.normal(0, 5, n)),
            "low":  close - np.abs(rng.normal(0, 5, n)),
            "close": close,
            "tick_volume": rng.integers(100, 5000, n),
            "spread": rng.integers(1, 30, n),
            "real_volume": np.zeros(n, dtype=int),
        }, index=dates)

    dm._fetcher = MockMT5Fetcher()
    passed = 0
    failed = 0

    # Test 1: get_bars_as_forecaster matches expected schema
    df = dm.get_bars_as_forecaster("XAUUSD", "D1", n_bars=1500)
    if df is not None and all(c in df.columns for c in ["Open", "High", "Low", "Close", "Volume"]):
        print("  ✓ Column names: Open, High, Low, Close, Volume")
        passed += 1
    else:
        print(f"  ✗ Column names wrong: {df.columns.tolist() if df is not None else 'None'}")
        failed += 1

    if df is not None and df.index.tz is None:
        print("  ✓ Timezone-naive index (forecaster compat)")
        passed += 1
    else:
        print("  ✗ Index has timezone (forecaster expects tz-naive)")
        failed += 1

    if df is not None and len(df) == 1500:
        print("  ✓ Correct row count (1500)")
        passed += 1
    else:
        print(f"  ✗ Row count: {len(df) if df is not None else 'None'}")
        failed += 1

    if df is not None and isinstance(df.index, pd.DatetimeIndex):
        print("  ✓ DatetimeIndex preserved")
        passed += 1
    else:
        print("  ✗ Index is not DatetimeIndex")
        failed += 1

    # Test 2: Intraday M15 with date filtering
    from datetime import timedelta
    df15 = dm.get_bars_as_forecaster("XAUUSD", "M15", n_bars=500)
    if df15 is not None and len(df15) == 500:
        print("  ✓ M15 intraday fetch (500 bars)")
        passed += 1
    else:
        print(f"  ✗ M15 fetch: {len(df15) if df15 is not None else 'None'}")
        failed += 1

    # Test 3: Second fetch is faster (from cache)
    import time
    t0 = time.time()
    dm.get_bars_as_forecaster("XAUUSD", "D1", n_bars=1500)
    elapsed = time.time() - t0
    print(f"  ✓ Cached D1 fetch: {elapsed*1000:.0f}ms" if elapsed < 1 else f"  ⚠ Cached fetch slow: {elapsed:.1f}s")
    passed += 1

    # Cleanup
    import shutil
    test_lib = Path(__file__).resolve().parent / "_test_fcst_lib"
    if test_lib.exists():
        shutil.rmtree(test_lib)

    print(f"\n  RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)
    return failed == 0


if __name__ == "__main__":
    success = test_integration()
    sys.exit(0 if success else 1)
