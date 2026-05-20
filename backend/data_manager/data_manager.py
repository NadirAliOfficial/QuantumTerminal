"""
DataManager — Main entry point for the Data Management Module.

Callers use this class exclusively.  It coordinates:
  - DataCatalog  (knows what's cached)
  - DataStore    (reads/writes Parquet)
  - Fetchers     (fill gaps from live sources)

All paths are derived relative to a configurable library root.
Default root is  <project_dir>/data_library/  for full portability.

Usage
-----
    from data_manager import dm              # pre-wired singleton
    df = dm.get_bars("EURUSD", "D1", 1500)   # last 1500 daily bars
    df = dm.get_bars("XAUUSD", "M15",        # date range fetch
                     date_from=datetime(2024,1,1),
                     date_to=datetime(2025,1,1))
"""

import time
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Union

from .data_catalog import DataCatalog, CoverageResult
from .data_store import DataStore
from .fetchers.base_fetcher import BaseFetcher
from .fetchers.mt5_fetcher import MT5Fetcher, DEFAULT_MT5_PATH

import logging
log = logging.getLogger("dmm.manager")


class DataManager:
    """
    Unified data access layer.

    Parameters
    ----------
    library_root : Path or str or None
        Where Parquet files and catalog.db live.
        Default: <directory of this file>/../data_library/
    mt5_terminal_path : str or None
        Path to MT5 terminal64.exe. If None, uses DEFAULT_MT5_PATH
        from mt5_fetcher.py (single source of truth for the terminal path).
    auto_init_mt5 : bool
        If True, DataManager will call mt5.initialize() automatically.
        If False (default), the caller is responsible for MT5 init —
        this is the typical pattern since the forecaster already inits MT5.
    cache_enabled : bool
        If False, always fetch fresh from source (passthrough mode for testing).
    """

    def __init__(
        self,
        library_root: Union[str, Path, None] = None,
        mt5_terminal_path: Optional[str] = None,
        auto_init_mt5: bool = False,
        cache_enabled: bool = True,
    ):
        # Resolve library root relative to this file (portable)
        if library_root is None:
            library_root = Path(__file__).resolve().parent.parent / "data_library"
        self.library_root = Path(library_root).resolve()

        self.cache_enabled = cache_enabled
        self.store = DataStore(self.library_root)
        self.catalog = DataCatalog(self.library_root / "catalog.db")

        # Primary fetcher: MT5
        self._fetcher = MT5Fetcher(
            terminal_path=mt5_terminal_path,
            auto_init=auto_init_mt5,
        )

        # Clean up any interrupted writes from previous runs
        self.store.cleanup_temp()

        log.info(f"DataManager ready | library: {self.library_root} | cache: {self.cache_enabled}")

    # ═══════════════════════════════════════════════════════════════════════════
    #  PUBLIC API — what callers use
    # ═══════════════════════════════════════════════════════════════════════════

    def get_bars(
        self,
        symbol: str,
        timeframe: str,
        n_bars: Optional[int] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        data_type: str = "ohlcv",
    ) -> Optional[pd.DataFrame]:
        """
        Get OHLCV bars — the main workhorse method.

        Two calling modes:
          1. n_bars mode:     dm.get_bars("EURUSD", "D1", n_bars=1500)
          2. date range mode: dm.get_bars("EURUSD", "M15", date_from=..., date_to=...)

        Returns a DataFrame with DatetimeIndex('time') and lowercase columns,
        identical to what MT5 returns (drop-in compatible).
        """
        symbol = symbol.upper()
        timeframe = timeframe.upper()

        # ── Mode 1: last N bars ──────────────────────────────────────────────
        if n_bars is not None:
            return self._get_bars_n(symbol, timeframe, n_bars, data_type)

        # ── Mode 2: date range ───────────────────────────────────────────────
        if date_from is None:
            raise ValueError("Must provide either n_bars or date_from")

        if date_to is None:
            date_to = datetime.now(timezone.utc)

        # Ensure UTC
        if date_from.tzinfo is None:
            date_from = date_from.replace(tzinfo=timezone.utc)
        if date_to.tzinfo is None:
            date_to = date_to.replace(tzinfo=timezone.utc)

        # Clamp future dates to now
        now = datetime.now(timezone.utc)
        if date_to > now:
            date_to = now

        return self._get_bars_range(symbol, timeframe, date_from, date_to, data_type)

    def get_bars_as_forecaster(
        self,
        symbol: str,
        timeframe: str,
        n_bars: Optional[int] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Same as get_bars() but returns with capitalized column names
        (Open, High, Low, Close, Volume) and 'time' as the index —
        matching what the forecaster currently expects from fetch_daily_data()
        and fetch_intraday_data().
        """
        df = self.get_bars(symbol, timeframe, n_bars=n_bars,
                           date_from=date_from, date_to=date_to)
        if df is None:
            return None

        # Rename to match forecaster convention
        rename_map = {
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "tick_volume": "Volume",
        }
        df.rename(columns=rename_map, inplace=True)

        # Strip timezone for compat with existing code that uses tz-naive datetimes
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        return df

    def prefetch(
        self,
        symbols: List[str],
        timeframes: List[str],
        date_from: datetime,
        date_to: datetime,
        data_type: str = "ohlcv",
    ):
        """Batch warm-up: ensure all (symbol, tf) combos are cached."""
        total = len(symbols) * len(timeframes)
        done = 0
        for sym in symbols:
            for tf in timeframes:
                done += 1
                log.info(f"[{done}/{total}] Prefetching {sym} {tf}...")
                self.get_bars(sym, tf, date_from=date_from, date_to=date_to, data_type=data_type)

    def get_catalog_summary(self) -> list:
        """Return a summary of all cached datasets."""
        return self.catalog.get_summary()

    def get_fetch_history(self, symbol: Optional[str] = None, limit: int = 50) -> list:
        return self.catalog.get_fetch_history(symbol, limit)

    def purge(self, symbol: str, timeframe: str, data_type: str = "ohlcv"):
        """Remove cached data for a specific (symbol, tf, data_type) tuple."""
        fpath = self.store._parquet_path(symbol, timeframe, data_type)
        if fpath.exists():
            fpath.unlink()
        meta = self.store._meta_path(symbol, timeframe, data_type)
        if meta.exists():
            meta.unlink()
        self.catalog.purge(symbol, timeframe, data_type)
        log.info(f"Purged {symbol} {timeframe} {data_type}")

    # ═══════════════════════════════════════════════════════════════════════════
    #  INTERNAL — fetch orchestration
    # ═══════════════════════════════════════════════════════════════════════════

    def _get_bars_n(self, symbol: str, timeframe: str, n_bars: int,
                    data_type: str) -> Optional[pd.DataFrame]:
        """
        Get last N bars.  Strategy:
        1. If cache has enough rows and is recent enough, return from cache.
        2. Otherwise, fetch from source, cache, return.
        """
        if self.cache_enabled:
            coverage = self.catalog.check_latest(symbol, timeframe, data_type)
            if coverage.covered and coverage.row_count >= n_bars:
                # Check if cache is reasonably fresh (within the timeframe interval)
                # For D1, "fresh" means updated today; for M15, within the last hour
                df = self.store.load(symbol, timeframe, data_type)
                if df is not None and len(df) >= n_bars:
                    # Still fetch a small update to catch new bars
                    self._update_trailing(symbol, timeframe, data_type, coverage)
                    df = self.store.load(symbol, timeframe, data_type)
                    if df is not None:
                        return df.tail(n_bars)

        # Cache miss or disabled — fetch from source
        t0 = time.time()
        df = self._fetcher.fetch_bars_n(symbol, timeframe, n_bars)
        elapsed_ms = int((time.time() - t0) * 1000)

        if df is None or df.empty:
            self.catalog.log_fetch(symbol, timeframe, self._fetcher.name,
                                   datetime.now(timezone.utc), datetime.now(timezone.utc),
                                   0, elapsed_ms, "failed", "No data returned")
            return None

        # Cache the result
        if self.cache_enabled:
            self.store.append(symbol, timeframe, df, data_type)
            checksum = self.store.compute_checksum(symbol, timeframe, data_type)
            file_path = str(self.store._parquet_path(symbol, timeframe, data_type).relative_to(self.library_root))
            self.catalog.register_dataset(
                symbol, timeframe, data_type, self._fetcher.name,
                df.index.min(), df.index.max(), len(df),
                file_path, checksum,
            )

        self.catalog.log_fetch(symbol, timeframe, self._fetcher.name,
                               df.index.min(), df.index.max(),
                               len(df), elapsed_ms, "success")

        return df.tail(n_bars)

    def _get_bars_range(self, symbol: str, timeframe: str,
                        date_from: datetime, date_to: datetime,
                        data_type: str) -> Optional[pd.DataFrame]:
        """
        Get bars for a date range.  Strategy:
        1. Check catalog for coverage.
        2. Fetch only gaps (smart gap filling).
        3. Return full range from cache.
        """
        if not self.cache_enabled:
            return self._fetch_and_log(symbol, timeframe, date_from, date_to, data_type)

        coverage = self.catalog.check_coverage(symbol, timeframe, date_from, date_to, data_type)

        if coverage.needs_fetch:
            for gap in coverage.gaps:
                gap_from = datetime.fromisoformat(gap["from"])
                gap_to = datetime.fromisoformat(gap["to"])
                log.info(f"Filling gap: {symbol} {timeframe} {gap_from.date()}→{gap_to.date()}")
                fetched = self._fetch_and_log(symbol, timeframe, gap_from, gap_to, data_type)
                if fetched is not None and not fetched.empty:
                    self.store.append(symbol, timeframe, fetched, data_type)

            # Update catalog with new combined coverage
            full_df = self.store.load(symbol, timeframe, data_type)
            if full_df is not None and not full_df.empty:
                checksum = self.store.compute_checksum(symbol, timeframe, data_type)
                file_path = str(self.store._parquet_path(symbol, timeframe, data_type).relative_to(self.library_root))
                self.catalog.register_dataset(
                    symbol, timeframe, data_type, self._fetcher.name,
                    full_df.index.min(), full_df.index.max(), len(full_df),
                    file_path, checksum,
                )

        # Return requested slice from cache
        df = self.store.load(symbol, timeframe, data_type, date_from, date_to)
        return df

    def _update_trailing(self, symbol: str, timeframe: str, data_type: str,
                         coverage: CoverageResult):
        """Fetch only bars newer than what's already cached."""
        if coverage.date_to is None:
            return
        # Start from last cached bar (will be deduped)
        fetch_from = coverage.date_to - timedelta(hours=1)  # small overlap for safety
        fetch_to = datetime.now(timezone.utc)
        if fetch_to <= fetch_from:
            return

        fetched = self._fetch_and_log(symbol, timeframe, fetch_from, fetch_to, data_type)
        if fetched is not None and not fetched.empty:
            self.store.append(symbol, timeframe, fetched, data_type)
            # Re-read and update catalog
            full_df = self.store.load(symbol, timeframe, data_type)
            if full_df is not None and not full_df.empty:
                checksum = self.store.compute_checksum(symbol, timeframe, data_type)
                file_path = str(self.store._parquet_path(symbol, timeframe, data_type).relative_to(self.library_root))
                self.catalog.register_dataset(
                    symbol, timeframe, data_type, self._fetcher.name,
                    full_df.index.min(), full_df.index.max(), len(full_df),
                    file_path, checksum,
                )

    def _fetch_and_log(self, symbol: str, timeframe: str,
                       date_from: datetime, date_to: datetime,
                       data_type: str) -> Optional[pd.DataFrame]:
        """Fetch from source with timing and audit logging."""
        t0 = time.time()
        df = self._fetcher.fetch_bars(symbol, timeframe, date_from, date_to)
        elapsed_ms = int((time.time() - t0) * 1000)

        if df is None or df.empty:
            self.catalog.log_fetch(symbol, timeframe, self._fetcher.name,
                                   date_from, date_to, 0, elapsed_ms,
                                   "failed", "No data returned")
            return None

        self.catalog.log_fetch(symbol, timeframe, self._fetcher.name,
                               date_from, date_to, len(df), elapsed_ms, "success")
        log.info(f"Fetched {len(df)} rows {symbol} {timeframe} ({elapsed_ms}ms)")
        return df
