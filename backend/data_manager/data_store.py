"""
DataStore — Format-agnostic data storage engine with append, dedup, and validation.

Storage format priority:
  1. Parquet (pyarrow) — production default, industry-standard columnar format
  2. Pickle — fallback when pyarrow is unavailable (testing / lightweight setups)

All file operations use relative paths anchored to the data_library root.
Designed for portability: no hardcoded absolute paths anywhere.
"""

import pandas as pd
import numpy as np
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List

import logging
log = logging.getLogger("dmm.store")

# ─── Detect best available engine ────────────────────────────────────────────
_ENGINE = "pickle"  # safe default
try:
    import pyarrow
    _ENGINE = "parquet"
except ImportError:
    try:
        import fastparquet
        _ENGINE = "parquet"
    except ImportError:
        pass  # stay on pickle

_EXT = ".parquet" if _ENGINE == "parquet" else ".pkl"

# ─── Expected OHLCV schema from MT5 ─────────────────────────────────────────
OHLCV_COLUMNS = ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]
OHLCV_DTYPES  = {
    "open": "float64", "high": "float64", "low": "float64", "close": "float64",
    "tick_volume": "int64", "spread": "int64", "real_volume": "int64",
}


class ValidationResult:
    """Result of a data file validation check."""
    def __init__(self, valid: bool, issues: List[str] = None, row_count: int = 0):
        self.valid = valid
        self.issues = issues or []
        self.row_count = row_count

    def __repr__(self):
        tag = "OK" if self.valid else f"FAIL({len(self.issues)})"
        return f"<ValidationResult {tag} rows={self.row_count}>"


class DataStore:
    """
    Data store with auto-detected format (Parquet or Pickle).

    Parameters
    ----------
    library_root : Path
        Absolute path to the data_library/ directory (created if missing).
    """

    def __init__(self, library_root: Path):
        self.root = Path(library_root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._temp = self.root / "_temp"
        self._temp.mkdir(exist_ok=True)
        self.engine = _ENGINE
        self.ext = _EXT
        log.info(f"DataStore engine: {self.engine} (ext: {self.ext})")

    # ── path helpers ─────────────────────────────────────────────────────────

    def _data_path(self, symbol: str, timeframe: str, data_type: str = "ohlcv") -> Path:
        return self.root / symbol.upper() / timeframe.upper() / f"{data_type}{self.ext}"

    def _meta_path(self, symbol: str, timeframe: str, data_type: str = "ohlcv") -> Path:
        return self.root / symbol.upper() / timeframe.upper() / f"{data_type}.meta.json"

    def _temp_path(self, symbol: str, timeframe: str, data_type: str = "ohlcv") -> Path:
        return self._temp / f"{symbol}_{timeframe}_{data_type}{self.ext}"

    # Backward-compat alias used by DataManager
    def _parquet_path(self, symbol: str, timeframe: str, data_type: str = "ohlcv") -> Path:
        return self._data_path(symbol, timeframe, data_type)

    # ── low-level IO ─────────────────────────────────────────────────────────

    def _write_df(self, df: pd.DataFrame, path: Path):
        if self.engine == "parquet":
            df.to_parquet(path, engine="pyarrow", compression="snappy", index=True)
        else:
            df.to_pickle(path)

    def _read_df(self, path: Path, columns: Optional[List[str]] = None) -> pd.DataFrame:
        if self.engine == "parquet":
            return pd.read_parquet(path, columns=columns)
        else:
            df = pd.read_pickle(path)
            if columns is not None:
                avail = [c for c in columns if c in df.columns]
                df = df[avail]
            return df

    # ── core API ─────────────────────────────────────────────────────────────

    def exists(self, symbol: str, timeframe: str, data_type: str = "ohlcv") -> bool:
        return self._data_path(symbol, timeframe, data_type).exists()

    def load(
        self,
        symbol: str,
        timeframe: str,
        data_type: str = "ohlcv",
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        columns: Optional[List[str]] = None,
    ) -> Optional[pd.DataFrame]:
        """Load data with optional date and column filtering. Returns None if missing."""
        fpath = self._data_path(symbol, timeframe, data_type)
        if not fpath.exists():
            return None

        try:
            df = self._read_df(fpath, columns)
        except Exception as e:
            log.error(f"Failed to read {fpath}: {e}")
            return None

        # Ensure DatetimeIndex
        if "time" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df.set_index("time", inplace=True)
        elif isinstance(df.index, pd.DatetimeIndex):
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")

        if date_from is not None:
            if hasattr(date_from, 'tzinfo') and date_from.tzinfo is None:
                date_from = date_from.replace(tzinfo=timezone.utc)
            df = df[df.index >= date_from]
        if date_to is not None:
            if hasattr(date_to, 'tzinfo') and date_to.tzinfo is None:
                date_to = date_to.replace(tzinfo=timezone.utc)
            df = df[df.index <= date_to]

        return df

    def save(self, symbol: str, timeframe: str, df: pd.DataFrame,
             data_type: str = "ohlcv") -> Path:
        """Full write. Writes to temp first, then atomic move to final path."""
        df = self._normalize(df)
        fpath = self._data_path(symbol, timeframe, data_type)
        fpath.parent.mkdir(parents=True, exist_ok=True)

        tmp = self._temp_path(symbol, timeframe, data_type)
        self._write_df(df, tmp)
        tmp.replace(fpath)

        self._write_meta(symbol, timeframe, data_type, df)
        log.info(f"Saved {len(df)} rows -> {fpath.relative_to(self.root)} [{self.engine}]")
        return fpath

    def append(self, symbol: str, timeframe: str, new_data: pd.DataFrame,
               data_type: str = "ohlcv") -> Path:
        """Append new rows with dedup at the seam. Creates file if missing."""
        new_data = self._normalize(new_data)
        fpath = self._data_path(symbol, timeframe, data_type)

        if not fpath.exists():
            return self.save(symbol, timeframe, new_data, data_type)

        existing = self._read_df(fpath)
        if "time" in existing.columns:
            existing["time"] = pd.to_datetime(existing["time"], utc=True)
            existing.set_index("time", inplace=True)
        if isinstance(existing.index, pd.DatetimeIndex) and existing.index.tz is None:
            existing.index = existing.index.tz_localize("UTC")

        combined = pd.concat([existing, new_data])
        combined = combined[~combined.index.duplicated(keep="last")]
        combined.sort_index(inplace=True)

        return self.save(symbol, timeframe, combined, data_type)

    def validate(self, symbol: str, timeframe: str, data_type: str = "ohlcv") -> ValidationResult:
        """Zero-trust validation: readability, schema, NaN, monotonic timestamps."""
        issues = []
        fpath = self._data_path(symbol, timeframe, data_type)

        if not fpath.exists():
            return ValidationResult(False, ["File does not exist"], 0)

        try:
            df = self._read_df(fpath)
        except Exception as e:
            return ValidationResult(False, [f"Cannot read file: {e}"], 0)

        row_count = len(df)
        if row_count == 0:
            issues.append("File is empty")

        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])
            if not df["time"].is_monotonic_increasing:
                issues.append("Timestamps are not monotonic")
        elif isinstance(df.index, pd.DatetimeIndex):
            if not df.index.is_monotonic_increasing:
                issues.append("Timestamps are not monotonic")
        else:
            issues.append("No 'time' column or DatetimeIndex found")

        if data_type == "ohlcv":
            for col in ["open", "high", "low", "close"]:
                if col in df.columns and df[col].isna().any():
                    n = df[col].isna().sum()
                    issues.append(f"NaN in '{col}': {n} rows")

        return ValidationResult(valid=len(issues) == 0, issues=issues, row_count=row_count)

    def compute_checksum(self, symbol: str, timeframe: str, data_type: str = "ohlcv") -> Optional[str]:
        fpath = self._data_path(symbol, timeframe, data_type)
        if not fpath.exists():
            return None
        h = hashlib.md5()
        with open(fpath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    # ── internals ────────────────────────────────────────────────────────────

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure consistent format: DatetimeIndex named 'time', UTC, sorted."""
        df = df.copy()

        if "time" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df.set_index("time", inplace=True)

        if isinstance(df.index, pd.DatetimeIndex):
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            elif str(df.index.tz) != "UTC":
                df.index = df.index.tz_convert("UTC")

        df.index.name = "time"
        df.sort_index(inplace=True)
        df.columns = [c.lower() for c in df.columns]

        return df

    def _write_meta(self, symbol: str, timeframe: str, data_type: str, df: pd.DataFrame):
        meta_path = self._meta_path(symbol, timeframe, data_type)
        meta = {
            "symbol": symbol.upper(),
            "timeframe": timeframe.upper(),
            "data_type": data_type,
            "storage_engine": self.engine,
            "row_count": len(df),
            "date_from": str(df.index.min()),
            "date_to": str(df.index.max()),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
        }
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

    def cleanup_temp(self):
        """Remove any leftover temp files from interrupted writes."""
        for f in self._temp.iterdir():
            if f.is_file():
                f.unlink()
                log.info(f"Cleaned temp file: {f.name}")
