"""
DataCatalog — SQLite-backed metadata catalog for the data library.

Tracks what data exists, date coverage, row counts, gaps, and fetch history.
All paths stored as relative strings for portability.
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, List

import logging
log = logging.getLogger("dmm.catalog")


@dataclass
class CoverageResult:
    """What the catalog knows about a (symbol, timeframe, data_type) tuple."""
    symbol: str
    timeframe: str
    data_type: str
    covered: bool = False
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    row_count: int = 0
    file_path: Optional[str] = None
    # Gaps that need filling relative to a request
    gaps: List[dict] = field(default_factory=list)

    @property
    def needs_fetch(self) -> bool:
        return not self.covered or len(self.gaps) > 0


class DataCatalog:
    """
    SQLite metadata catalog.

    Parameters
    ----------
    db_path : Path
        Absolute path to catalog.db (created if missing).
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._create_tables()

    # ── schema ───────────────────────────────────────────────────────────────

    def _create_tables(self):
        c = self._conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS data_registry (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT NOT NULL,
                timeframe   TEXT NOT NULL,
                data_type   TEXT NOT NULL DEFAULT 'ohlcv',
                source      TEXT NOT NULL DEFAULT 'mt5',
                date_from   TEXT,
                date_to     TEXT,
                row_count   INTEGER DEFAULT 0,
                file_path   TEXT,
                last_updated TEXT,
                checksum    TEXT,
                gaps        TEXT DEFAULT '[]',
                UNIQUE(symbol, timeframe, data_type)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS fetch_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol        TEXT NOT NULL,
                timeframe     TEXT NOT NULL,
                source        TEXT NOT NULL,
                fetch_from    TEXT,
                fetch_to      TEXT,
                rows_returned INTEGER DEFAULT 0,
                duration_ms   INTEGER DEFAULT 0,
                status        TEXT DEFAULT 'success',
                error_msg     TEXT,
                timestamp     TEXT
            )
        """)
        self._conn.commit()

    # ── coverage API ─────────────────────────────────────────────────────────

    def check_coverage(
        self,
        symbol: str,
        timeframe: str,
        date_from: datetime,
        date_to: datetime,
        data_type: str = "ohlcv",
    ) -> CoverageResult:
        """
        Check what coverage exists for a request and identify gaps to fill.
        """
        result = CoverageResult(
            symbol=symbol.upper(),
            timeframe=timeframe.upper(),
            data_type=data_type,
        )

        row = self._conn.execute(
            "SELECT * FROM data_registry WHERE symbol=? AND timeframe=? AND data_type=?",
            (symbol.upper(), timeframe.upper(), data_type),
        ).fetchone()

        if row is None:
            # Nothing cached — entire range is a gap
            result.gaps.append({
                "from": date_from.isoformat(),
                "to": date_to.isoformat(),
            })
            return result

        result.covered = True
        result.date_from = datetime.fromisoformat(row["date_from"]) if row["date_from"] else None
        result.date_to = datetime.fromisoformat(row["date_to"]) if row["date_to"] else None
        result.row_count = row["row_count"] or 0
        result.file_path = row["file_path"]

        # Identify leading gap (request starts before cached range)
        if result.date_from and date_from < result.date_from:
            result.gaps.append({
                "from": date_from.isoformat(),
                "to": result.date_from.isoformat(),
            })

        # Identify trailing gap (request ends after cached range)
        if result.date_to and date_to > result.date_to:
            result.gaps.append({
                "from": result.date_to.isoformat(),
                "to": date_to.isoformat(),
            })

        return result

    def check_latest(self, symbol: str, timeframe: str, data_type: str = "ohlcv") -> CoverageResult:
        """Quick check: what's the latest bar we have?"""
        result = CoverageResult(symbol=symbol.upper(), timeframe=timeframe.upper(), data_type=data_type)
        row = self._conn.execute(
            "SELECT * FROM data_registry WHERE symbol=? AND timeframe=? AND data_type=?",
            (symbol.upper(), timeframe.upper(), data_type),
        ).fetchone()

        if row:
            result.covered = True
            result.date_from = datetime.fromisoformat(row["date_from"]) if row["date_from"] else None
            result.date_to = datetime.fromisoformat(row["date_to"]) if row["date_to"] else None
            result.row_count = row["row_count"] or 0
            result.file_path = row["file_path"]

        return result

    # ── registration & updates ───────────────────────────────────────────────

    def register_dataset(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
        date_from: datetime,
        date_to: datetime,
        row_count: int,
        file_path: str,
        checksum: Optional[str] = None,
    ):
        """Insert or update a dataset record in the registry."""
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute("""
            INSERT INTO data_registry (symbol, timeframe, data_type, source,
                                        date_from, date_to, row_count, file_path,
                                        last_updated, checksum)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, timeframe, data_type) DO UPDATE SET
                source=excluded.source,
                date_from=excluded.date_from,
                date_to=excluded.date_to,
                row_count=excluded.row_count,
                file_path=excluded.file_path,
                last_updated=excluded.last_updated,
                checksum=excluded.checksum
        """, (
            symbol.upper(), timeframe.upper(), data_type, source,
            date_from.isoformat(), date_to.isoformat(), row_count,
            file_path, now, checksum,
        ))
        self._conn.commit()

    def log_fetch(
        self,
        symbol: str,
        timeframe: str,
        source: str,
        fetch_from: datetime,
        fetch_to: datetime,
        rows_returned: int,
        duration_ms: int,
        status: str = "success",
        error_msg: Optional[str] = None,
    ):
        """Record a fetch attempt in the audit log."""
        self._conn.execute("""
            INSERT INTO fetch_log (symbol, timeframe, source, fetch_from, fetch_to,
                                    rows_returned, duration_ms, status, error_msg, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol.upper(), timeframe.upper(), source,
            fetch_from.isoformat(), fetch_to.isoformat(),
            rows_returned, duration_ms, status, error_msg,
            datetime.now(timezone.utc).isoformat(),
        ))
        self._conn.commit()

    # ── summary / status ─────────────────────────────────────────────────────

    def get_summary(self) -> list:
        """Return all registry entries as a list of dicts."""
        rows = self._conn.execute("SELECT * FROM data_registry ORDER BY symbol, timeframe").fetchall()
        return [dict(r) for r in rows]

    def get_fetch_history(self, symbol: Optional[str] = None, limit: int = 50) -> list:
        if symbol:
            rows = self._conn.execute(
                "SELECT * FROM fetch_log WHERE symbol=? ORDER BY timestamp DESC LIMIT ?",
                (symbol.upper(), limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM fetch_log ORDER BY timestamp DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def purge(self, symbol: str, timeframe: str, data_type: str = "ohlcv"):
        """Remove a dataset from the registry (caller must also delete the Parquet file)."""
        self._conn.execute(
            "DELETE FROM data_registry WHERE symbol=? AND timeframe=? AND data_type=?",
            (symbol.upper(), timeframe.upper(), data_type),
        )
        self._conn.commit()

    def close(self):
        self._conn.close()
