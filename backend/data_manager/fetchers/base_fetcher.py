"""
BaseFetcher — abstract interface for data source adapters.

Every fetcher (MT5, DXFeed, futures bridge, CSV import) implements this.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
import pandas as pd


class BaseFetcher(ABC):
    """
    Abstract data fetcher interface.

    Implementations must return DataFrames with:
      - DatetimeIndex named 'time' (UTC)
      - Lowercase column names: open, high, low, close, tick_volume, spread, real_volume
    """

    name: str = "base"  # Override in subclass

    @abstractmethod
    def fetch_bars(
        self,
        symbol: str,
        timeframe: str,
        date_from: datetime,
        date_to: datetime,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV bars for the given symbol/timeframe/date range.
        Returns None on failure.
        """
        ...

    @abstractmethod
    def fetch_bars_n(
        self,
        symbol: str,
        timeframe: str,
        n_bars: int,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch the most recent N bars.
        Returns None on failure.
        """
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this data source is currently reachable."""
        ...
