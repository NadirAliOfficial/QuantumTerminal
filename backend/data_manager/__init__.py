"""
Data Management Module (DMM)
============================

Unified data access layer for the Quantum Terminal quant platform.
Parquet-cached, gap-filling, source-agnostic data delivery.

Quick start (singleton — shares one cache across all scripts):
    from data_manager import dm
    df = dm.get_bars("EURUSD", "D1", n_bars=1500)

Or instantiate with custom settings:
    from data_manager import DataManager
    dm = DataManager(library_root="D:/MyData", auto_init_mt5=True)
    df = dm.get_bars("XAUUSD", "M15", date_from=..., date_to=...)
"""

from .data_manager import DataManager
from .data_store import DataStore
from .data_catalog import DataCatalog

import logging

# Configure DMM logging — INFO by default, callers can override
logging.getLogger("dmm").setLevel(logging.INFO)

# ── Lazy singleton ───────────────────────────────────────────────────────────
# Created on first access.  Uses default library_root (../data_library/).
# The forecaster's MT5 init is expected to happen BEFORE first dm.get_bars().

_dm_instance = None


def get_dm(**kwargs) -> DataManager:
    """Get or create the module-level DataManager singleton."""
    global _dm_instance
    if _dm_instance is None:
        _dm_instance = DataManager(**kwargs)
    return _dm_instance


# Convenience alias — `from data_manager import dm` gives you the singleton
class _LazyDM:
    """Proxy that creates the real DataManager on first attribute access."""
    def __getattr__(self, name):
        return getattr(get_dm(), name)

dm = _LazyDM()

__all__ = ["DataManager", "DataStore", "DataCatalog", "dm", "get_dm"]
