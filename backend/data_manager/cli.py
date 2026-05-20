"""
Data Manager CLI — command-line interface for data library management.

Usage:
    python -m data_manager.cli --status
    python -m data_manager.cli --prefetch --symbols EURUSD XAUUSD --tf D1 M15
    python -m data_manager.cli --history --symbol EURUSD --limit 20
    python -m data_manager.cli --purge --symbol EURUSD --tf D1
    python -m data_manager.cli --validate
"""

import argparse
import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure parent dir is on path for relative imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data_manager import DataManager


def cmd_status(dm: DataManager):
    """Print catalog summary."""
    summary = dm.get_catalog_summary()
    if not summary:
        print("\n  [DATA LIBRARY] Empty — no datasets cached yet.\n")
        return
    print(f"\n  [DATA LIBRARY] {dm.library_root}")
    print(f"  {'Symbol':<10} {'TF':<6} {'Type':<12} {'From':<12} {'To':<12} {'Rows':>8} {'Source':<6}")
    print("  " + "─" * 72)
    for r in summary:
        d_from = r.get("date_from", "")[:10] if r.get("date_from") else "—"
        d_to   = r.get("date_to", "")[:10] if r.get("date_to") else "—"
        print(f"  {r['symbol']:<10} {r['timeframe']:<6} {r['data_type']:<12} "
              f"{d_from:<12} {d_to:<12} {r.get('row_count',0):>8} {r.get('source','?'):<6}")
    print()


def cmd_prefetch(dm: DataManager, symbols: list, timeframes: list, days: int):
    """Batch warm-up cache for given symbols/timeframes."""
    date_from = datetime.now(timezone.utc) - timedelta(days=days)
    date_to = datetime.now(timezone.utc)
    print(f"\n  Prefetching {len(symbols)} symbols × {len(timeframes)} timeframes "
          f"({days} days back)...\n")
    dm.prefetch(symbols, timeframes, date_from, date_to)
    print("\n  Done.\n")


def cmd_history(dm: DataManager, symbol: str, limit: int):
    """Show fetch history."""
    history = dm.get_fetch_history(symbol, limit)
    if not history:
        print(f"\n  No fetch history{' for '+symbol if symbol else ''}.\n")
        return
    print(f"\n  {'Time':<22} {'Symbol':<10} {'TF':<6} {'Rows':>8} {'ms':>8} {'Status':<10}")
    print("  " + "─" * 68)
    for r in history:
        ts = r.get("timestamp", "")[:19]
        print(f"  {ts:<22} {r['symbol']:<10} {r['timeframe']:<6} "
              f"{r.get('rows_returned',0):>8} {r.get('duration_ms',0):>8} {r.get('status','?'):<10}")
    print()


def cmd_validate(dm: DataManager):
    """Validate all cached Parquet files."""
    summary = dm.get_catalog_summary()
    if not summary:
        print("\n  Nothing to validate — library is empty.\n")
        return
    print(f"\n  Validating {len(summary)} datasets...")
    all_ok = True
    for r in summary:
        result = dm.store.validate(r["symbol"], r["timeframe"], r.get("data_type", "ohlcv"))
        tag = "✓" if result.valid else "✗"
        print(f"  {tag} {r['symbol']:<10} {r['timeframe']:<6} rows={result.row_count}", end="")
        if not result.valid:
            all_ok = False
            print(f"  ISSUES: {'; '.join(result.issues)}")
        else:
            print()
    print(f"\n  {'All datasets valid.' if all_ok else 'Some datasets have issues — see above.'}\n")


def cmd_purge(dm: DataManager, symbol: str, timeframe: str, data_type: str):
    """Remove a specific dataset from cache."""
    dm.purge(symbol, timeframe, data_type)
    print(f"\n  Purged {symbol} {timeframe} {data_type}\n")


def main():
    parser = argparse.ArgumentParser(description="Data Manager CLI")
    parser.add_argument("--status", action="store_true", help="Show catalog summary")
    parser.add_argument("--prefetch", action="store_true", help="Batch prefetch data")
    parser.add_argument("--history", action="store_true", help="Show fetch history")
    parser.add_argument("--validate", action="store_true", help="Validate all cached files")
    parser.add_argument("--purge", action="store_true", help="Purge a dataset")

    parser.add_argument("--symbols", nargs="+", default=[])
    parser.add_argument("--symbol", type=str, default=None)
    parser.add_argument("--tf", nargs="+", default=["D1"])
    parser.add_argument("--days", type=int, default=1500, help="Days back for prefetch")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--data-type", type=str, default="ohlcv")
    parser.add_argument("--mt5-path", type=str, default=None)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    dm = DataManager(mt5_terminal_path=args.mt5_path, auto_init_mt5=True)

    if args.status:
        cmd_status(dm)
    elif args.prefetch:
        if not args.symbols:
            print("  --prefetch requires --symbols")
            sys.exit(1)
        cmd_prefetch(dm, args.symbols, args.tf, args.days)
    elif args.history:
        cmd_history(dm, args.symbol, args.limit)
    elif args.validate:
        cmd_validate(dm)
    elif args.purge:
        if not args.symbol or not args.tf:
            print("  --purge requires --symbol and --tf")
            sys.exit(1)
        cmd_purge(dm, args.symbol, args.tf[0], args.data_type)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
