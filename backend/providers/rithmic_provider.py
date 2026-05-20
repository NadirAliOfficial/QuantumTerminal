"""
================================================================================
Quantum Terminal — Rithmic Provider
================================================================================
Implements BaseProvider for Rithmic futures data feed.

Uses the async_rithmic library (Protocol Buffer API over WebSocket) to:
    - Stream live BBO ticks for futures instruments
    - Stream live time bars (M1/M5/M15/M30/H1)
    - Fetch historical bars on demand
    - Auto-resolve front month contracts (ES → ESM6, etc.)

Architecture:
    async_rithmic requires running inside a proper asyncio event loop.
    This provider exposes async_connect()/async_disconnect() methods that
    run directly in the caller's event loop (FastAPI's uvicorn loop).

    data_server.py lifespan calls:
        await provider.async_connect()      # in the main event loop
    
    Sync methods (get_latest_ticks, get_bars, etc.) read from caches
    populated by streaming callbacks running in the same event loop.

    For non-async contexts (test scripts), connect() falls back to
    asyncio.run() which works but blocks the calling thread.

Config dict keys:
    id:          str  — unique provider ID (e.g., "rithmic_default")
    type:        str  — "rithmic"
    label:       str  — display name (e.g., "Rithmic — Paper Trading")
    enabled:     bool — True
    user:        str  — Rithmic username (from local_config.ini)
    password:    str  — Rithmic password (from local_config.ini)
    system_name: str  — "Rithmic Test" / "Rithmic 01" etc.
    url:         str  — server URL (e.g., "rituz00100.rithmic.com:443")
    app_name:    str  — "Quantum Terminal" (default)
    app_version: str  — "1.0" (default)
    symbol_map:  dict — canonical → {base, exchange} overrides (optional)

Dependencies:
    pip install async_rithmic
================================================================================
"""

import logging
import threading
import asyncio
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

from models import (
    TickData, BarData, AccountInfo, SymbolInfo,
    OrderRequest, OrderResult, Position,
)
from providers.base_provider import BaseProvider

log = logging.getLogger("provider.rithmic")

# ── async_rithmic imported lazily ──
try:
    from async_rithmic import (
        RithmicClient,
        TimeBarType,
        DataType,
        LastTradePresenceBits,
        BestBidOfferPresenceBits,
    )
    RITHMIC_AVAILABLE = True
except ImportError:
    RITHMIC_AVAILABLE = False
    RithmicClient = None
    TimeBarType = None
    DataType = None


# ============================================================
# 1. SYMBOL MAPPING — Canonical → Rithmic
# ============================================================

# Maps Quantum Terminal canonical tickers to Rithmic base symbols + exchange.
# get_front_month_contract() auto-resolves the actual contract code
# (e.g., "ES" → "ESM6" for June 2026).
#
# IMPORTANT: Futures canonical tickers are SEPARATE from CFD tickers.
#   ES   = CME E-mini S&P 500 futures   (Rithmic)
#   US500 = S&P 500 CFD                  (MT5/CFI)
# They coexist in the universe as independent instruments.
RITHMIC_SYMBOL_MAP = {
    # Equity index futures
    "ES":   {"base": "ES",   "exchange": "CME"},
    "NQ":   {"base": "NQ",   "exchange": "CME"},
    "YM":   {"base": "YM",   "exchange": "CBOT"},
    # Metal futures
    "GC":   {"base": "GC",   "exchange": "COMEX"},
    "SI":   {"base": "SI",   "exchange": "COMEX"},
    # Energy futures
    "CL":   {"base": "CL",   "exchange": "NYMEX"},
    "BZ":   {"base": "BZ",   "exchange": "NYMEX"},
    # European index futures (Eurex — may not be available on all accounts)
    "FDAX": {"base": "FDAX", "exchange": "EUREX"},
    "Z":    {"base": "Z",    "exchange": "LIFFE"},
}

# Maps our timeframe strings to (TimeBarType enum name, period) pairs.
RITHMIC_TF_MAP = {
    "M1":  ("MINUTE_BAR", 1),
    "M5":  ("MINUTE_BAR", 5),
    "M15": ("MINUTE_BAR", 15),
    "M30": ("MINUTE_BAR", 30),
    "H1":  ("MINUTE_BAR", 60),
    "H4":  ("MINUTE_BAR", 240),
    "D1":  ("DAILY_BAR",  1),
}

# Futures contract specs (static — used for SymbolInfo).
FUTURES_SPECS = {
    "ES":   {"decimals": 2, "tick_size": 0.25, "tick_value": 12.50,
             "contract_size": 50.0, "description": "E-mini S&P 500",
             "currency": "USD", "min_lot": 1, "lot_step": 1, "max_lot": 100},
    "NQ":   {"decimals": 2, "tick_size": 0.25, "tick_value": 5.00,
             "contract_size": 20.0, "description": "E-mini NASDAQ-100",
             "currency": "USD", "min_lot": 1, "lot_step": 1, "max_lot": 100},
    "YM":   {"decimals": 0, "tick_size": 1.0,  "tick_value": 5.00,
             "contract_size": 5.0,  "description": "E-mini Dow",
             "currency": "USD", "min_lot": 1, "lot_step": 1, "max_lot": 100},
    "GC":   {"decimals": 2, "tick_size": 0.10, "tick_value": 10.00,
             "contract_size": 100.0, "description": "Gold Futures",
             "currency": "USD", "min_lot": 1, "lot_step": 1, "max_lot": 100},
    "SI":   {"decimals": 3, "tick_size": 0.005, "tick_value": 25.00,
             "contract_size": 5000.0, "description": "Silver Futures",
             "currency": "USD", "min_lot": 1, "lot_step": 1, "max_lot": 100},
    "CL":   {"decimals": 2, "tick_size": 0.01, "tick_value": 10.00,
             "contract_size": 1000.0, "description": "Crude Oil WTI",
             "currency": "USD", "min_lot": 1, "lot_step": 1, "max_lot": 100},
    "BZ":   {"decimals": 2, "tick_size": 0.01, "tick_value": 10.00,
             "contract_size": 1000.0, "description": "Brent Crude Oil",
             "currency": "USD", "min_lot": 1, "lot_step": 1, "max_lot": 100},
    "FDAX": {"decimals": 1, "tick_size": 0.5,  "tick_value": 12.50,
             "contract_size": 25.0, "description": "DAX Futures",
             "currency": "EUR", "min_lot": 1, "lot_step": 1, "max_lot": 50},
    "Z":    {"decimals": 1, "tick_size": 0.5,  "tick_value": 5.00,
             "contract_size": 10.0, "description": "FTSE 100 Futures",
             "currency": "GBP", "min_lot": 1, "lot_step": 1, "max_lot": 50},
}

# Minutes per timeframe — used to compute bar fetch windows
TF_MINUTES = {
    "M1": 1, "M5": 5, "M15": 15, "M30": 30,
    "H1": 60, "H4": 240, "D1": 1440, "W1": 10080,
}


# ============================================================
# 2. RITHMIC PROVIDER
# ============================================================

class RithmicProvider(BaseProvider):
    """
    Rithmic futures data provider.

    Data-only provider (can_execute=False for now). Streams live
    ticks and bars via async_rithmic, serves data through the
    synchronous BaseProvider interface.

    IMPORTANT: async_rithmic must run inside a proper asyncio event
    loop. Use async_connect() from FastAPI's lifespan, or connect()
    which falls back to asyncio.run() for standalone scripts.
    """

    def __init__(self, account_config: Dict[str, Any]):
        self._id = account_config.get("id", "rithmic_default")
        self._label = account_config.get("label", "Rithmic")
        self._user = account_config.get("user", "")
        self._password = account_config.get("password", "")
        self._system_name = account_config.get("system_name", "Rithmic Test")
        self._url = account_config.get("url", "")
        self._app_name = account_config.get("app_name", "Quantum Terminal")
        self._app_version = account_config.get("app_version", "1.0")

        # Symbol map: merge defaults with user overrides
        self._symbol_map = dict(RITHMIC_SYMBOL_MAP)
        user_map = account_config.get("symbol_map", {})
        self._symbol_map.update(user_map)

        # Connection state
        self._connected = False
        self._client: Optional[Any] = None  # RithmicClient instance

        # Reference to the event loop we're running in (set during connect)
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Resolved front month contracts: canonical → "ESM6" etc.
        self._front_months: Dict[str, str] = {}
        # Reverse map: "ESM6" → "ES"
        self._reverse_map: Dict[str, str] = {}

        # Tick cache: canonical → TickData (written from async callbacks,
        # read from sync methods — both in the same thread in production)
        self._tick_cache: Dict[str, TickData] = {}

        # Bar buffer: "TICKER_TF" → deque of BarData (ring buffer)
        self._bar_buffers: Dict[str, deque] = {}
        self._max_bar_buffer = 500

        # Bar tracking for check_new_bars
        self._last_bar_times: Dict[str, str] = {}

        # Subscribed symbols (canonical names that resolved successfully)
        self._subscribed: List[str] = []

    # ── Identity ──

    @property
    def provider_type(self) -> str:
        return "rithmic"

    @property
    def provider_id(self) -> str:
        return self._id

    @property
    def label(self) -> str:
        return self._label

    # ── Capabilities ──

    @property
    def can_execute(self) -> bool:
        return False  # Data-only for now

    @property
    def can_stream_ticks(self) -> bool:
        return True

    @property
    def supported_timeframes(self) -> List[str]:
        return list(RITHMIC_TF_MAP.keys())

    # ── Connection Lifecycle ──

    @property
    def connected(self) -> bool:
        return self._connected

    def connect(self) -> bool:
        """
        Synchronous connect — for use in non-async contexts (test scripts).

        In production (data_server.py), use async_connect() instead.
        This method uses asyncio.run() which blocks the calling thread.
        """
        if not RITHMIC_AVAILABLE:
            log.warning("async_rithmic package not installed — "
                        "run: pip install async_rithmic")
            return False

        if self._connected:
            return True

        if not self._user or not self._password or not self._url:
            log.error("Rithmic credentials missing — "
                      "check [rithmic] in local_config.ini")
            return False

        try:
            return asyncio.run(self.async_connect())
        except Exception as e:
            log.error(f"Rithmic connect error: {e}")
            return False

    async def async_connect(self) -> bool:
        """
        Async connect — runs in the caller's event loop.

        Called from data_server.py lifespan:
            connected = await provider.async_connect()

        1. Create RithmicClient
        2. Connect to server
        3. Resolve front month contracts
        4. Subscribe to BBO ticks + M1 time bars
        """
        if not RITHMIC_AVAILABLE:
            log.warning("async_rithmic not installed")
            return False

        if self._connected:
            return True

        if not self._user or not self._password or not self._url:
            log.error("Rithmic credentials missing")
            return False

        try:
            self._loop = asyncio.get_running_loop()

            self._client = RithmicClient(
                user=self._user,
                password=self._password,
                system_name=self._system_name,
                app_name=self._app_name,
                app_version=self._app_version,
                url=self._url,
            )

            await self._client.connect()
            log.info("Rithmic client connected to server")

            # Register event callbacks
            self._client.on_tick += self._on_tick
            self._client.on_time_bar += self._on_time_bar

            # Resolve front month contracts
            for canonical, mapping in self._symbol_map.items():
                base = mapping["base"]
                exchange = mapping["exchange"]
                try:
                    front = await self._client.get_front_month_contract(
                        base, exchange
                    )
                    self._front_months[canonical] = front
                    self._reverse_map[front] = canonical
                    log.info(f"  {canonical} -> {front} ({exchange})")
                except Exception as e:
                    log.warning(
                        f"  {canonical} -> FAILED to resolve "
                        f"{base}@{exchange}: {e}"
                    )

            # Subscribe to live data for resolved symbols
            for canonical, contract in self._front_months.items():
                exchange = self._symbol_map[canonical]["exchange"]
                try:
                    # Subscribe to BBO ticks
                    data_type = DataType.LAST_TRADE | DataType.BBO
                    await self._client.subscribe_to_market_data(
                        contract, exchange, data_type
                    )

                    # Subscribe to M1 time bars
                    await self._client.subscribe_to_time_bar_data(
                        contract, exchange, TimeBarType.MINUTE_BAR, 1
                    )

                    self._subscribed.append(canonical)

                    # Initialize bar buffer
                    self._bar_buffers[f"{canonical}_M1"] = deque(
                        maxlen=self._max_bar_buffer
                    )

                    log.info(
                        f"  Subscribed: {canonical} ({contract}@{exchange})"
                    )
                except Exception as e:
                    log.warning(
                        f"  Subscribe failed for {canonical} "
                        f"({contract}@{exchange}): {e}"
                    )

            if self._subscribed:
                self._connected = True
                log.info(
                    f"Rithmic connected: {self._system_name} | "
                    f"Subscribed: {len(self._subscribed)} symbols | "
                    f"URL: {self._url}"
                )
                return True
            else:
                log.error("No symbols subscribed — connection not useful")
                await self._async_disconnect()
                return False

        except Exception as e:
            log.error(f"Rithmic async connect failed: {e}")
            return False

    def disconnect(self) -> None:
        """
        Synchronous disconnect — for use in non-async contexts.

        In production, use async_disconnect() instead, or this method
        will schedule the disconnect in the running event loop.
        """
        if not self._connected:
            return

        # If we have a reference to the event loop and it's running,
        # schedule the async disconnect
        if self._loop and self._loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self._async_disconnect(), self._loop
            )
            try:
                future.result(timeout=10)
            except Exception as e:
                log.warning(f"Rithmic disconnect error: {e}")
        else:
            # No running loop — try asyncio.run as last resort
            try:
                asyncio.run(self._async_disconnect())
            except Exception:
                pass

        self._connected = False
        self._subscribed.clear()
        self._front_months.clear()
        self._reverse_map.clear()
        self._tick_cache.clear()
        self._bar_buffers.clear()
        self._client = None
        log.info("Rithmic disconnected")

    async def async_disconnect(self) -> None:
        """Async disconnect — called from lifespan shutdown."""
        await self._async_disconnect()
        self._connected = False
        self._subscribed.clear()
        self._front_months.clear()
        self._reverse_map.clear()
        self._tick_cache.clear()
        self._bar_buffers.clear()
        self._client = None
        log.info("Rithmic disconnected")

    def heartbeat(self) -> bool:
        """Check if the Rithmic connection is alive."""
        # async_rithmic handles heartbeats internally
        return self._connected

    # ── Market Data ──

    def get_latest_ticks(self, symbols: List[str]) -> Dict[str, TickData]:
        """Return latest cached ticks for requested symbols."""
        result = {}
        for s in symbols:
            if s in self._tick_cache:
                result[s] = self._tick_cache[s]
        return result

    def get_bars(
        self, ticker: str, timeframe: str = "M15", count: int = 200
    ) -> List[BarData]:
        """
        Fetch bars for a canonical ticker.

        Checks local buffer first. If not enough bars, fetches from
        Rithmic history plant via the event loop.
        """
        if ticker not in self._front_months:
            return []

        # Check buffer first
        buf_key = f"{ticker}_{timeframe}"
        if buf_key in self._bar_buffers:
            bars = list(self._bar_buffers[buf_key])
            if len(bars) >= count:
                return bars[-count:]

        # Fetch from history — need the event loop
        if not self._loop or not self._loop.is_running():
            return []

        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_get_bars(ticker, timeframe, count),
                self._loop,
            )
            return future.result(timeout=30)
        except Exception as e:
            log.warning(f"[{ticker}] Historical bar fetch failed: {e}")
            return []

    def check_new_bars(
        self, symbols: List[str], timeframe: str = "M15"
    ) -> List[dict]:
        """Return bars received since last check."""
        new_bars = []
        for s in symbols:
            buf_key = f"{s}_{timeframe}"
            if buf_key not in self._bar_buffers:
                continue

            track_key = f"{s}_{timeframe}"
            last_time = self._last_bar_times.get(track_key)
            buf = self._bar_buffers[buf_key]

            for bar in buf:
                if last_time is None or bar.time > last_time:
                    new_bars.append({
                        "type": "bar",
                        "ticker": s,
                        "timeframe": timeframe,
                        "bar": bar.to_dict(),
                    })
                    self._last_bar_times[track_key] = bar.time

        return new_bars

    def get_symbol_info(self, ticker: str) -> Optional[SymbolInfo]:
        """Return static futures contract metadata."""
        if ticker not in self._symbol_map:
            return None

        mapping = self._symbol_map[ticker]
        base = mapping["base"]
        spec = FUTURES_SPECS.get(base)
        if not spec:
            return None

        broker_symbol = self._front_months.get(ticker, base)

        return SymbolInfo(
            ticker=ticker,
            broker_symbol=broker_symbol,
            asset_class="FUTURES",
            decimals=spec["decimals"],
            description=spec["description"],
            trade_allowed=False,
            min_lot=spec["min_lot"],
            max_lot=spec["max_lot"],
            lot_step=spec["lot_step"],
            contract_size=spec["contract_size"],
            currency_profit=spec["currency"],
            currency_margin=spec["currency"],
            tick_size=spec["tick_size"],
            tick_value=spec["tick_value"],
        )

    def get_account_info(self) -> Optional[AccountInfo]:
        """Not applicable for data-only provider."""
        return None

    # ── Symbol Resolution ──

    def resolve_symbol(self, canonical: str) -> Optional[str]:
        """Map canonical ticker to resolved Rithmic contract code."""
        return self._front_months.get(canonical)

    def resolve_universe(self, universe: List[str]) -> List[str]:
        """Return which universe tickers this provider can serve."""
        return [t for t in universe if t in self._front_months]

    def get_available_symbols(self) -> List[str]:
        """Return canonical symbols that were successfully subscribed."""
        return list(self._subscribed)

    # ============================================================
    # INTERNAL — Async Operations
    # ============================================================

    async def _async_disconnect(self) -> None:
        """Async cleanup: unsubscribe and disconnect."""
        if not self._client:
            return

        try:
            for canonical in self._subscribed:
                if canonical not in self._front_months:
                    continue
                contract = self._front_months[canonical]
                exchange = self._symbol_map[canonical]["exchange"]
                try:
                    await self._client.unsubscribe_from_market_data(
                        contract, exchange,
                        DataType.LAST_TRADE | DataType.BBO,
                    )
                    await self._client.unsubscribe_from_time_bar_data(
                        contract, exchange, TimeBarType.MINUTE_BAR, 1,
                    )
                except Exception:
                    pass

            await self._client.disconnect()
        except Exception as e:
            log.warning(f"Rithmic async disconnect error: {e}")

    async def _async_get_bars(
        self, ticker: str, timeframe: str, count: int
    ) -> List[BarData]:
        """Fetch historical time bars from Rithmic history plant."""
        if ticker not in self._front_months or not self._client:
            return []

        contract = self._front_months[ticker]
        exchange = self._symbol_map[ticker]["exchange"]

        tf_info = RITHMIC_TF_MAP.get(timeframe)
        if not tf_info:
            log.warning(f"[{ticker}] Unsupported timeframe: {timeframe}")
            return []

        bar_type_name, period = tf_info

        if bar_type_name == "MINUTE_BAR":
            bar_type = TimeBarType.MINUTE_BAR
        elif bar_type_name == "DAILY_BAR":
            bar_type = TimeBarType.DAILY_BAR
        else:
            bar_type = TimeBarType.MINUTE_BAR

        # Calculate time window
        minutes_per_bar = TF_MINUTES.get(timeframe, 15)
        total_minutes = int(count * minutes_per_bar * 1.2)  # 20% buffer
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=total_minutes)

        try:
            raw_bars = await self._client.get_time_bars(
                contract, exchange, bar_type, period,
                start_time, end_time,
            )

            bars = []
            for rb in raw_bars:
                bar = self._parse_bar(rb)
                if bar:
                    bars.append(bar)

            bars.sort(key=lambda b: b.time)
            bars = bars[-count:]

            # Cache in buffer
            buf_key = f"{ticker}_{timeframe}"
            self._bar_buffers[buf_key] = deque(bars, maxlen=self._max_bar_buffer)

            log.info(
                f"[{ticker}] Fetched {len(bars)} {timeframe} bars "
                f"from Rithmic history"
            )
            return bars

        except AttributeError:
            log.warning(
                f"[{ticker}] get_time_bars() not available. "
                f"Bars will accumulate from live stream."
            )
            return []
        except Exception as e:
            log.warning(f"[{ticker}] Historical bar fetch error: {e}")
            return []

    # ============================================================
    # INTERNAL — Streaming Callbacks
    # ============================================================

    async def _on_tick(self, data: dict) -> None:
        """Callback for live tick data. Updates tick cache."""
        try:
            security_code = data.get("symbol", "")
            canonical = self._reverse_map.get(security_code)
            if not canonical:
                return

            data_type = data.get("data_type")
            presence = data.get("presence_bits", 0)

            # Preserve existing values
            existing = self._tick_cache.get(canonical)
            bid = existing.bid if existing else 0.0
            ask = existing.ask if existing else 0.0
            last = existing.last if existing else 0.0

            if data_type == DataType.BBO:
                if presence & BestBidOfferPresenceBits.BID:
                    bid = float(data.get("bid_price", bid))
                if presence & BestBidOfferPresenceBits.ASK:
                    ask = float(data.get("ask_price", ask))
            elif data_type == DataType.LAST_TRADE:
                if presence & LastTradePresenceBits.LAST_TRADE:
                    last = float(data.get("trade_price", last))

            if bid > 0 or ask > 0 or last > 0:
                now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
                spread = round(ask - bid, 6) if (bid > 0 and ask > 0) else 0.0
                if last == 0 and bid > 0:
                    last = (bid + ask) / 2

                self._tick_cache[canonical] = TickData(
                    ticker=canonical,
                    bid=bid,
                    ask=ask,
                    last=last,
                    time=now,
                    spread=spread,
                )

        except Exception as e:
            log.debug(f"Tick callback error: {e}")

    async def _on_time_bar(self, data: dict) -> None:
        """Callback for live time bar data. Adds to ring buffer."""
        try:
            security_code = data.get("symbol", "")
            canonical = self._reverse_map.get(security_code)
            if not canonical:
                return

            bar = self._parse_bar(data)
            if not bar:
                return

            period = data.get("period", 1)
            tf_key = self._period_to_tf(period)
            buf_key = f"{canonical}_{tf_key}"

            if buf_key not in self._bar_buffers:
                self._bar_buffers[buf_key] = deque(
                    maxlen=self._max_bar_buffer
                )
            self._bar_buffers[buf_key].append(bar)

        except Exception as e:
            log.debug(f"Time bar callback error: {e}")

    # ============================================================
    # INTERNAL — Helpers
    # ============================================================

    def _parse_bar(self, data: dict) -> Optional[BarData]:
        """Parse a raw Rithmic bar dict into a BarData object."""
        try:
            open_p = float(data.get("open_price", data.get("open", 0)))
            high_p = float(data.get("high_price", data.get("high", 0)))
            low_p = float(data.get("low_price", data.get("low", 0)))
            close_p = float(data.get("close_price", data.get("close", 0)))
            volume = int(data.get("volume", 0))

            bar_time = data.get("bar_end_time", data.get("time", ""))
            if isinstance(bar_time, datetime):
                time_str = bar_time.strftime("%Y-%m-%dT%H:%M:%S")
            elif isinstance(bar_time, (int, float)):
                time_str = datetime.fromtimestamp(
                    bar_time, tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%S")
            else:
                time_str = str(bar_time)

            if open_p == 0 and close_p == 0:
                return None

            return BarData(
                time=time_str,
                open=open_p,
                high=high_p,
                low=low_p,
                close=close_p,
                volume=volume,
            )
        except Exception:
            return None

    @staticmethod
    def _period_to_tf(period: int) -> str:
        """Convert minute period to our timeframe string."""
        tf_map = {1: "M1", 5: "M5", 15: "M15", 30: "M30", 60: "H1", 240: "H4"}
        return tf_map.get(period, "M1")
