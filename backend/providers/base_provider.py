"""
================================================================================
Quantum Terminal — Base Provider Interface
================================================================================
Abstract contract that every data/execution provider must implement.

The data_server and config_manager talk to providers ONLY through this
interface. MT5, Binance, Polygon, or any future source plugs in by
subclassing BaseProvider and implementing the required methods.

Design principles:
    - All methods are synchronous (callers use asyncio.to_thread)
    - Providers manage their own connection lifecycle
    - Canonical ticker names everywhere — providers map internally
    - Providers declare their capabilities (data-only vs data+execution)

Usage:
    from providers.base_provider import BaseProvider
    from providers.mt5_provider import MT5Provider

    provider = MT5Provider(account_config)
    provider.connect()
    ticks = provider.get_latest_ticks(["XAUUSD", "EURUSD"])
================================================================================
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from models import (
    TickData, BarData, AccountInfo, SymbolInfo,
    OrderRequest, OrderResult, Position, PendingOrder,
)


class BaseProvider(ABC):
    """
    Abstract provider interface.
    
    Every provider has:
        - A type name (e.g., "mt5", "binance")
        - A unique instance ID (e.g., "mt5_primary", "binance_spot")
        - Connection lifecycle (connect/disconnect/reconnect)
        - Market data methods (ticks, bars, symbol info)
        - Optional execution methods (orders, positions)
    
    Providers are synchronous. The data_server wraps calls in
    asyncio.to_thread() to avoid blocking the event loop.
    """

    # ── Identity ──

    @property
    @abstractmethod
    def provider_type(self) -> str:
        """Provider type identifier. E.g., 'mt5', 'binance', 'polygon'."""
        ...

    @property
    @abstractmethod
    def provider_id(self) -> str:
        """Unique instance ID. E.g., 'mt5_primary'. Set from account config."""
        ...

    @property
    @abstractmethod
    def label(self) -> str:
        """Human-readable label. E.g., 'MT5 — CFI (Live)'."""
        ...

    # ── Capabilities ──

    @property
    def can_stream_ticks(self) -> bool:
        """Whether this provider supports live tick polling."""
        return True

    @property
    def can_execute(self) -> bool:
        """Whether this provider supports order execution."""
        return False

    @property
    def supported_timeframes(self) -> List[str]:
        """List of timeframe strings this provider supports."""
        return ["M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1"]

    # ── Connection Lifecycle ──

    @property
    @abstractmethod
    def connected(self) -> bool:
        """Whether the provider is currently connected."""
        ...

    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the data/execution source.
        Returns True on success, False on failure.
        Must be idempotent — calling connect() when already connected is safe.
        """
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Cleanly shut down the connection."""
        ...

    def reconnect(self) -> bool:
        """Disconnect and reconnect. Override for custom reconnect logic."""
        self.disconnect()
        return self.connect()

    def heartbeat(self) -> bool:
        """
        Lightweight connection health check.
        Returns True if the connection is alive, False otherwise.
        If False, sets internal connected state to False so reconnect_loop picks it up.
        Override in subclasses for provider-specific health checks.
        """
        return self.connected

    # ── Market Data ──

    @abstractmethod
    def get_latest_ticks(self, symbols: List[str]) -> Dict[str, TickData]:
        """
        Fetch latest tick for each symbol.
        
        Args:
            symbols: List of canonical ticker names (e.g., ["XAUUSD", "EURUSD"])
        
        Returns:
            Dict mapping canonical ticker → TickData.
            Missing/failed symbols are simply omitted.
        """
        ...

    @abstractmethod
    def get_bars(
        self, ticker: str, timeframe: str = "M15", count: int = 200
    ) -> List[BarData]:
        """
        Fetch recent OHLCV bars for a canonical ticker.
        
        Args:
            ticker: Canonical symbol name
            timeframe: Timeframe string (M1, M5, M15, H1, H4, D1, etc.)
            count: Number of bars to fetch
        
        Returns:
            List of BarData, oldest first. Empty list on failure.
        """
        ...

    @abstractmethod
    def check_new_bars(
        self, symbols: List[str], timeframe: str = "M15"
    ) -> List[dict]:
        """
        Detect newly closed bars since last check.
        
        Returns list of dicts:
            {"type": "bar", "ticker": str, "timeframe": str, "bar": BarData.to_dict()}
        
        Implementation must track last-seen bar timestamps internally.
        """
        ...

    @abstractmethod
    def get_symbol_info(self, ticker: str) -> Optional[SymbolInfo]:
        """
        Get metadata for a symbol (decimals, lot sizing, contract size, etc.)
        Returns None if symbol not found.
        """
        ...

    def get_all_symbol_info(self, symbols: List[str]) -> Dict[str, SymbolInfo]:
        """
        Batch symbol info for multiple tickers.
        Default implementation calls get_symbol_info() in a loop.
        Override for providers that support batch queries.
        """
        result = {}
        for s in symbols:
            info = self.get_symbol_info(s)
            if info is not None:
                result[s] = info
        return result

    # ── Account Info ──

    @abstractmethod
    def get_account_info(self) -> Optional[AccountInfo]:
        """
        Get current account snapshot (balance, equity, margin, etc.)
        Returns None if not connected or not applicable.
        """
        ...

    # ── Execution (optional — override if can_execute is True) ──

    def place_order(self, order: OrderRequest) -> OrderResult:
        """Place an order. Override in execution-capable providers."""
        return OrderResult(
            success=False,
            error=f"Provider '{self.provider_type}' does not support execution",
        )

    def get_positions(self) -> List[Position]:
        """Get all open positions. Override in execution-capable providers."""
        return []

    def close_position(self, ticket: str, lots: Optional[float] = None) -> OrderResult:
        """
        Close a position (fully or partially).
        Override in execution-capable providers.
        """
        return OrderResult(
            success=False,
            error=f"Provider '{self.provider_type}' does not support execution",
        )

    def get_bars_range(self, ticker, timeframe, from_dt, to_dt):
        """Fetch OHLCV bars between two datetimes.
        Override in providers that support historical range queries."""
        return []

    def get_pending_orders(self) -> List[PendingOrder]:
        """
        List all resting (non-filled) pending orders (LIMIT / STOP).
        Override in execution-capable providers.
        """
        return []

    def cancel_order(self, ticket: str) -> OrderResult:
        """Cancel a resting pending order. Override in execution-capable providers."""
        return OrderResult(
            success=False,
            error=f"Provider '{self.provider_type}' does not support execution",
        )

    def modify_order(
        self, ticket: str,
        price: Optional[float] = None,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
    ) -> OrderResult:
        """Modify price / SL / TP on a pending order. Override in execution-capable providers."""
        return OrderResult(
            success=False,
            error=f"Provider '{self.provider_type}' does not support execution",
        )

    # ── Symbol Resolution ──

    @abstractmethod
    def resolve_symbol(self, canonical: str) -> Optional[str]:
        """
        Map a canonical ticker name to the provider's native symbol.
        Returns None if the symbol is not available in this provider.
        """
        ...

    def get_available_symbols(self) -> List[str]:
        """
        Return list of all canonical symbols this provider can serve.
        Default: empty (override to support symbol discovery).
        """
        return []

    # ── String Representation ──

    def __repr__(self) -> str:
        status = "connected" if self.connected else "disconnected"
        return f"<{self.__class__.__name__} id='{self.provider_id}' [{status}]>"