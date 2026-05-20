# version: v1
"""
================================================================================
Quantum Terminal — Tradovate Provider (POC)

Proof-of-work integration with Tradovate for users who don't run MT5.

What this does:
  · Authenticates against /auth/accesstokenrequest (REST)
  · Resolves CFD-style tickers (XAUUSD, US500, ...) → Tradovate futures roots
    (GC, ES, ...) → front-month contracts (GCM6, ESM6, ...)
  · Fetches historical bars over the market-data WebSocket (md/getChart)

What this does NOT do (deferred to later phases):
  · Live tick streaming (comes next)
  · Order placement (futures orders are quite different from CFDs)
  · Price conversion between CFD and futures price space
  · Rollover-adjusted continuous contracts
  · Settlement/margin accounting

Reversibility: this file is standalone. Nothing in MT5 or the base terminal
touches it. Delete the file + one route-include line and Tradovate goes
away entirely.
================================================================================
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger("tradovate_provider")

# ─── Dependencies (both already in consumer_venv) ────────────────────────
try:
    import httpx
except ImportError:
    httpx = None
    log.warning("httpx not installed — Tradovate provider will be inert")

try:
    import websockets
except ImportError:
    websockets = None
    log.warning("websockets not installed — Tradovate provider will be inert")


# ─── Endpoint config ─────────────────────────────────────────────────────
ENDPOINTS = {
    "demo": {
        "rest":  "https://demo.tradovateapi.com/v1",
        "md_ws": "wss://md-demo.tradovateapi.com/v1/websocket",
    },
    "live": {
        "rest":  "https://live.tradovateapi.com/v1",
        "md_ws": "wss://md.tradovateapi.com/v1/websocket",
    },
}


# ─── CFD → Futures root map (POC v1) ────────────────────────────────────
# Add more entries as we validate them. Unmapped tickers raise a clean
# "symbol not supported by Tradovate" error rather than crashing.
CFD_TO_FUTURES_ROOT = {
    "XAUUSD": "GC",   # Gold futures (100 oz, COMEX)
    "XAGUSD": "SI",   # Silver futures (5,000 oz, COMEX)
    "XTIUSD": "CL",   # Crude oil futures (1,000 bbl, NYMEX)
    "US500":  "ES",   # E-mini S&P 500 (CME)
    "USTEC":  "NQ",   # E-mini Nasdaq 100 (CME)
    "GER40":  "FDAX", # DAX futures (Eurex) — requires exchange subscription
    "BTCUSD": "BTC",  # Micro Bitcoin or BTC futures (CME)
    # FX / UK100 / SOLUSD intentionally unmapped for POC.
}

# ─── Futures month codes ────────────────────────────────────────────────
MONTH_CODES = {1:"F", 2:"G", 3:"H", 4:"J", 5:"K", 6:"M",
               7:"N", 8:"Q", 9:"U", 10:"V", 11:"X", 12:"Z"}

# Which months does each product expire in?
# Quarterlies = Mar/Jun/Sep/Dec, monthly = every month, bi-monthly = even months.
PRODUCT_EXPIRY_MONTHS = {
    "ES":  [3, 6, 9, 12],   # E-mini S&P — quarterly
    "NQ":  [3, 6, 9, 12],
    "GC":  [2, 4, 6, 8, 10, 12],  # Gold — bi-monthly
    "SI":  [1, 3, 5, 7, 9, 12],   # Silver — not every month
    "CL":  list(range(1, 13)),     # Crude — monthly
    "BTC": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],  # Bitcoin — monthly
    "FDAX":[3, 6, 9, 12],
}


# ─── Timeframe → Tradovate chart description ────────────────────────────
def _tf_to_chart_desc(timeframe: str) -> Dict[str, Any]:
    tf_map = {
        "M1":  ("MinuteBar",   1),
        "M5":  ("MinuteBar",   5),
        "M15": ("MinuteBar",  15),
        "M30": ("MinuteBar",  30),
        "H1":  ("MinuteBar",  60),
        "H4":  ("MinuteBar", 240),
        "D1":  ("DailyBar",    1),
    }
    underlying, size = tf_map.get(timeframe.upper(), ("MinuteBar", 15))
    return {
        "underlyingType": underlying,
        "elementSize": size,
        "elementSizeUnit": "UnderlyingUnits",
        "withHistogram": False,
    }


@dataclass
class TradovateAuth:
    access_token: str = ""
    md_access_token: str = ""
    user_id: int = 0
    name: str = ""
    has_live: bool = False
    user_status: str = ""
    expires_at: float = 0.0   # epoch seconds
    md_expires_at: float = 0.0

    def is_valid(self) -> bool:
        return bool(self.access_token) and time.time() < self.expires_at - 30


class TradovateProvider:
    """POC Tradovate provider. Not a full BaseProvider subclass yet —
    we'll upgrade to that once the smoke test passes. For now it exposes
    the methods tradovate_routes.py needs to service the POC endpoint."""

    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._auth = TradovateAuth()
        self._last_error: str = ""

    # ── Config updates (called from PATCH /api/tradovate/config) ──
    def update_config(self, new_config: Dict[str, Any]) -> None:
        self._config.update(new_config or {})
        # Invalidate cached auth so the next connect uses new creds.
        self._auth = TradovateAuth()

    @property
    def connected(self) -> bool:
        return self._auth.is_valid()

    @property
    def is_delayed(self) -> bool:
        # Free Tradovate demo accounts are delayed 10 minutes for most CME
        # products. hasLive=True means the user has paid live market data;
        # False means delayed. Real instrument-by-instrument delay info comes
        # from md/getContract-like queries — POC just surfaces the boolean.
        return not self._auth.has_live

    @property
    def status_dict(self) -> Dict[str, Any]:
        return {
            "connected":   self.connected,
            "user_id":     self._auth.user_id,
            "name":        self._auth.name,
            "user_status": self._auth.user_status,
            "has_live":    self._auth.has_live,
            "delayed":     self.is_delayed,
            "env":         self._config.get("env", "demo"),
            "expires_at":  self._auth.expires_at,
            "error":       self._last_error,
        }

    # ── Authentication ────────────────────────────────────────────
    async def authenticate(self) -> Dict[str, Any]:
        self._last_error = ""
        if httpx is None:
            self._last_error = "httpx not installed in venv"
            return {"success": False, "error": self._last_error}
        cfg = self._config
        env = (cfg.get("env") or "demo").lower()
        if env not in ENDPOINTS:
            self._last_error = f"unknown env '{env}' (expected demo or live)"
            return {"success": False, "error": self._last_error}
        required = ["app_id", "cid", "sec", "username", "password"]
        missing = [k for k in required if not cfg.get(k)]
        if missing:
            self._last_error = f"missing creds: {', '.join(missing)}"
            return {"success": False, "error": self._last_error}

        url = ENDPOINTS[env]["rest"] + "/auth/accesstokenrequest"
        body = {
            "name": cfg["username"],
            "password": cfg["password"],
            "appId": cfg["app_id"],
            "appVersion": cfg.get("app_version", "1.0"),
            "cid": int(cfg["cid"]),
            "sec": cfg["sec"],
            "deviceId": cfg.get("device_id", "QuantumTerminal-consumer-poc"),
        }
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.post(url, json=body)
            if r.status_code != 200:
                self._last_error = f"HTTP {r.status_code}: {r.text[:200]}"
                return {"success": False, "error": self._last_error}
            data = r.json()
        except Exception as e:
            self._last_error = f"auth request failed: {e}"
            log.error(self._last_error)
            return {"success": False, "error": self._last_error}

        # Tradovate returns error info inside a 200 response for bad creds
        err_code = data.get("errorText") or data.get("p-ticket")
        if err_code and not data.get("accessToken"):
            self._last_error = f"tradovate rejected auth: {err_code}"
            return {"success": False, "error": self._last_error}

        # Parse token expiration — Tradovate returns ISO8601 in expirationTime
        def _parse_iso(s):
            if not s:
                return time.time() + 4000  # ~66 min fallback
            try:
                s = s.replace("Z", "+00:00")
                return datetime.fromisoformat(s).timestamp()
            except Exception:
                return time.time() + 4000

        self._auth = TradovateAuth(
            access_token   = data.get("accessToken", ""),
            md_access_token= data.get("mdAccessToken", ""),
            user_id        = data.get("userId", 0) or 0,
            name           = data.get("name", "") or cfg["username"],
            has_live       = bool(data.get("hasLive", False)),
            user_status    = data.get("userStatus", "") or "",
            expires_at     = _parse_iso(data.get("expirationTime")),
            md_expires_at  = _parse_iso(data.get("expirationTime")),
        )
        if not self._auth.access_token:
            self._last_error = "no accessToken in response"
            return {"success": False, "error": self._last_error}
        return {"success": True, **self.status_dict}

    def disconnect(self) -> None:
        self._auth = TradovateAuth()
        self._last_error = ""

    # ── Symbol resolution ─────────────────────────────────────────
    def _resolve_root(self, ticker: str) -> Optional[str]:
        t = (ticker or "").upper()
        if t in CFD_TO_FUTURES_ROOT:
            return CFD_TO_FUTURES_ROOT[t]
        # Pass-through: if the user passes a root directly (e.g. "GC"), accept
        if t in PRODUCT_EXPIRY_MONTHS:
            return t
        return None

    def _front_month_contract(self, root: str, today: Optional[datetime] = None) -> str:
        """Cheap calendar-based front-month resolver. Good enough for POC.
        For production we should hit /contract/suggest for accurate roll
        timing (contracts roll a few days before the last notice date).
        """
        months = PRODUCT_EXPIRY_MONTHS.get(root)
        if not months:
            # Default to quarterly if unknown
            months = [3, 6, 9, 12]
        now = today or datetime.now(timezone.utc)
        y, m = now.year, now.month
        # Pick the next expiry month that's >= current month. Add 5-day lead
        # to avoid the last-trading-day rush — we want the LIQUID contract.
        lead_day = now.day >= 10
        candidate_month = None
        for mm in months:
            if mm > m or (mm == m and not lead_day):
                candidate_month = mm
                break
        if candidate_month is None:
            # Wrap to next year's first expiry month
            candidate_month = months[0]
            y += 1
        return f"{root}{MONTH_CODES[candidate_month]}{y % 10}"

    async def resolve_contract_via_api(self, root: str) -> Optional[str]:
        """Ask Tradovate to suggest the current tradeable contract. Used as
        a fallback/verification — if this disagrees with the calendar
        heuristic we prefer this answer."""
        if httpx is None or not self._auth.access_token:
            return None
        url = ENDPOINTS[self._config.get("env", "demo")]["rest"] + "/contract/suggest"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(
                    url, params={"t": root, "l": 5},
                    headers={"Authorization": f"Bearer {self._auth.access_token}"},
                )
            if r.status_code != 200:
                return None
            contracts = r.json() or []
            # Return the shortest-name contract (usually the front month).
            if contracts:
                contracts.sort(key=lambda c: len(c.get("name", "")))
                return contracts[0].get("name")
        except Exception as e:
            log.warning(f"contract/suggest failed for {root}: {e}")
        return None

    # ── Market-data WebSocket helpers ─────────────────────────────
    async def _ws_fetch_bars(self, contract: str, timeframe: str, count: int) -> List[Dict[str, Any]]:
        """Open a one-shot WebSocket, auth, request bars, return them."""
        if websockets is None or not self._auth.md_access_token:
            raise RuntimeError("not authenticated or websockets missing")
        env = self._config.get("env", "demo")
        url = ENDPOINTS[env]["md_ws"]

        # Tradovate WebSocket protocol: each message is a plaintext frame
        #   <endpoint>\n<id>\n\n<body>
        # Responses come as JSON arrays prefixed by "a" (for "array frames")
        # and single-letter frames "o" (open), "h" (heartbeat), "c" (close).
        def _encode(endpoint: str, msg_id: int, body: Any = "") -> str:
            body_str = json.dumps(body) if not isinstance(body, str) else body
            return f"{endpoint}\n{msg_id}\n\n{body_str}"

        bars: List[Dict[str, Any]] = []
        base_price = 0.0
        tick_size = 0.01
        tick_mult = 1.0

        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                # 1st server frame should be "o"
                open_frame = await asyncio.wait_for(ws.recv(), timeout=10.0)
                if not (isinstance(open_frame, str) and open_frame.startswith("o")):
                    raise RuntimeError(f"unexpected open frame: {open_frame!r}")

                # Authorize with MD token
                await ws.send(_encode("authorize", 1, self._auth.md_access_token))

                # Request chart
                chart_req = {
                    "symbol": contract,
                    "chartDescription": _tf_to_chart_desc(timeframe),
                    "timeRange": {"asMuchAsElements": max(1, min(int(count), 2000))},
                }
                await ws.send(_encode("md/getChart", 2, chart_req))

                # Read frames until we receive the charts packet for our id
                deadline = time.time() + 20.0
                subscription_id: Optional[int] = None
                while time.time() < deadline and len(bars) < count:
                    try:
                        frame = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        break
                    if not isinstance(frame, str):
                        continue
                    # "h" = heartbeat — send one back to stay alive
                    if frame == "h":
                        await ws.send("[]")
                        continue
                    if frame.startswith("a"):
                        try:
                            arr = json.loads(frame[1:])
                        except Exception:
                            continue
                        for item in arr or []:
                            # Response to md/getChart (id=2) returns subscription id
                            if item.get("i") == 2 and item.get("s") == 200:
                                body = item.get("d") or {}
                                subscription_id = body.get("subscriptionId") or body.get("id")
                            # Streaming chart data comes with "e": "chart"
                            if item.get("e") == "chart":
                                cd = item.get("d") or {}
                                charts = cd.get("charts") or []
                                for ch in charts:
                                    base_price = ch.get("bp", base_price)
                                    tick_size  = ch.get("ts", tick_size) or tick_size
                                    tick_mult  = ch.get("tm", tick_mult) or tick_mult
                                    raw_bars = ch.get("bars") or []
                                    for b in raw_bars:
                                        bars.append(_decode_bar(b, base_price, tick_size, tick_mult))

                # Clean up — unsubscribe if we got a subscription id
                if subscription_id is not None:
                    try:
                        await ws.send(_encode("md/cancelChart", 3, {"subscriptionId": subscription_id}))
                    except Exception:
                        pass
        except Exception as e:
            self._last_error = f"ws fetch failed: {e}"
            log.error(self._last_error)
            raise

        return bars[-count:] if len(bars) > count else bars

    async def get_bars(self, ticker: str, timeframe: str = "M15", count: int = 200) -> List[Dict[str, Any]]:
        """Main entry point. Maps ticker → contract, fetches bars via WS."""
        if not self._auth.is_valid():
            raise RuntimeError("not authenticated")
        root = self._resolve_root(ticker)
        if not root:
            raise ValueError(f"{ticker} not mapped to a Tradovate futures root")
        # Try API-based resolution first, fall back to calendar heuristic.
        contract = await self.resolve_contract_via_api(root)
        if not contract:
            contract = self._front_month_contract(root)
        bars = await self._ws_fetch_bars(contract, timeframe, count)
        return bars


# ─── Bar decoding helper ──────────────────────────────────────────
def _decode_bar(b: Dict[str, Any], base_price: float, tick_size: float, tick_mult: float) -> Dict[str, Any]:
    """Tradovate returns bars with fields encoded as offsets from a base
    price in tick units. Decode to absolute OHLC floats."""
    # Some Tradovate responses send bars already in absolute prices; others
    # send deltas. Handle both by checking if base_price + tick_size make
    # the output make sense.
    def _px(val):
        if val is None:
            return None
        # Heuristic: if val is very small (|val| < 1e6) and base_price > 0
        # and tick_size > 0, treat as offset. Otherwise treat as absolute.
        if base_price > 0 and tick_size > 0 and abs(val) < 1_000_000:
            return base_price + (val * tick_size * (tick_mult or 1))
        return float(val)

    ts = b.get("timestamp") or b.get("t")
    # timestamp can be ISO string or epoch ms
    if isinstance(ts, (int, float)):
        iso = datetime.fromtimestamp(ts / (1000 if ts > 1e11 else 1), tz=timezone.utc) \
                      .strftime("%Y-%m-%dT%H:%M:%S")
    elif isinstance(ts, str):
        iso = ts.replace("Z", "").split(".")[0]
    else:
        iso = ""

    return {
        "time":   iso,
        "open":   _px(b.get("open"))  or 0.0,
        "high":   _px(b.get("high"))  or 0.0,
        "low":    _px(b.get("low"))   or 0.0,
        "close":  _px(b.get("close")) or 0.0,
        "volume": int(b.get("upVolume", 0) or 0) + int(b.get("downVolume", 0) or 0)
                  or int(b.get("volume", 0) or 0),
    }


# ─── Singleton accessor (one provider instance per process) ─────
_instance: Optional[TradovateProvider] = None

def get_tradovate_provider(config: Optional[Dict[str, Any]] = None) -> TradovateProvider:
    global _instance
    if _instance is None:
        _instance = TradovateProvider(config or {})
    elif config:
        _instance.update_config(config)
    return _instance
