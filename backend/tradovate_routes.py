# version: v1
"""
tradovate_routes.py — HTTP API shell for the Tradovate POC.

Routes mirror the MT5 management routes so the SettingsPanel can drive
both providers with the same UX pattern. Tradovate state is held in the
singleton TradovateProvider from providers/tradovate_provider.py.

    GET    /api/tradovate/config       → read saved creds + status
    PATCH  /api/tradovate/config       → save creds (env, app_id, cid, sec,
                                          username, password)
    POST   /api/tradovate/connect      → authenticate with current creds
    POST   /api/tradovate/disconnect   → clear tokens
    GET    /api/tradovate/bars/{ticker}?timeframe=&count=   → fetch bars

Reversibility: this module is standalone. Nothing in MT5 or the base
terminal references it.  Dropping the include_router() line from
consumer_startup.py removes every route above in one edit.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from providers.tradovate_provider import get_tradovate_provider

log = logging.getLogger("tradovate_routes")


class TradovateConfigBody(BaseModel):
    env:        Optional[str] = None   # "demo" | "live"
    app_id:     Optional[str] = None
    app_version:Optional[str] = None
    cid:        Optional[str] = None
    sec:        Optional[str] = None
    username:   Optional[str] = None
    password:   Optional[str] = None
    device_id:  Optional[str] = None


def create_tradovate_router(cfg_manager=None) -> APIRouter:
    router = APIRouter(tags=["tradovate"])

    def _cfg_store() -> dict:
        """Slice of user_config where Tradovate creds live. Uses
        cfg_manager's internal _user_config dict (same pattern mt5_routes
        uses) so edits persist to disk via _save_user_config()."""
        if cfg_manager is None or not hasattr(cfg_manager, "_user_config"):
            # Fallback: in-memory only (creds lost on restart)
            if not hasattr(_cfg_store, "_mem"):
                _cfg_store._mem = {}
            store = _cfg_store._mem
        else:
            store = cfg_manager._user_config
        providers = store.setdefault("providers", {}) if isinstance(store, dict) else {}
        accounts  = providers.setdefault("accounts", {}) if isinstance(providers, dict) else {}
        tv        = accounts.setdefault("tradovate_default", {
            "id": "tradovate_default",
            "type": "tradovate",
            "label": "Tradovate",
            "enabled": False,
            "env": "demo",
            "app_id": "",
            "app_version": "1.0",
            "cid": "",
            "sec": "",
            "username": "",
            "password": "",
            "device_id": "QuantumTerminal-consumer-poc",
        })
        return tv

    def _masked(cfg: dict) -> dict:
        """Never return secrets to the UI — mask them so we don't leak."""
        out = dict(cfg or {})
        for k in ("sec", "password"):
            if out.get(k):
                out[k] = "••••••"
        return out

    # ── GET /api/tradovate/config ──────────────────────────────
    @router.get("/api/tradovate/config")
    async def get_config():
        cfg = _cfg_store()
        prov = get_tradovate_provider(cfg)
        return {"config": _masked(cfg), "status": prov.status_dict}

    # ── PATCH /api/tradovate/config ────────────────────────────
    @router.patch("/api/tradovate/config")
    async def patch_config(body: TradovateConfigBody):
        cfg = _cfg_store()
        updates = {k: v for k, v in body.model_dump().items() if v is not None and v != ""}
        cfg.update(updates)
        # Persist via the same internal save hook config_manager uses.
        try:
            if cfg_manager and hasattr(cfg_manager, "_save_user_config"):
                cfg_manager._save_user_config()
        except Exception as e:
            log.warning(f"_save_user_config failed: {e}")
        # Push new values into the live provider instance (clears token cache)
        get_tradovate_provider().update_config(cfg)
        return {"config": _masked(cfg), "status": get_tradovate_provider().status_dict}

    # ── POST /api/tradovate/connect ────────────────────────────
    @router.post("/api/tradovate/connect")
    async def connect():
        cfg = _cfg_store()
        prov = get_tradovate_provider(cfg)
        result = await prov.authenticate()
        return result

    # ── POST /api/tradovate/disconnect ─────────────────────────
    @router.post("/api/tradovate/disconnect")
    async def disconnect():
        get_tradovate_provider().disconnect()
        return {"success": True, "status": get_tradovate_provider().status_dict}

    # ── GET /api/tradovate/bars/{ticker} ───────────────────────
    @router.get("/api/tradovate/bars/{ticker}")
    async def get_bars(ticker: str, timeframe: str = "M15", count: int = 200):
        prov = get_tradovate_provider()
        if not prov.connected:
            raise HTTPException(503, {"error": "tradovate_not_connected",
                                       "message": "Connect to Tradovate in Settings → Providers first."})
        try:
            bars = await prov.get_bars(ticker, timeframe, count)
        except ValueError as e:
            # Unmapped ticker — clean 404
            raise HTTPException(404, {"error": "symbol_not_supported",
                                       "message": str(e)})
        except Exception as e:
            raise HTTPException(500, {"error": "tradovate_bars_failed",
                                       "message": str(e)})
        return {
            "ticker":    ticker.upper(),
            "timeframe": timeframe,
            "bars":      bars,
            "delayed":   prov.is_delayed,
            "count":     len(bars),
        }

    return router
