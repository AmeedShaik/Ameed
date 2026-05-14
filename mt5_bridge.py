"""
MT5 -> Browser bridge for the MTF RSI Confluence dashboard.

Reads OHLCV bars from a running MetaTrader 5 terminal and re-exposes them
over HTTP + WebSocket using the same payload shape as Binance, so the
existing dashboard JavaScript (fetchKlines / ws.onmessage) works unchanged.

Endpoints
---------
GET  /                                         -> serves mtf_dashboard.html
GET  /api/klines?symbol=XAUUSD&interval=1m     -> Binance-shaped array
WS   /ws/{symbol}/{interval}                   -> { k: {t,o,h,l,c,v,x} } frames

Run
---
1. Open MT5 and log in to your broker (must stay open).
2. pip install -r requirements.txt
3. python mt5_bridge.py
4. Open http://localhost:8000 in your browser.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import Dict, List, Optional

import MetaTrader5 as mt5
import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HOST = "127.0.0.1"
PORT = 8000
POLL_SECONDS = 1.0          # how often the bridge polls MT5 for new bars
DEFAULT_LIMIT = 200          # bars to return on warm-up
DASHBOARD_FILE = Path(__file__).with_name("mtf_dashboard.html")

# Maps the dashboard's interval codes to MT5 timeframe constants.
TF_MAP = {
    "1m":  mt5.TIMEFRAME_M1,
    "5m":  mt5.TIMEFRAME_M5,
    "15m": mt5.TIMEFRAME_M15,
    "1h":  mt5.TIMEFRAME_H1,
    "4h":  mt5.TIMEFRAME_H4,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("mt5-bridge")

# ---------------------------------------------------------------------------
# MT5 lifecycle
# ---------------------------------------------------------------------------
def mt5_init() -> None:
    """Attach to the running MT5 terminal. Raises on failure."""
    if not mt5.initialize():
        raise RuntimeError(
            f"mt5.initialize() failed: {mt5.last_error()} "
            "(is the MT5 terminal open and logged in?)"
        )
    info = mt5.terminal_info()
    acct = mt5.account_info()
    log.info("MT5 connected: terminal=%s build=%s account=%s server=%s",
             getattr(info, "name", "?"),
             getattr(info, "build", "?"),
             getattr(acct, "login", "?"),
             getattr(acct, "server", "?"))


def mt5_shutdown() -> None:
    with contextlib.suppress(Exception):
        mt5.shutdown()


def ensure_symbol(symbol: str) -> None:
    """Make sure the symbol is visible in Market Watch so rates are available."""
    info = mt5.symbol_info(symbol)
    if info is None:
        raise HTTPException(404, f"Unknown MT5 symbol: {symbol!r}")
    if not info.visible:
        if not mt5.symbol_select(symbol, True):
            raise HTTPException(500, f"Could not select symbol {symbol!r} in Market Watch")


# ---------------------------------------------------------------------------
# Bar fetching (sync MT5 calls run in a worker thread)
# ---------------------------------------------------------------------------
def _fetch_bars_sync(symbol: str, interval: str, limit: int) -> List[dict]:
    if interval not in TF_MAP:
        raise HTTPException(400, f"Unsupported interval: {interval!r}")
    ensure_symbol(symbol)
    rates = mt5.copy_rates_from_pos(symbol, TF_MAP[interval], 0, limit)
    if rates is None or len(rates) == 0:
        err = mt5.last_error()
        raise HTTPException(502, f"copy_rates_from_pos returned no data: {err}")

    out = []
    for r in rates:
        # MT5 'time' is bar OPEN time in seconds (broker time).
        # tick_volume is the closest analogue to Binance volume for FX/CFDs.
        out.append({
            "t": int(r["time"]) * 1000,
            "o": float(r["open"]),
            "h": float(r["high"]),
            "l": float(r["low"]),
            "c": float(r["close"]),
            "v": float(r["tick_volume"]),
        })
    return out


async def fetch_bars(symbol: str, interval: str, limit: int) -> List[dict]:
    return await asyncio.to_thread(_fetch_bars_sync, symbol, interval, limit)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="MT5 Dashboard Bridge")


@app.on_event("startup")
def _startup() -> None:
    mt5_init()


@app.on_event("shutdown")
def _shutdown() -> None:
    mt5_shutdown()


@app.get("/")
def index() -> FileResponse:
    if not DASHBOARD_FILE.exists():
        raise HTTPException(404, f"Dashboard file not found: {DASHBOARD_FILE}")
    return FileResponse(DASHBOARD_FILE)


@app.get("/api/health")
def health() -> dict:
    info = mt5.terminal_info()
    return {
        "ok": info is not None,
        "connected": getattr(info, "connected", False),
        "build": getattr(info, "build", None),
    }


@app.get("/api/symbols")
def symbols() -> JSONResponse:
    """Optional helper: list visible MT5 symbols so you can find broker names."""
    syms = mt5.symbols_get() or []
    return JSONResponse([s.name for s in syms])


@app.get("/api/klines")
async def klines(symbol: str, interval: str, limit: int = DEFAULT_LIMIT) -> JSONResponse:
    limit = max(1, min(limit, 1000))
    bars = await fetch_bars(symbol, interval, limit)
    return JSONResponse(bars)


# ---------------------------------------------------------------------------
# WebSocket: poll MT5 once per second and emit Binance-shaped kline frames
# ---------------------------------------------------------------------------
@app.websocket("/ws/{symbol}/{interval}")
async def ws_kline(ws: WebSocket, symbol: str, interval: str) -> None:
    await ws.accept()
    if interval not in TF_MAP:
        await ws.close(code=1003, reason=f"bad interval: {interval}")
        return

    log.info("WS open: %s @ %s", symbol, interval)
    last_open_t: Optional[int] = None
    last_bar: Optional[dict] = None

    try:
        while True:
            try:
                # Pull the two most recent bars: [previous_closed, current_in_progress]
                bars = await fetch_bars(symbol, interval, 2)
            except HTTPException as e:
                await ws.send_json({"error": e.detail})
                await asyncio.sleep(POLL_SECONDS)
                continue

            if not bars:
                await asyncio.sleep(POLL_SECONDS)
                continue

            current = bars[-1]
            previous = bars[-2] if len(bars) >= 2 else None

            # Detect bar rollover: emit the previous bar with x=true (closed)
            # exactly once, so the dashboard finalizes it before moving on.
            if last_open_t is not None and current["t"] > last_open_t and previous:
                if previous["t"] == last_open_t:
                    await _emit(ws, symbol, interval, previous, closed=True)

            # Emit current in-progress bar on every tick.
            await _emit(ws, symbol, interval, current, closed=False)

            last_open_t = current["t"]
            last_bar = current
            await asyncio.sleep(POLL_SECONDS)

    except WebSocketDisconnect:
        log.info("WS closed: %s @ %s", symbol, interval)
    except Exception as e:
        log.exception("WS error %s @ %s: %s", symbol, interval, e)
        with contextlib.suppress(Exception):
            await ws.close()


async def _emit(ws: WebSocket, symbol: str, interval: str, bar: dict, closed: bool) -> None:
    """Send a frame matching Binance's @kline payload shape (only fields the dashboard reads)."""
    await ws.send_json({
        "e": "kline",
        "s": symbol,
        "k": {
            "t": bar["t"],
            "i": interval,
            "o": str(bar["o"]),
            "h": str(bar["h"]),
            "l": str(bar["l"]),
            "c": str(bar["c"]),
            "v": str(bar["v"]),
            "x": closed,
        },
    })


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
