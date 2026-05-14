"""
MT5 -> Browser bridge for the MTF RSI Confluence dashboard.

Reads OHLCV bars from a running MetaTrader 5 terminal and re-exposes them
over HTTP + WebSocket using the same payload shape as Binance, so the
existing dashboard JavaScript (fetchKlines / ws.onmessage) works unchanged.

It also exposes read-only account + positions endpoints, OPTIONALLY
order placement when started with --enable-trading, and an OPTIONAL
trailing-SL background task when started with --trailing-sl.

Endpoints
---------
GET  /                                         -> serves mtf_dashboard.html
GET  /api/health                               -> connection + flags
GET  /api/symbols                              -> list visible MT5 symbols
GET  /api/symbol_info?symbol=XAUUSD            -> point, digits, tick value, etc.
GET  /api/klines?symbol=XAUUSD&interval=1m     -> Binance-shaped array
GET  /api/account                              -> balance, equity, margin, etc.
GET  /api/positions?symbol=XAUUSD              -> open positions (filtered)
POST /api/order      [trading]                 -> place market order
POST /api/close/{ticket} [trading]             -> close one position
POST /api/modify_sl  [trading]                 -> move SL/TP on a position
WS   /ws/{symbol}/{interval}                   -> { k: {t,o,h,l,c,v,x} } frames

Run
---
1. Open MT5 and log in to your broker (must stay open).
2. pip install -r requirements.txt
3. Pick a mode:
     python mt5_bridge.py                                # read-only
     python mt5_bridge.py --enable-trading               # manual orders allowed
     python mt5_bridge.py --enable-trading --trailing-sl # also auto-trail SL
4. Open http://localhost:8000 in your browser.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
from pathlib import Path
from typing import Dict, List, Optional

import MetaTrader5 as mt5
import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HOST = "127.0.0.1"
PORT = 8000
POLL_SECONDS = 1.0          # how often the bridge polls MT5 for new bars
DEFAULT_LIMIT = 200          # bars to return on warm-up
DASHBOARD_FILE = Path(__file__).with_name("mtf_dashboard.html")
MAGIC = 90210                # tags trades placed by the dashboard
DEVIATION = 20               # max price slippage (points)

# Trailing-SL parameters (only used when --trailing-sl is set).
TRAIL_POLL_SECONDS = 5.0     # how often to evaluate trailing SL
TRAIL_ATR_MULT = 2.0         # trail at price - 2.0 * ATR(M5,14) for longs
TRAIL_MIN_STEP_ATR = 0.5     # only update SL if new SL is >=0.5 ATR better

# Set by CLI args at startup. Defaults to safe (read-only).
TRADING_ENABLED = False
TRAILING_ENABLED = False

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
    if acct is not None:
        log.info("Account currency=%s balance=%.2f leverage=1:%d",
                 acct.currency, acct.balance, acct.leverage)
    log.warning(
        "TRADING %s. Start with --enable-trading to allow order placement.",
        "ENABLED" if TRADING_ENABLED else "DISABLED (read-only)",
    )
    if TRAILING_ENABLED:
        log.warning("TRAILING-SL ENABLED. Will trail magic=%d positions.", MAGIC)


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


def pick_filling_mode(symbol: str) -> int:
    """Choose an order filling mode the broker accepts for this symbol."""
    info = mt5.symbol_info(symbol)
    if info is None:
        return mt5.ORDER_FILLING_FOK
    fm = info.filling_mode
    # bitmask: 1=FOK, 2=IOC. RETURN is always allowed as fallback.
    if fm & 1:
        return mt5.ORDER_FILLING_FOK
    if fm & 2:
        return mt5.ORDER_FILLING_IOC
    return mt5.ORDER_FILLING_RETURN


def require_trading() -> None:
    if not TRADING_ENABLED:
        raise HTTPException(
            403,
            "Trading is disabled. Restart bridge with --enable-trading to allow.",
        )


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


def _atr_from_rates(symbol: str, timeframe: int, period: int = 14) -> Optional[float]:
    """Wilder ATR over `period` bars on the given MT5 timeframe."""
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, period + 50)
    if rates is None or len(rates) < period + 1:
        return None
    trs = []
    for i in range(1, len(rates)):
        h, l = float(rates[i]["high"]), float(rates[i]["low"])
        pc = float(rates[i - 1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="MT5 Dashboard Bridge")


@app.on_event("startup")
async def _startup() -> None:
    mt5_init()
    if TRAILING_ENABLED:
        asyncio.create_task(_trailing_sl_loop())


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
    acct = mt5.account_info()
    return {
        "ok": info is not None,
        "connected": getattr(info, "connected", False),
        "build": getattr(info, "build", None),
        "trading_enabled": TRADING_ENABLED,
        "trailing_enabled": TRAILING_ENABLED,
        "magic": MAGIC,
        "account": {
            "login": getattr(acct, "login", None),
            "server": getattr(acct, "server", None),
            "currency": getattr(acct, "currency", None),
            "leverage": getattr(acct, "leverage", None),
            "trade_allowed": getattr(acct, "trade_allowed", False),
        } if acct else None,
    }


@app.get("/api/symbols")
def symbols() -> JSONResponse:
    """Optional helper: list visible MT5 symbols so you can find broker names."""
    syms = mt5.symbols_get() or []
    return JSONResponse([s.name for s in syms])


@app.get("/api/symbol_info")
def symbol_info(symbol: str) -> dict:
    """Expose the fields the dashboard needs for lot sizing."""
    ensure_symbol(symbol)
    info = mt5.symbol_info(symbol)
    return {
        "name": info.name,
        "digits": info.digits,
        "point": info.point,
        "trade_tick_value": info.trade_tick_value,   # value of 1 tick in account ccy
        "trade_tick_size": info.trade_tick_size,     # price size of 1 tick
        "volume_min": info.volume_min,
        "volume_max": info.volume_max,
        "volume_step": info.volume_step,
        "trade_contract_size": info.trade_contract_size,
        "trade_stops_level": info.trade_stops_level, # min SL/TP distance in points
    }


@app.get("/api/klines")
async def klines(symbol: str, interval: str, limit: int = DEFAULT_LIMIT) -> JSONResponse:
    limit = max(1, min(limit, 1000))
    bars = await fetch_bars(symbol, interval, limit)
    return JSONResponse(bars)


# ---------------------------------------------------------------------------
# Account + positions (read-only)
# ---------------------------------------------------------------------------
@app.get("/api/account")
def account() -> dict:
    acct = mt5.account_info()
    if acct is None:
        raise HTTPException(502, "account_info() returned None")
    return {
        "login": acct.login,
        "currency": acct.currency,
        "leverage": acct.leverage,
        "balance": acct.balance,
        "equity": acct.equity,
        "margin": acct.margin,
        "margin_free": acct.margin_free,
        "margin_level": acct.margin_level,
        "profit": acct.profit,
        "trade_allowed": acct.trade_allowed,
    }


@app.get("/api/positions")
def positions(symbol: Optional[str] = None) -> JSONResponse:
    raw = mt5.positions_get(symbol=symbol) if symbol else mt5.positions_get()
    raw = raw or ()
    out = []
    for p in raw:
        out.append({
            "ticket":     p.ticket,
            "symbol":     p.symbol,
            "type":       "BUY" if p.type == mt5.ORDER_TYPE_BUY else "SELL",
            "volume":     p.volume,
            "price_open": p.price_open,
            "sl":         p.sl,
            "tp":         p.tp,
            "price_current": p.price_current,
            "profit":     p.profit,
            "swap":       p.swap,
            "magic":      p.magic,
            "comment":    p.comment,
            "time":       int(p.time) * 1000,
        })
    return JSONResponse(out)


# ---------------------------------------------------------------------------
# Order placement (gated by --enable-trading)
# ---------------------------------------------------------------------------
class OrderRequest(BaseModel):
    symbol: str
    side: str = Field(..., pattern="^(BUY|SELL)$")
    volume: float = Field(..., gt=0)
    sl: Optional[float] = None
    tp: Optional[float] = None
    comment: str = "kiro-mtf"


class ModifySLRequest(BaseModel):
    ticket: int
    sl: Optional[float] = None
    tp: Optional[float] = None


@app.post("/api/order")
def place_order(req: OrderRequest) -> dict:
    require_trading()
    ensure_symbol(req.symbol)

    tick = mt5.symbol_info_tick(req.symbol)
    if tick is None:
        raise HTTPException(502, "symbol_info_tick returned None")

    is_buy = req.side == "BUY"
    order_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
    price = tick.ask if is_buy else tick.bid

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": req.symbol,
        "volume": float(req.volume),
        "type": order_type,
        "price": float(price),
        "deviation": DEVIATION,
        "magic": MAGIC,
        "comment": req.comment[:31],          # MT5 caps comments at ~31 chars
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": pick_filling_mode(req.symbol),
    }
    if req.sl is not None:
        request["sl"] = float(req.sl)
    if req.tp is not None:
        request["tp"] = float(req.tp)

    log.info("ORDER >> %s", request)
    result = mt5.order_send(request)
    if result is None:
        raise HTTPException(502, f"order_send returned None: {mt5.last_error()}")

    payload = result._asdict()
    log.info("ORDER << retcode=%s deal=%s order=%s comment=%s",
             payload.get("retcode"), payload.get("deal"),
             payload.get("order"), payload.get("comment"))

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        # Surface the broker's reason verbatim so the UI can show it.
        raise HTTPException(
            502,
            f"Order rejected (retcode={result.retcode}): {payload.get('comment')}",
        )
    return payload


@app.post("/api/close/{ticket}")
def close_position(ticket: int) -> dict:
    require_trading()
    matches = mt5.positions_get(ticket=ticket) or ()
    if not matches:
        raise HTTPException(404, f"No open position with ticket {ticket}")
    pos = matches[0]
    tick = mt5.symbol_info_tick(pos.symbol)
    if tick is None:
        raise HTTPException(502, "symbol_info_tick returned None")

    # Opposite side closes the position.
    closing_buy = pos.type == mt5.ORDER_TYPE_SELL
    close_type = mt5.ORDER_TYPE_BUY if closing_buy else mt5.ORDER_TYPE_SELL
    price = tick.ask if closing_buy else tick.bid

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": pos.symbol,
        "volume": pos.volume,
        "type": close_type,
        "position": ticket,
        "price": float(price),
        "deviation": DEVIATION,
        "magic": pos.magic or MAGIC,
        "comment": "kiro-close",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": pick_filling_mode(pos.symbol),
    }
    log.info("CLOSE >> ticket=%s %s", ticket, request)
    result = mt5.order_send(request)
    if result is None:
        raise HTTPException(502, f"order_send returned None: {mt5.last_error()}")

    payload = result._asdict()
    log.info("CLOSE << retcode=%s comment=%s",
             payload.get("retcode"), payload.get("comment"))

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        raise HTTPException(
            502,
            f"Close rejected (retcode={result.retcode}): {payload.get('comment')}",
        )
    return payload


@app.post("/api/modify_sl")
def modify_sl(req: ModifySLRequest) -> dict:
    """Move SL and/or TP on an existing position. Used by trailing-SL loop and by manual UI."""
    require_trading()
    matches = mt5.positions_get(ticket=req.ticket) or ()
    if not matches:
        raise HTTPException(404, f"No open position with ticket {req.ticket}")
    pos = matches[0]

    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "position": req.ticket,
        "symbol": pos.symbol,
        "sl": float(req.sl) if req.sl is not None else float(pos.sl),
        "tp": float(req.tp) if req.tp is not None else float(pos.tp),
        "magic": pos.magic or MAGIC,
    }
    log.info("MODIFY-SL >> %s", request)
    result = mt5.order_send(request)
    if result is None:
        raise HTTPException(502, f"order_send returned None: {mt5.last_error()}")

    payload = result._asdict()
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        raise HTTPException(
            502,
            f"Modify-SL rejected (retcode={result.retcode}): {payload.get('comment')}",
        )
    log.info("MODIFY-SL << ticket=%s new SL=%s TP=%s", req.ticket, request["sl"], request["tp"])
    return payload


# ---------------------------------------------------------------------------
# Trailing-SL background task (only runs if --trailing-sl is set)
# ---------------------------------------------------------------------------
def _trail_target_sl(pos, atr5: float, info) -> Optional[float]:
    """
    Compute the candidate new SL for a position.
    Trails at price - TRAIL_ATR_MULT*ATR (long) or price + TRAIL_ATR_MULT*ATR (short).
    Returns None if the new SL would not be strictly better than the current SL.
    """
    if atr5 is None or atr5 <= 0:
        return None
    digits = info.digits
    is_buy = pos.type == mt5.ORDER_TYPE_BUY

    # Use mid price as anchor; current_price would also work.
    price = pos.price_current
    if is_buy:
        candidate = round(price - TRAIL_ATR_MULT * atr5, digits)
        # Long: only raise SL, never lower it. Also must be above entry-eps to lock profit.
        if pos.sl and candidate <= pos.sl:
            return None
        # Require new SL to be at least TRAIL_MIN_STEP_ATR closer than the old one.
        if pos.sl and (candidate - pos.sl) < TRAIL_MIN_STEP_ATR * atr5:
            return None
        return candidate
    else:
        candidate = round(price + TRAIL_ATR_MULT * atr5, digits)
        # Short: only lower SL.
        if pos.sl and candidate >= pos.sl:
            return None
        if pos.sl and (pos.sl - candidate) < TRAIL_MIN_STEP_ATR * atr5:
            return None
        return candidate


def _trailing_sl_tick_sync() -> List[dict]:
    """Run one trailing-SL evaluation pass. Returns list of actions taken."""
    actions: List[dict] = []
    raw = mt5.positions_get() or ()
    for pos in raw:
        # Only touch positions we placed (magic) and that have an existing SL.
        if pos.magic != MAGIC or not pos.sl:
            continue
        info = mt5.symbol_info(pos.symbol)
        if info is None:
            continue
        atr5 = _atr_from_rates(pos.symbol, mt5.TIMEFRAME_M5, 14)
        if atr5 is None:
            continue
        new_sl = _trail_target_sl(pos, atr5, info)
        if new_sl is None:
            continue
        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "position": pos.ticket,
            "symbol": pos.symbol,
            "sl": new_sl,
            "tp": float(pos.tp),
            "magic": pos.magic,
        }
        result = mt5.order_send(request)
        ok = result is not None and result.retcode == mt5.TRADE_RETCODE_DONE
        actions.append({
            "ticket": pos.ticket,
            "symbol": pos.symbol,
            "old_sl": pos.sl,
            "new_sl": new_sl,
            "ok": ok,
            "comment": getattr(result, "comment", "no result") if result else "send-failed",
        })
        if ok:
            log.info("TRAIL %s #%s SL %.5f -> %.5f", pos.symbol, pos.ticket, pos.sl, new_sl)
        else:
            log.warning("TRAIL %s #%s rejected: %s", pos.symbol, pos.ticket,
                        getattr(result, "comment", mt5.last_error()))
    return actions


async def _trailing_sl_loop() -> None:
    log.info("Trailing-SL loop started (poll=%.1fs, atr_mult=%.1f, min_step=%.2f ATR)",
             TRAIL_POLL_SECONDS, TRAIL_ATR_MULT, TRAIL_MIN_STEP_ATR)
    while True:
        try:
            await asyncio.to_thread(_trailing_sl_tick_sync)
        except Exception as e:
            log.exception("Trailing-SL tick error: %s", e)
        await asyncio.sleep(TRAIL_POLL_SECONDS)


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
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MT5 -> Browser bridge")
    p.add_argument("--host", default=HOST, help="Bind host (default 127.0.0.1)")
    p.add_argument("--port", type=int, default=PORT, help="Bind port (default 8000)")
    p.add_argument("--enable-trading", action="store_true",
                   help="Allow order placement endpoints (default: read-only)")
    p.add_argument("--trailing-sl", action="store_true",
                   help="Run background trailing-SL loop (only on magic=%d positions). "
                        "Requires --enable-trading." % MAGIC)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    TRADING_ENABLED = args.enable_trading
    TRAILING_ENABLED = args.trailing_sl and TRADING_ENABLED
    if args.trailing_sl and not TRADING_ENABLED:
        log.warning("--trailing-sl ignored: requires --enable-trading")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
