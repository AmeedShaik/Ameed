# ─────────────────────────────────────────────────────────────────
#  mt5_auto_trader.py
#  Live auto-trader that ports the KNN/ANN backtest dashboard's exact
#  pipeline (VWAP deviation + Volume Profile POC/VAH/VAL + OHLCV features,
#  KNN classifier, 7→12→3 ANN) to Python + MT5, and trades BOTH models
#  live as two independent, separately-tagged position streams.
#
#  Requires: pip install MetaTrader5 numpy pandas
#
#  SAFETY:
#    - DRY_RUN=True by default. No orders are sent until you flip it off.
#    - ACCOUNT_MODE_CHECK refuses to trade if MT5 reports a live (real)
#      account, unless you explicitly set ALLOW_LIVE=True.
#    - Only ever holds ONE open position per model at a time (matches the
#      backtest engine's pos==0 gate before opening a new trade).
#
#  Run:
#    python mt5_auto_trader.py
#  Logs every decision to stdout and appends every fill to trade_log.csv
#  (same "unified CSV" pattern as your other MT5 bots).
# ─────────────────────────────────────────────────────────────────

import time
import csv
import os
import json
import random
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import numpy as np
import pandas as pd
import MetaTrader5 as mt5

# ═══════════════════════════════════════
# CONFIG — mirror the dashboard controls that produced your good run
# ═══════════════════════════════════════
SYMBOL          = 'XAUUSD'
TIMEFRAME_STR   = 'H4'          # must match TF_MAP below
KNN_K           = 13            # <-- dashboard: KNN K = 13
ANN_EPOCHS      = 200           # unused while TRADE_KNN_ONLY=True, kept for parity/future use
FWD_BARS        = 7             # <-- dashboard: FWD BARS = 7 (forward-return label horizon, in bars)
THRESHOLD       = 0.0010        # <-- dashboard: THRESHOLD = 0.10% fwd-return label threshold
COST_PCT        = 0.0005        # 0.05% round-trip, informational only (broker applies real spread/commission)
STOP_ATR_MULT   = 2             # <-- dashboard: STOP ATR× = 2
TP_ATR_MULT     = 3             # <-- dashboard: TP ATR× = 3
ATR_PERIOD      = 14
LOT_SIZE        = 0.10          # <-- dashboard: LOT SIZE = 0.10
DEVIATION       = 20            # max price slippage, points
HISTORY_BARS    = 500           # bars pulled each cycle for feature/train context (matches CANDLES=500)
WARMUP          = 50            # bars burned by VWAP/volume-profile lookback, same as buildDataset()
RETRAIN_EVERY_BARS = 30         # retrain every N new closed bars (~5 days on H4)
POLL_SECONDS    = 30            # how often to check for a newly closed bar

TRADE_KNN_ONLY  = True          # <-- only KNN opens/manages live positions; ANN is not traded

BRIDGE_ENABLED  = True           # <-- serve the HTTP bridge the dashboard's Auto Trade panel talks to
BRIDGE_HOST     = '0.0.0.0'
BRIDGE_PORT     = 7000           # <-- must match atBridgeUrlInput / MT5_BRIDGE_URL in the dashboard

KNN_MAGIC = 990101
ANN_MAGIC = 990102
KNN_TAG   = 'Ameed_KNN'   # <-- comment tag on every KNN order/close/partial
ANN_TAG   = 'ANN_LIVE'

DRY_RUN     = False     # <-- flip to False only after you've watched it decide correctly on demo
ALLOW_LIVE  = True    # <-- must be True AND DRY_RUN False to ever trade a real-money account

# ═══════════════════════════════════════
# TRAILING STOP + PARTIAL CLOSE
# Runs every poll cycle (POLL_SECONDS), independent of the bar-close signal
# logic above — profit protection shouldn't wait for the next candle.
# ═══════════════════════════════════════
TRAILING_SL_ENABLED      = True
TRAIL_ACTIVATE_ATR_MULT  = 1.0   # start trailing once price is this many ATRs in profit
TRAIL_DISTANCE_ATR_MULT  = 1.0   # keep the trailing SL this many ATRs behind current price

PARTIAL_CLOSE_ENABLED    = True
PARTIAL_CLOSE_TRIGGER_USD = 50.0  # take partial profit once floating P/L reaches this many $
PARTIAL_CLOSE_LOT        = 0.05   # fixed lot size to close at trigger (not a %)
MOVE_SL_TO_BE_ON_PARTIAL = True   # after partial close, move remaining SL to breakeven
                                   # remaining lot's TP is left untouched — it still rides to the original TP

IMMEDIATE_REENTRY_ENABLED = True  # once a model is FLAT (stopped out — whether by the original SL,
                                   # the breakeven SL set after a partial close, or it simply hit TP)
                                   # recompute its signal every poll cycle instead of waiting for the
                                   # next bar close, and re-enter as soon as a fresh directional signal
                                   # appears.

LOG_FILE = 'trade_log.csv'

TF_MAP = {
    'M1': mt5.TIMEFRAME_M1, 'M5': mt5.TIMEFRAME_M5, 'M15': mt5.TIMEFRAME_M15,
    'M30': mt5.TIMEFRAME_M30, 'H1': mt5.TIMEFRAME_H1, 'H4': mt5.TIMEFRAME_H4,
    'D1': mt5.TIMEFRAME_D1, 'W1': mt5.TIMEFRAME_W1,
}
TIMEFRAME = TF_MAP[TIMEFRAME_STR]


def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}", flush=True)


def log_trade_csv(row):
    exists = os.path.isfile(LOG_FILE)
    with open(LOG_FILE, 'a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)


# ═══════════════════════════════════════
# MT5 CONNECTION + SAFETY GATE
# ═══════════════════════════════════════
def connect():
    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize() failed: {mt5.last_error()}")
    acc = mt5.account_info()
    if acc is None:
        raise RuntimeError("Could not read account_info() — check MT5 terminal is logged in.")
    is_demo = acc.trade_mode == mt5.ACCOUNT_TRADE_MODE_DEMO
    log(f"Connected: login={acc.login} server={acc.server} balance={acc.balance} "
        f"mode={'DEMO' if is_demo else 'LIVE/CONTEST'}")
    if not is_demo and not ALLOW_LIVE:
        raise RuntimeError(
            "This account is NOT a demo account and ALLOW_LIVE=False. "
            "Refusing to run — switch to a demo account or explicitly set ALLOW_LIVE=True."
        )
    if DRY_RUN:
        log("DRY_RUN=True — decisions will be logged, NO orders will be sent.")
    return acc


# ═══════════════════════════════════════
# DATA FETCH
# ═══════════════════════════════════════
def fetch_candles(n=HISTORY_BARS):
    rates = mt5.copy_rates_from_pos(SYMBOL, TIMEFRAME, 0, n)
    if rates is None or len(rates) == 0:
        raise RuntimeError(f"copy_rates_from_pos failed: {mt5.last_error()}")
    df = pd.DataFrame(rates)
    df['tp'] = (df['high'] + df['low'] + df['close']) / 3
    df = df.rename(columns={'tick_volume': 'volume'})
    return df.reset_index(drop=True)


# ═══════════════════════════════════════
# FEATURE ENGINEERING — 1:1 port of the dashboard's JS
# ═══════════════════════════════════════
def vwap(df, idx, period):
    s = max(0, idx - period + 1)
    w = df.iloc[s:idx+1]
    tpv = (w['tp'] * w['volume']).sum()
    v = w['volume'].sum()
    return tpv / v if v > 0 else df['close'].iloc[idx]


def volume_profile(df, idx, lookback=50, bins=20):
    s = max(0, idx - lookback + 1)
    w = df.iloc[s:idx+1]
    lo, hi = w['low'].min(), w['high'].max()
    if hi <= lo:
        c = df['close'].iloc[idx]
        return c, df['high'].iloc[idx], df['low'].iloc[idx]
    bs = (hi - lo) / bins
    vb = np.zeros(bins)
    for _, c in w.iterrows():
        bl = max(0, int((c['low'] - lo) / bs))
        bh = min(bins - 1, int((c['high'] - lo) / bs))
        sp = bh - bl + 1
        for b in range(bl, bh + 1):
            vb[b] += c['volume'] / sp
    poc_bin = int(np.argmax(vb))
    poc = lo + (poc_bin + 0.5) * bs
    total = vb.sum()
    cum = vb[poc_bin]
    up = dn = poc_bin
    while cum < total * 0.7:
        uv = vb[up + 1] if up + 1 < bins else -np.inf
        dv = vb[dn - 1] if dn - 1 >= 0 else -np.inf
        if uv == -np.inf and dv == -np.inf:
            break
        if uv >= dv:
            up += 1; cum += vb[up]
        else:
            dn -= 1; cum += vb[dn]
    return poc, lo + (up + 1) * bs, lo + dn * bs


def avg_vol(df, idx, period=20):
    s = max(0, idx - period + 1)
    return df['volume'].iloc[s:idx+1].mean()


def build_atr_series(df, period=ATR_PERIOD):
    high, low, close = df['high'].values, df['low'].values, df['close'].values
    n = len(df)
    tr = np.zeros(n)
    for i in range(n):
        if i == 0:
            tr[i] = high[i] - low[i]
        else:
            pc = close[i-1]
            tr[i] = max(high[i]-low[i], abs(high[i]-pc), abs(low[i]-pc))
    atr = np.zeros(n)
    for i in range(n):
        s = max(0, i - period + 1)
        atr[i] = tr[s:i+1].mean()
    return atr


def build_dataset(df, fwd=FWD_BARS, thresh=THRESHOLD, warmup=WARMUP):
    features, labels, idxs = [], [], []
    n = len(df)
    for i in range(warmup, n - fwd):
        c = df.iloc[i]
        vw = vwap(df, i, 24)
        poc, vah, val = volume_profile(df, i, 50, 20)
        av = avg_vol(df, i, 20)
        feat = [
            (c['close'] - vw) / (vw or 1),
            (c['close'] - poc) / (poc or 1),
            (c['close'] - vah) / (vah or 1),
            (c['close'] - val) / (val or 1),
            (c['close'] - c['open']) / (c['open'] or 1),
            (c['high'] - c['low']) / (c['close'] or 1),
            (c['volume'] / av) if av > 0 else 1,
        ]
        fut = (df['close'].iloc[i + fwd] - c['close']) / c['close']
        label = 2 if fut > thresh else (0 if fut < -thresh else 1)  # 2=BUY 1=HOLD 0=SELL
        features.append(feat); labels.append(label); idxs.append(i)
    return np.array(features), np.array(labels), idxs


def build_live_feature(df, idx):
    """Feature vector for the most recent CLOSED bar, same math as build_dataset()."""
    c = df.iloc[idx]
    vw = vwap(df, idx, 24)
    poc, vah, val = volume_profile(df, idx, 50, 20)
    av = avg_vol(df, idx, 20)
    return np.array([
        (c['close'] - vw) / (vw or 1),
        (c['close'] - poc) / (poc or 1),
        (c['close'] - vah) / (vah or 1),
        (c['close'] - val) / (val or 1),
        (c['close'] - c['open']) / (c['open'] or 1),
        (c['high'] - c['low']) / (c['close'] or 1),
        (c['volume'] / av) if av > 0 else 1,
    ])


def minmax_fit(X):
    mn, mx = X.min(axis=0), X.max(axis=0)
    return mn, mx


def minmax_apply(X, mn, mx):
    rng = mx - mn
    rng[rng == 0] = 1
    out = (X - mn) / rng
    return np.clip(out, 0, 1)


# ═══════════════════════════════════════
# KNN — same Euclidean-distance vote as the dashboard
# ═══════════════════════════════════════
class KNN:
    def __init__(self, k):
        self.k = k

    def fit(self, X, y):
        self.X, self.y = X, y

    def predict(self, x):
        d = np.linalg.norm(self.X - x, axis=1)
        nn = np.argsort(d)[:self.k]
        votes = np.bincount(self.y[nn], minlength=3)
        return int(np.argmax(votes))


# ═══════════════════════════════════════
# ANN — 7→12→3, ReLU+Softmax, SGD+momentum. Same architecture as dashboard.
# ═══════════════════════════════════════
class ANN:
    def __init__(self, lr=0.01, mom=0.85, seed=None):
        rng = np.random.default_rng(seed)
        self.lr, self.mom = lr, mom
        s1 = np.sqrt(2/7); s2 = np.sqrt(2/12)
        self.W1 = (rng.random((12, 7)) * 2 - 1) * s1
        self.b1 = np.zeros(12)
        self.W2 = (rng.random((3, 12)) * 2 - 1) * s2
        self.b2 = np.zeros(3)
        self.vW1 = np.zeros((12, 7)); self.vb1 = np.zeros(12)
        self.vW2 = np.zeros((3, 12)); self.vb2 = np.zeros(3)

    @staticmethod
    def relu(v): return np.maximum(v, 0)
    @staticmethod
    def relu_d(v): return (v > 0).astype(float)
    @staticmethod
    def softmax(a):
        e = np.exp(a - a.max())
        return e / e.sum()

    def forward(self, x):
        self.z1 = self.W1 @ x + self.b1
        self.a1 = self.relu(self.z1)
        self.z2 = self.W2 @ self.a1 + self.b2
        self.a2 = self.softmax(self.z2)
        return self.a2

    def backward(self, x, y_true):
        d2 = self.a2.copy()
        d2[y_true] -= 1
        d1 = (self.W2.T @ d2) * self.relu_d(self.z1)
        self.vW2 = self.mom*self.vW2 - self.lr*np.outer(d2, self.a1)
        self.W2 += self.vW2
        self.vb2 = self.mom*self.vb2 - self.lr*d2
        self.b2 += self.vb2
        self.vW1 = self.mom*self.vW1 - self.lr*np.outer(d1, x)
        self.W1 += self.vW1
        self.vb1 = self.mom*self.vb1 - self.lr*d1
        self.b1 += self.vb1

    def predict(self, x):
        return int(np.argmax(self.forward(x)))


def sig2dir(label):
    return 1 if label == 2 else (-1 if label == 0 else 0)


# ═══════════════════════════════════════
# TRAINING — retrain on ALL available history each cycle (live analogue of
# the dashboard's expanding-window fold, minus the held-out test slice
# since in live trading every historical bar is fair game for training)
# ═══════════════════════════════════════
def train_models(df):
    X, y, idxs = build_dataset(df)
    mn, mx = minmax_fit(X)
    Xn = minmax_apply(X, mn, mx)

    knn = KNN(KNN_K)
    knn.fit(Xn, y)

    ann = None
    if not TRADE_KNN_ONLY:
        ann = ANN(0.01, 0.85)
        n = len(Xn)
        idx_order = list(range(n))
        for epoch in range(ANN_EPOCHS):
            random.shuffle(idx_order)
            for i in idx_order:
                ann.forward(Xn[i])
                ann.backward(Xn[i], y[i])

    log(f"Retrained on {len(Xn)} samples (bars {idxs[0]}..{idxs[-1]}) — "
        f"class balance BUY/HOLD/SELL = {np.bincount(y, minlength=3).tolist()}"
        f"{' (KNN only — ANN not trained)' if TRADE_KNN_ONLY else ''}")
    return knn, ann, mn, mx


# ═══════════════════════════════════════
# POSITION MANAGEMENT
# ═══════════════════════════════════════
def get_open_position(magic):
    positions = mt5.positions_get(symbol=SYMBOL)
    if not positions:
        return None
    for p in positions:
        if p.magic == magic:
            return p
    return None


def send_market_order(direction, atr_value, tag, magic):
    """direction: 1=BUY, -1=SELL. Returns the order result or None (dry-run)."""
    tick = mt5.symbol_info_tick(SYMBOL)
    price = tick.ask if direction > 0 else tick.bid
    order_type = mt5.ORDER_TYPE_BUY if direction > 0 else mt5.ORDER_TYPE_SELL

    sl = price - STOP_ATR_MULT*atr_value if direction > 0 else price + STOP_ATR_MULT*atr_value
    tp = price + TP_ATR_MULT*atr_value if direction > 0 else price - TP_ATR_MULT*atr_value

    request = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       SYMBOL,
        "volume":       LOT_SIZE,
        "type":         order_type,
        "price":        price,
        "sl":           sl,
        "tp":           tp,
        "deviation":    DEVIATION,
        "magic":        magic,
        "comment":      tag,
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    log(f"{tag}: {'BUY' if direction>0 else 'SELL'} {LOT_SIZE} lots @ {price:.2f} "
        f"SL={sl:.2f} TP={tp:.2f} (ATR={atr_value:.2f})")

    if DRY_RUN:
        log(f"{tag}: DRY_RUN — order not sent.")
        return None

    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        log(f"{tag}: order_send FAILED retcode={result.retcode} comment={result.comment}")
        return None

    log_trade_csv({
        'time': datetime.now(timezone.utc).isoformat(), 'tag': tag, 'action': 'OPEN',
        'dir': 'BUY' if direction > 0 else 'SELL', 'price': price, 'sl': sl, 'tp': tp,
        'lots': LOT_SIZE, 'ticket': result.order,
    })
    return result


def close_position(pos, tag, reason):
    tick = mt5.symbol_info_tick(SYMBOL)
    is_buy = pos.type == mt5.POSITION_TYPE_BUY
    price = tick.bid if is_buy else tick.ask
    request = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       SYMBOL,
        "volume":       pos.volume,
        "type":         mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY,
        "position":     pos.ticket,
        "price":        price,
        "deviation":    DEVIATION,
        "magic":        pos.magic,
        "comment":      f"{tag}_CLOSE_{reason}",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    log(f"{tag}: closing ticket={pos.ticket} reason={reason} @ {price:.2f}")

    if DRY_RUN:
        log(f"{tag}: DRY_RUN — close not sent.")
        return None

    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        log(f"{tag}: close FAILED retcode={result.retcode} comment={result.comment}")
        return None

    log_trade_csv({
        'time': datetime.now(timezone.utc).isoformat(), 'tag': tag, 'action': f'CLOSE_{reason}',
        'dir': 'BUY' if is_buy else 'SELL', 'price': price, 'sl': '', 'tp': '',
        'lots': pos.volume, 'ticket': pos.ticket,
    })
    return result


# ═══════════════════════════════════════
# TRAILING STOP + PARTIAL CLOSE
# ═══════════════════════════════════════
_partially_closed_tickets = set()  # in-memory only — resets on script restart


def _symbol_volume_step():
    info = mt5.symbol_info(SYMBOL)
    step = info.volume_step if info and info.volume_step else 0.01
    return step


def _round_volume(vol, step):
    if step <= 0:
        step = 0.01
    n = round(vol / step)
    return round(n * step, 8)


def modify_sl(pos, new_sl, tag):
    """Move a position's SL (TP left untouched). No-op in DRY_RUN except logging."""
    if DRY_RUN:
        log(f"{tag}: DRY_RUN — would trail SL to {new_sl:.2f} (ticket={pos.ticket})")
        return True
    request = {
        "action":   mt5.TRADE_ACTION_SLTP,
        "symbol":   pos.symbol,
        "position": pos.ticket,
        "sl":       new_sl,
        "tp":       pos.tp,
        "magic":    pos.magic,
    }
    result = mt5.order_send(request)
    ok = result is not None and result.retcode == mt5.TRADE_RETCODE_DONE
    if ok:
        log(f"{tag}: trailed SL -> {new_sl:.2f} (ticket={pos.ticket})")
    else:
        detail = result.comment if result is not None else str(mt5.last_error())
        log(f"{tag}: trail SL FAILED ticket={pos.ticket} detail={detail}")
    return ok


def partial_close(pos, close_volume, tag):
    """Close close_volume lots of an open position, leaving the rest running."""
    tick = mt5.symbol_info_tick(pos.symbol)
    if tick is None:
        log(f"{tag}: partial close skipped — no tick for {pos.symbol}")
        return False
    is_buy = pos.type == mt5.POSITION_TYPE_BUY
    price = tick.bid if is_buy else tick.ask

    if DRY_RUN:
        log(f"{tag}: DRY_RUN — would partial-close {close_volume} lots @ {price:.2f} "
            f"(ticket={pos.ticket})")
        return True

    request = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       pos.symbol,
        "volume":       close_volume,
        "type":         mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY,
        "position":     pos.ticket,
        "price":        price,
        "deviation":    DEVIATION,
        "magic":        pos.magic,
        "comment":      f"{tag}_PARTIAL",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(request)
    ok = result is not None and result.retcode == mt5.TRADE_RETCODE_DONE
    if ok:
        log(f"{tag}: partial-closed {close_volume} lots @ {price:.2f} (ticket={pos.ticket})")
        log_trade_csv({
            'time': datetime.now(timezone.utc).isoformat(), 'tag': tag, 'action': 'PARTIAL_CLOSE',
            'dir': 'BUY' if is_buy else 'SELL', 'price': price, 'sl': '', 'tp': '',
            'lots': close_volume, 'ticket': pos.ticket,
        })
    else:
        detail = result.comment if result is not None else str(mt5.last_error())
        log(f"{tag}: partial close FAILED ticket={pos.ticket} detail={detail}")
    return ok


def manage_trailing_and_partial(pos, atr_value, tag):
    """
    Called every poll cycle for whatever position is currently open on this
    model's magic number. Two independent protections:
      1) Partial close: once floating profit >= PARTIAL_CLOSE_TRIGGER_USD,
         close a fixed PARTIAL_CLOSE_LOT lot (once per ticket) and move the
         remaining lot's SL to breakeven. The remaining lot's TP is left
         untouched, so it keeps running to the original take-profit.
      2) Trailing SL: once profit >= TRAIL_ACTIVATE_ATR_MULT * ATR, keep the
         SL trailing TRAIL_DISTANCE_ATR_MULT behind current price. The SL
         only ever tightens, never loosens.
    Positions are re-queried live from MT5 every cycle (see get_open_position),
    so a broker-side SL/TP hit is picked up automatically on the very next
    poll — no separate "restart" logic is needed. Once pos is None again,
    the normal bar-close signal logic (7-feature KNN prediction) resumes
    opening new trades exactly as before.
    """
    if pos is None:
        return

    is_buy = pos.type == mt5.POSITION_TYPE_BUY
    tick = mt5.symbol_info_tick(pos.symbol)
    if tick is None:
        return
    price = tick.bid if is_buy else tick.ask
    entry = pos.price_open

    # --- Partial close: fixed $ trigger, fixed lot size ---
    if PARTIAL_CLOSE_ENABLED and pos.ticket not in _partially_closed_tickets:
        if pos.profit >= PARTIAL_CLOSE_TRIGGER_USD:
            step = _symbol_volume_step()
            close_vol = _round_volume(PARTIAL_CLOSE_LOT, step)
            remaining = round(pos.volume - close_vol, 8)
            if close_vol >= step and remaining >= step:
                if partial_close(pos, close_vol, tag):
                    _partially_closed_tickets.add(pos.ticket)
                    if MOVE_SL_TO_BE_ON_PARTIAL:
                        modify_sl(pos, entry, tag)  # SL -> BE; TP untouched, remainder rides to original TP
            elif close_vol >= pos.volume:
                log(f"{tag}: partial-close lot {close_vol} >= position volume {pos.volume} — "
                    f"skipping partial, letting SL/TP manage full exit")
            else:
                log(f"{tag}: skip partial close — remainder {remaining} below step={step}")

    # --- Trailing SL (unchanged: ATR-based, only tightens) ---
    if atr_value is None or atr_value <= 0:
        return
    profit_dist = (price - entry) if is_buy else (entry - price)
    if profit_dist <= 0:
        return
    if TRAILING_SL_ENABLED and profit_dist >= TRAIL_ACTIVATE_ATR_MULT * atr_value:
        trail_dist = TRAIL_DISTANCE_ATR_MULT * atr_value
        candidate_sl = price - trail_dist if is_buy else price + trail_dist
        cur_sl = pos.sl
        improved = (cur_sl == 0) or \
                   (is_buy and candidate_sl > cur_sl) or \
                   (not is_buy and candidate_sl < cur_sl)
        if improved:
            modify_sl(pos, candidate_sl, tag)


_last_entry_bar = {}          # magic -> bar_time of the last entry opened for that model,
                               # in-memory only. Prevents the bar-close entry path and the
                               # immediate-reentry-when-flat path from both firing on the same bar.


def maybe_send_entry(signal, atr_value, tag, magic, bar_time):
    """
    Single choke point for opening a NEW position for a model. Both the
    normal bar-close flow (manage_model) and the immediate-reentry-when-flat
    check (check_immediate_reentry) call this instead of send_market_order
    directly, so a given model can only open once per distinct bar_time —
    whichever path gets there first wins, the other is a no-op.
    """
    if signal == 0:
        return
    if get_open_position(magic) is not None:
        return  # something already opened it (race between the two call sites)
    if _last_entry_bar.get(magic) == bar_time:
        return  # already acted on this bar for this model
    send_market_order(signal, atr_value, tag, magic)
    _last_entry_bar[magic] = bar_time


def check_immediate_reentry(tag, magic, model, mn, mx, atr_value, df):
    """
    Called every poll cycle (same cadence as manage_trailing_and_partial),
    for whichever model. Normally a model only gets a chance to open a NEW
    position once per bar close, inside manage_model(). That means if a
    position is closed mid-bar — full SL, the breakeven SL left behind by a
    partial close, or the original TP — the bot would otherwise sit flat and
    do nothing until the next candle closes.

    This closes that gap: if the model currently has NO open position, it
    recomputes the same live 7-feature signal used everywhere else and, if
    a fresh directional signal exists, opens immediately via
    maybe_send_entry() instead of waiting for the next bar.

    maybe_send_entry()'s per-bar guard means this can only fire once per
    distinct bar_time, so it won't spam repeated orders every poll cycle
    while sitting flat with an unchanged signal.
    """
    if not IMMEDIATE_REENTRY_ENABLED or model is None:
        return
    if get_open_position(magic) is not None:
        return  # still running — nothing to do here, trailing/partial handle it

    last_idx = len(df) - 1
    feat = build_live_feature(df, last_idx)
    feat_n = minmax_apply(feat.reshape(1, -1), mn, mx)[0]
    fresh_sig = sig2dir(model.predict(feat_n))
    bar_time = df['time'].iloc[last_idx]

    if fresh_sig != 0:
        log(f"{tag}: flat (no open position) — fresh signal={fresh_sig:+d}, "
            f"entering immediately without waiting for next bar close")
    maybe_send_entry(fresh_sig, atr_value, tag, magic, bar_time)


# ═══════════════════════════════════════
# HTTP BRIDGE — serves the same /account /tick /history /positions /order /close
# endpoints the dashboard's Auto Trade panel expects (atBridgeUrlInput / MT5_BRIDGE_URL).
# Runs in a background thread alongside the main signal loop, sharing the same
# MT5 terminal connection (the MetaTrader5 package is connection-per-process, not
# per-thread, so no extra initialize() call is needed here).
# ═══════════════════════════════════════
def _ms(dt_seconds):
    """MT5 timestamps are unix seconds; the dashboard expects milliseconds."""
    return int(dt_seconds) * 1000


def _bridge_history(params):
    symbol = params.get('symbol', [SYMBOL])[0]
    tf_str = params.get('tf', [TIMEFRAME_STR])[0]
    tf = TF_MAP.get(tf_str)
    if tf is None:
        return 400, {'error': 'bad_timeframe', 'detail': f'unknown tf={tf_str}'}

    frm, to = params.get('from', [None])[0], params.get('to', [None])[0]
    if frm and to:
        d_from = datetime.fromisoformat(frm).replace(tzinfo=timezone.utc)
        d_to = datetime.fromisoformat(to).replace(tzinfo=timezone.utc)
        rates = mt5.copy_rates_range(symbol, tf, d_from, d_to)
    else:
        count = int(params.get('count', [HISTORY_BARS])[0])
        rates = mt5.copy_rates_from_pos(symbol, tf, 0, count)

    if rates is None:
        return 502, {'error': 'mt5_error', 'detail': str(mt5.last_error())}

    bars = [{
        'time': _ms(r['time']), 'open': float(r['open']), 'high': float(r['high']),
        'low': float(r['low']), 'close': float(r['close']), 'volume': float(r['tick_volume']),
    } for r in rates]
    return 200, {'bars': bars}


def _bridge_account():
    acc = mt5.account_info()
    if acc is None:
        return 502, {'error': 'mt5_error', 'detail': str(mt5.last_error())}
    is_demo = acc.trade_mode == mt5.ACCOUNT_TRADE_MODE_DEMO
    return 200, {
        'login': acc.login, 'server': acc.server, 'balance': acc.balance,
        'equity': acc.equity, 'currency': acc.currency,
        'mode': 'DEMO' if is_demo else 'LIVE',
        'trading_allowed': bool(is_demo or ALLOW_LIVE) and not DRY_RUN,
    }


def _bridge_tick(params):
    symbol = params.get('symbol', [SYMBOL])[0]
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return 502, {'error': 'mt5_error', 'detail': str(mt5.last_error())}
    return 200, {'bid': tick.bid, 'ask': tick.ask, 'time': _ms(tick.time)}


def _bridge_positions(params):
    symbol = params.get('symbol', [SYMBOL])[0]
    magic = params.get('magic', [None])[0]
    positions = mt5.positions_get(symbol=symbol) or []
    if magic is not None:
        positions = [p for p in positions if p.magic == int(magic)]
    out = [{
        'ticket': p.ticket, 'type': 'BUY' if p.type == mt5.POSITION_TYPE_BUY else 'SELL',
        'volume': p.volume, 'price_open': p.price_open, 'sl': p.sl, 'tp': p.tp,
        'profit': p.profit, 'time': _ms(p.time), 'magic': p.magic, 'comment': p.comment,
    } for p in positions]
    return 200, out


def _bridge_order(body):
    symbol = body.get('symbol', SYMBOL)
    direction = 1 if str(body.get('type', '')).upper() == 'BUY' else -1
    magic = int(body.get('magic', 0))
    tag = body.get('comment', 'BRIDGE')
    sl_in, tp_in = float(body.get('sl') or 0), float(body.get('tp') or 0)
    volume = float(body.get('volume', LOT_SIZE))

    if not DRY_RUN:
        acc = mt5.account_info()
        is_demo = acc is not None and acc.trade_mode == mt5.ACCOUNT_TRADE_MODE_DEMO
        if not is_demo and not ALLOW_LIVE:
            return 403, {'error': 'live_blocked', 'detail': 'ALLOW_LIVE=False on a non-demo account'}

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return 502, {'error': 'mt5_error', 'detail': str(mt5.last_error())}
    price = tick.ask if direction > 0 else tick.bid

    if DRY_RUN:
        log(f"BRIDGE {tag}: DRY_RUN — order not sent.")
        return 200, {'ticket': 0, 'price': price, 'dry_run': True}

    request = {
        "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": volume,
        "type": mt5.ORDER_TYPE_BUY if direction > 0 else mt5.ORDER_TYPE_SELL,
        "price": price, "sl": sl_in, "tp": tp_in, "deviation": DEVIATION,
        "magic": magic, "comment": tag, "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(request)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        detail = result.comment if result is not None else str(mt5.last_error())
        return 502, {'error': 'order_failed', 'detail': detail}

    log_trade_csv({
        'time': datetime.now(timezone.utc).isoformat(), 'tag': tag, 'action': 'OPEN',
        'dir': 'BUY' if direction > 0 else 'SELL', 'price': price, 'sl': sl_in, 'tp': tp_in,
        'lots': volume, 'ticket': result.order,
    })
    return 200, {'ticket': result.order, 'price': price}


def _bridge_close(body):
    ticket = int(body.get('ticket'))
    reason = body.get('reason', 'BRIDGE')
    positions = mt5.positions_get(ticket=ticket)
    if not positions:
        return 404, {'error': 'not_found', 'detail': f'no open position with ticket={ticket}'}
    pos = positions[0]

    if DRY_RUN:
        log(f"BRIDGE: DRY_RUN — close not sent (ticket={ticket}).")
        tick = mt5.symbol_info_tick(pos.symbol)
        price = (tick.bid if pos.type == mt5.POSITION_TYPE_BUY else tick.ask) if tick else pos.price_open
        return 200, {'price': price, 'dry_run': True}

    is_buy = pos.type == mt5.POSITION_TYPE_BUY
    tick = mt5.symbol_info_tick(pos.symbol)
    if tick is None:
        return 502, {'error': 'mt5_error', 'detail': str(mt5.last_error())}
    price = tick.bid if is_buy else tick.ask
    request = {
        "action": mt5.TRADE_ACTION_DEAL, "symbol": pos.symbol, "volume": pos.volume,
        "type": mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY,
        "position": pos.ticket, "price": price, "deviation": DEVIATION,
        "magic": pos.magic, "comment": f"{pos.comment}_CLOSE_{reason}",
        "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(request)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        detail = result.comment if result is not None else str(mt5.last_error())
        return 502, {'error': 'close_failed', 'detail': detail}

    log_trade_csv({
        'time': datetime.now(timezone.utc).isoformat(), 'tag': pos.comment, 'action': f'CLOSE_{reason}',
        'dir': 'BUY' if is_buy else 'SELL', 'price': price, 'sl': '', 'tp': '',
        'lots': pos.volume, 'ticket': pos.ticket,
    })
    return 200, {'price': price}


class BridgeHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # keep stdout to the main loop's own log() lines

    def _send(self, status, payload):
        body = json.dumps(payload).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        try:
            if parsed.path == '/account':
                status, payload = _bridge_account()
            elif parsed.path == '/tick':
                status, payload = _bridge_tick(params)
            elif parsed.path == '/history':
                status, payload = _bridge_history(params)
            elif parsed.path == '/positions':
                status, payload = _bridge_positions(params)
            else:
                status, payload = 404, {'error': 'not_found'}
        except Exception as e:
            status, payload = 500, {'error': 'internal_error', 'detail': str(e)}
        self._send(status, payload)

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(length) if length else b'{}'
        try:
            body = json.loads(raw or b'{}')
        except json.JSONDecodeError:
            self._send(400, {'error': 'bad_json'})
            return
        try:
            if parsed.path == '/order':
                status, payload = _bridge_order(body)
            elif parsed.path == '/close':
                status, payload = _bridge_close(body)
            else:
                status, payload = 404, {'error': 'not_found'}
        except Exception as e:
            status, payload = 500, {'error': 'internal_error', 'detail': str(e)}
        self._send(status, payload)


def start_bridge_server():
    server = ThreadingHTTPServer((BRIDGE_HOST, BRIDGE_PORT), BridgeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log(f"HTTP bridge listening on http://{BRIDGE_HOST}:{BRIDGE_PORT} "
        f"(point the dashboard's Auto Trade bridge URL at http://localhost:{BRIDGE_PORT})")
    return server


def manage_model(tag, magic, signal, atr_value, df):
    """One iteration of the open/hold logic. Exits are left entirely to the
    broker-side SL/TP (plus trailing SL / partial close), since the
    time-based and signal-flip exits have been removed."""
    pos = get_open_position(magic)

    if pos is not None:
        log(f"{tag}: holding ticket={pos.ticket} (SL/TP are broker-side, no action needed)")

    if pos is None:
        bar_time = df['time'].iloc[len(df) - 1]
        maybe_send_entry(signal, atr_value, tag, magic, bar_time)


# ═══════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════
def main():
    connect()
    if BRIDGE_ENABLED:
        start_bridge_server()
    knn = ann = mn = mx = None
    last_processed_bar_time = None
    bars_since_retrain = RETRAIN_EVERY_BARS  # force a train on first pass

    while True:
        try:
            df = fetch_candles()
            last_closed_idx = len(df) - 1
            last_closed_time = df['time'].iloc[last_closed_idx]
            atr_series = build_atr_series(df)
            atr_now = atr_series[last_closed_idx]

            # Trailing SL + partial close run every poll cycle (not just on new
            # bar closes) so profit protection reacts within POLL_SECONDS.
            manage_trailing_and_partial(get_open_position(KNN_MAGIC), atr_now, KNN_TAG)
            if not TRADE_KNN_ONLY:
                manage_trailing_and_partial(get_open_position(ANN_MAGIC), atr_now, ANN_TAG)

            # Immediate re-entry check, also every poll cycle. Only runs once a
            # model has been trained at least once (knn/mn/mx not None).
            if knn is not None:
                check_immediate_reentry(KNN_TAG, KNN_MAGIC, knn, mn, mx, atr_now, df)
            if not TRADE_KNN_ONLY and ann is not None:
                check_immediate_reentry(ANN_TAG, ANN_MAGIC, ann, mn, mx, atr_now, df)

            if last_closed_time == last_processed_bar_time:
                time.sleep(POLL_SECONDS)
                continue

            log(f"New closed bar: {datetime.fromtimestamp(last_closed_time, tz=timezone.utc)}")
            last_processed_bar_time = last_closed_time
            bars_since_retrain += 1

            if knn is None or bars_since_retrain >= RETRAIN_EVERY_BARS:
                log("Retraining KNN + ANN on latest history…")
                knn, ann, mn, mx = train_models(df)
                bars_since_retrain = 0

            live_idx = last_closed_idx - FWD_BARS  # must match build_dataset's valid-index range
            live_idx = min(live_idx, last_closed_idx)
            feat = build_live_feature(df, last_closed_idx)
            feat_n = minmax_apply(feat.reshape(1, -1), mn, mx)[0]

            knn_sig = sig2dir(knn.predict(feat_n))

            if TRADE_KNN_ONLY:
                log(f"Signals — KNN={knn_sig:+d} (ANN disabled) ATR={atr_now:.2f} "
                    f"close={df['close'].iloc[last_closed_idx]:.2f}")
                manage_model(KNN_TAG, KNN_MAGIC, knn_sig, atr_now, df)
            else:
                ann_sig = sig2dir(ann.predict(feat_n))
                log(f"Signals — KNN={knn_sig:+d} ANN={ann_sig:+d} ATR={atr_now:.2f} "
                    f"close={df['close'].iloc[last_closed_idx]:.2f}")
                manage_model(KNN_TAG, KNN_MAGIC, knn_sig, atr_now, df)
                manage_model(ANN_TAG, ANN_MAGIC, ann_sig, atr_now, df)

        except Exception as e:
            log(f"ERROR: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == '__main__':
    main()
