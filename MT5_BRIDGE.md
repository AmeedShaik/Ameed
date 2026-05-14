# MT5 Bridge Setup

Connects `mtf_dashboard.html` to your local MetaTrader 5 terminal.

## How it works

```
MT5 Terminal  <--IPC-->  mt5_bridge.py  <--HTTP/WS-->  mtf_dashboard.html
  (logged in)            (FastAPI)                     (browser)
```

The bridge polls MT5 every second and re-emits bars in the same JSON shape the
dashboard expected from Binance, so the front-end indicator code is unchanged.
It also exposes account, positions, and (optionally) order placement +
trailing-SL.

## One-time setup

1. Install Python 3.10+ for Windows.
2. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
3. Open MetaTrader 5 and log in to your broker. Leave it running.
4. In MT5, right-click **Market Watch** -> `Show All`, so the symbols you want
   to scan/trade are visible (`XAUUSD`, `BTCUSD`, `EURUSD`, `GBPUSD`,
   `USDJPY`, `US30` by default).
5. In MT5 toolbar, make sure **AutoTrading** is green. Without that, the
   broker rejects every order with retcode 10027.

## Run modes

```powershell
# Read-only: dashboard, scanner, account, positions
python mt5_bridge.py

# + manual order placement (ARM in UI to enable)
python mt5_bridge.py --enable-trading

# + auto-execute on Tier 1 confluence (UI AUTO toggle still required)
# + trailing SL on bridge-placed positions
python mt5_bridge.py --enable-trading --trailing-sl
```

Then open <http://localhost:8000>. Badge should read `LIVE - MT5`.

## Trading flow (manual)

1. Start with `--enable-trading`.
2. ARM the trade panel.
3. Adjust risk % (default 0.5%); lot size auto-calculates.
4. Click EXECUTE LONG / SHORT, confirm dialog, order placed.
5. Panel auto-disarms; re-arm to place another.

## Auto-execute

A separate AUTO toggle in the trade panel. Gates (must ALL be true to fire):

1. Bridge started with `--enable-trading`
2. ARM is on (manual)
3. AUTO is on (manual, defaults off every page load)
4. Confluence count >= 4/5 TFs (Tier 1 only — Tier 2/3 ignored)
5. Cooldown elapsed since last auto-fire on this symbol (default 5 min)
6. No existing position open for this symbol

Auto trades are tagged with comment `kiro-auto` and the same magic number
`90210`, so the trailing-SL loop will manage their stops.

## Trailing SL (`--trailing-sl`)

A background task that runs every 5 seconds:

- Only touches positions where `magic == 90210` (i.e. trades placed by the
  bridge) — your manual MT4/MT5 trades are never touched.
- For longs: candidate SL = current price - 2.0 * ATR(M5,14).
- For shorts: candidate SL = current price + 2.0 * ATR(M5,14).
- Only updates SL when the new value is strictly better AND at least 0.5 ATR
  closer than the existing SL (avoids per-tick spam updates).
- Only ever moves SL toward break-even, never away.

Tuneables in `mt5_bridge.py`:

```python
TRAIL_POLL_SECONDS  = 5.0   # eval frequency
TRAIL_ATR_MULT      = 2.0   # distance from price
TRAIL_MIN_STEP_ATR  = 0.5   # minimum SL movement before updating
```

## Multi-symbol scanner

The bottom row of the dashboard is a heatmap of every symbol in `PAIRS`
(top of the JS in `mtf_dashboard.html`). Each card shows:

- RSI bucket per timeframe (M1, M5, M15, H1, H4) — green if oversold, red if
  overbought.
- Status: LONG / SHORT / NEUTRAL based on confluence.
- Click a card to jump the main dashboard to that symbol.

It re-scans every 30s. Tier 1 alerts (4-5/5 TFs aligned) post to the signal
log, deduped per symbol+direction for 5 minutes.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /api/health` | bridge + account status; includes `trading_enabled`, `trailing_enabled`, `magic` |
| `GET /api/symbols` | broker-visible symbol names |
| `GET /api/symbol_info?symbol=XAUUSD` | digits, point, tick value (for lot sizing) |
| `GET /api/klines?symbol=...&interval=1m` | OHLCV history |
| `GET /api/account` | balance, equity, free margin, P/L |
| `GET /api/positions?symbol=XAUUSD` | open positions |
| `POST /api/order` (trading only) | `{symbol, side, volume, sl?, tp?, comment?}` |
| `POST /api/close/{ticket}` (trading only) | close one position |
| `POST /api/modify_sl` (trading only) | `{ticket, sl?, tp?}` |
| `WS /ws/{symbol}/{interval}` | live bar updates |

## Symbol names

If your broker's symbols are postfixed (`XAUUSD.s`, `BTCUSDm`, `BTCUSD.pro`),
hit <http://localhost:8000/api/symbols> to find the actual names, then
update both:

- `<option value="...">` lines near the top of `mtf_dashboard.html`
- `const PAIRS = [...]` constant in the same file
- `decimalsFor()` if you add a new symbol with non-standard decimals

## Troubleshooting

- **`mt5.initialize() failed`** - MT5 isn't running, or different Windows user.
- **Order rejected (retcode=10027)** - "AutoTrading disabled by client". Click
  the AutoTrading button in the MT5 toolbar so it's green.
- **Order rejected (retcode=10018)** - "Market closed".
- **Lot size 0.00** - either equity didn't load yet, or risk % is below
  broker's `volume_min`. Try 1%.
- **Trailing SL doesn't fire** - check the bridge log for `TRAIL` lines. It
  only acts on `magic=90210` positions; manual MT5 trades are intentionally
  skipped.
- **Scanner shows ERROR for one symbol** - usually means the symbol doesn't
  exist on your broker. Remove it from `PAIRS`.

## Customising

- Polling interval (bars): `POLL_SECONDS` in `mt5_bridge.py`
- Magic number: `MAGIC` in `mt5_bridge.py`
- Auto-execute cooldown: `AUTO_COOLDOWN_MS` in `mtf_dashboard.html`
- Auto-execute threshold: `AUTO_TIER1_THRESHOLD` in `mtf_dashboard.html`
- Scanner refresh: `SCAN_INTERVAL_MS` in `mtf_dashboard.html`
- Bind to LAN: `python mt5_bridge.py --host 0.0.0.0`
