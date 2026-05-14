# MT5 Bridge Setup

Connects `mtf_dashboard.html` to your local MetaTrader 5 terminal.

## How it works

```
MT5 Terminal  <--IPC-->  mt5_bridge.py  <--HTTP/WS-->  mtf_dashboard.html
  (logged in)            (FastAPI)                     (browser)
```

The bridge polls MT5 every second and re-emits bars in the same JSON shape the
dashboard expected from Binance, so the front-end indicator code is unchanged.
It also exposes account, positions, and (optionally) order placement.

## One-time setup

1. Install Python 3.10+ for Windows.
2. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
3. Open MetaTrader 5 and log in to your broker. Leave it running.
4. In MT5, right-click the **Market Watch** panel and `Show All` so the symbols
   `XAUUSD` and `BTCUSD` are visible.

## Run

```powershell
# Read-only: dashboard, account, positions (no order placement)
python mt5_bridge.py

# With trading enabled (allows order placement and closing)
python mt5_bridge.py --enable-trading
```

Then open <http://localhost:8000>. The badge should read `LIVE - MT5`.

## Trading mode

Order placement is **disabled by default** for safety. To allow it:

1. Start the bridge with `--enable-trading`.
2. In the dashboard, click the ARM switch in the TRADE panel.
3. Adjust risk % (default 0.5%). The lot size auto-calculates so a stop-loss
   hit costs that risk % of equity.
4. Click EXECUTE LONG or EXECUTE SHORT. A confirm dialog shows entry / SL / TP.
5. After a successful order, the panel auto-disarms. Re-arm to place another.

Safety rails in place:

- Bridge refuses orders unless started with `--enable-trading`
- UI starts disarmed on every page load (no persistent armed state)
- Buttons disabled unless ARMED + signal data available
- Confirm dialog before every order
- Auto-disarm after each successful order
- All bridge orders tagged with magic number `90210` and comment `kiro-mtf`
- Order endpoints return the broker's exact rejection reason on failure

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /api/health` | bridge + account status, includes `trading_enabled` flag |
| `GET /api/symbols` | broker-visible symbol names |
| `GET /api/symbol_info?symbol=XAUUSD` | digits, point, tick value (for lot sizing) |
| `GET /api/klines?symbol=...&interval=1m` | OHLCV history |
| `GET /api/account` | balance, equity, free margin, P/L |
| `GET /api/positions?symbol=XAUUSD` | open positions |
| `POST /api/order` (trading only) | `{symbol, side, volume, sl?, tp?, comment?}` |
| `POST /api/close/{ticket}` (trading only) | close one position |
| `WS /ws/{symbol}/{interval}` | live bar updates |

## Symbol names

If `XAUUSD` or `BTCUSD` doesn't exist on your broker (e.g. `XAUUSD.s`,
`BTCUSDm`, `BTCUSD.pro`), hit <http://localhost:8000/api/symbols> to see the
full list, then update the `<option value="...">` lines near the top of
`mtf_dashboard.html`.

## Troubleshooting

- **`mt5.initialize() failed`** - MT5 isn't running, or you're running the
  bridge as a different Windows user than MT5.
- **Order rejected (retcode=10027)** - "AutoTrading disabled by client" - click
  the **AutoTrading** button in the MT5 toolbar so it turns green.
- **Order rejected (retcode=10018)** - "Market closed" - the symbol's session
  is closed. Try during market hours.
- **Order rejected (retcode=10030)** - "Unsupported filling mode" - rare; the
  bridge auto-picks FOK / IOC / RETURN based on `symbol_info.filling_mode`.
- **Lot size shows 0.00** - either equity didn't load yet, or risk % is too
  small for the broker's `volume_min`. Try raising risk % to 1%.

## Customising

- Polling interval: change `POLL_SECONDS` in `mt5_bridge.py`
- Magic number / comment: change `MAGIC` in `mt5_bridge.py`
- Bind to LAN: `python mt5_bridge.py --host 0.0.0.0`
