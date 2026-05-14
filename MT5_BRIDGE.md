# MT5 Bridge Setup

Connects `mtf_dashboard.html` to your local MetaTrader 5 terminal.

## How it works

```
MT5 Terminal  <--IPC-->  mt5_bridge.py  <--HTTP/WS-->  mtf_dashboard.html
  (logged in)            (FastAPI)                     (browser)
```

The bridge polls MT5 every second and re-emits bars in the same JSON shape the
dashboard already expected from Binance, so the front-end indicator code is
unchanged.

## One-time setup

1. Install Python 3.10+ for Windows.
2. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
3. Open MetaTrader 5 and log in to your broker. Leave it running.
4. In MT5, right-click the **Market Watch** panel and `Show All` so the symbols
   `XAUUSD` and `BTCUSD` are visible (the bridge will also try to enable them
   automatically, but this avoids the first-run hiccup).

## Run

```powershell
python mt5_bridge.py
```

Then open <http://localhost:8000> in your browser. The badge should read
`LIVE - MT5` once history loads.

## Symbol names

Different brokers use different symbol names. If `XAUUSD` or `BTCUSD` doesn't
exist on your broker (e.g. it might be `XAUUSD.s`, `XAUUSDm`, `BTCUSD.pro`),
hit `http://localhost:8000/api/symbols` to see the full list, then update the
`<option value="...">` entries near the top of `mtf_dashboard.html`.

## Troubleshooting

- **`mt5.initialize() failed`** - MT5 isn't running, or you're running the
  bridge as a different Windows user than MT5. Start MT5 first, log in, then
  run the bridge from the same user account.
- **`Unknown MT5 symbol`** - broker uses a different name. See above.
- **`copy_rates_from_pos returned no data`** - the symbol exists but isn't
  enabled in Market Watch. Right-click the symbol -> Show.
- **WebSocket keeps reconnecting** - check the bridge console for errors.
  Closing MT5 will drop the connection; reopen and the dashboard reconnects
  automatically.
- **Times look off by a few hours** - MT5 returns broker server time, not UTC.
  The dashboard only uses bar ordering, not absolute time, so this is cosmetic.

## Customising

- Polling interval: change `POLL_SECONDS` in `mt5_bridge.py`.
- History bars: change `DEFAULT_LIMIT` (or pass `?limit=` on the REST call).
- Bind to LAN: change `HOST = "127.0.0.1"` to `"0.0.0.0"` (then access by
  the PC's LAN IP from another device).
