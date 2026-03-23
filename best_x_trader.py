# Author: Ridhaant Ajoy Thackur
"""
best_x_trader.py — Best-X Paper Trading Engine
================================================
Uses the best X value found by the optimizer today to simulate
real-time paper trades using live prices from Algofinal.

Features:
  - Reads best X from x_optimizer_results/xopt_live_YYYYMMDD.csv every 30s
  - Reads live prices from levels/live_prices.json (written by Algofinal every 2s)
  - Simulates BUY/SELL entries, T1-T5 targets, SL, retreat exits
  - Writes trade log to best_x_trades/best_x_trades_YYYYMMDD.xlsx every exit
  - Exposes JSON state for unified_dash_v3.py /bestx page

Startup: run alongside Algofinal and the 3 scanners.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pytz

log = logging.getLogger("best_x_trader")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [BestX] %(levelname)s %(message)s")

IST               = pytz.timezone("Asia/Kolkata")
LEVELS_DIR        = "levels"
LIVE_PRICES_JSON  = os.path.join(LEVELS_DIR, "live_prices.json")
OPT_RESULTS_DIR   = "x_optimizer_results"
TRADES_DIR        = "best_x_trades"
BROKERAGE         = 20.0           # Rs per round-trip
STATE_FILE        = os.path.join(TRADES_DIR, "live_state.json")
CURRENT_X_DEFAULT = 0.008575
BUDGET            = 100_000.0      # Rs per position


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _now() -> datetime:
    return datetime.now(IST)

def _ts() -> str:
    return _now().strftime("%H:%M:%S")

def _rj(path: str):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _dynamic_qty(price: float) -> int:
    return int(BUDGET // price) if price > 0 else 0

def _in_trading(now: datetime) -> bool:
    t = now.hour * 60 + now.minute
    return 9 * 60 + 35 <= t <= 15 * 60 + 11   # 9:35 onwards (post blackout)

def _load_best_x() -> float:
    """Read best X from optimizer CSV (live first, then ranked fallback)."""
    def _pick(df: pd.DataFrame) -> Optional[float]:
        if df is None or df.empty or "x_value" not in df.columns:
            return None
        d = df.copy()
        d["x_value"] = pd.to_numeric(d["x_value"], errors="coerce")
        d = d.dropna(subset=["x_value"])
        if d.empty:
            return None
        if "score" in d.columns:
            d["score"] = pd.to_numeric(d["score"], errors="coerce").fillna(-1e9)
            d = d.sort_values("score", ascending=False)
        elif "total_pnl" in d.columns:
            d["total_pnl"] = pd.to_numeric(d["total_pnl"], errors="coerce").fillna(-1e18)
            d = d.sort_values("total_pnl", ascending=False)
        x = float(d.iloc[0]["x_value"])
        # Guard against corrupt CSV rows / impossible values.
        if 0.0001 <= x <= 0.05:
            return x
        return None

    try:
        ds = _now().strftime("%Y%m%d")
        live = os.path.join(OPT_RESULTS_DIR, f"xopt_live_{ds}.csv")
        if os.path.exists(live):
            x = _pick(pd.read_csv(live))
            if x is not None:
                return x
        ranked = os.path.join(OPT_RESULTS_DIR, f"xopt_ranked_{ds}.csv")
        if os.path.exists(ranked):
            x = _pick(pd.read_csv(ranked))
            if x is not None:
                return x
    except Exception:
        pass
    return CURRENT_X_DEFAULT

def _load_prev_closes() -> Dict[str, float]:
    """Load prev closes from Algofinal's persistent cache."""
    ds = _now().strftime("%Y%m%d")
    for fname in (f"prev_closes_persistent_{ds}.json",
                  f"prev_closes_cache_{ds}.json"):
        path = os.path.join(LEVELS_DIR, fname)
        if os.path.exists(path):
            d = _rj(path)
            if d:
                return {k.upper(): float(v) for k, v in d.items()
                        if isinstance(v, (int, float))}
    return {}

def _load_live_prices() -> Dict[str, float]:
    d = _rj(LIVE_PRICES_JSON) or {}
    p = d.get("equity_prices") or d.get("prices") or {}
    out: Dict[str, float] = {}
    for k, v in p.items():
        try:
            out[str(k).upper()] = float(v)
        except Exception:
            continue
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  SYMBOL STATE (one per stock)
# ══════════════════════════════════════════════════════════════════════════════

class SymState:
    def __init__(self, symbol: str, prev_close: float, x: float) -> None:
        self.symbol     = symbol
        self.prev_close = prev_close
        self.x          = x                       # Rs deviation
        step            = x                        # step = x (non-special)
        special = {"RELIANCE","SBIN","KOTAKBANK","ICICIBANK","HUL","HDFC"}
        if symbol in special:
            step = x * 0.6

        self.buy_above  = prev_close + x
        self.sell_below = prev_close - x
        self.buy_sl     = prev_close              # buy_above - x
        self.sell_sl    = prev_close              # sell_below + x
        self.t  = [self.buy_above  + step * i for i in range(1, 6)]
        self.st = [self.sell_below - step * i for i in range(1, 6)]
        self.step       = step

        # Position
        self.in_position  = False
        self.side: Optional[str] = None
        self.entry_price  = 0.0
        self.qty          = 0
        self.exited_today = False
        self.last_price   = 0.0

        # Trade history
        self.trades: List[dict] = []

    def calc_upnl(self, price: float) -> float:
        if not self.in_position or self.qty == 0:
            return 0.0
        if self.side == "BUY":
            return (price - self.entry_price) * self.qty
        return (self.entry_price - price) * self.qty

    def on_price(self, price: float, now: datetime) -> Optional[dict]:
        """Process one price tick. Returns trade dict if exit occurred."""
        self.last_price = price
        ts = now.strftime("%H:%M:%S")

        if not self.in_position:
            if self.exited_today:
                return None
            # Entry
            if price >= self.buy_above:
                qty = _dynamic_qty(price)
                if qty <= 0:
                    return None
                self.in_position = True
                self.side        = "BUY"
                self.entry_price = price
                self.qty         = qty
                log.info("BUY  %s @ %.2f  qty=%d  x=%.6f", self.symbol, price, qty, self.x / self.prev_close)
            elif price <= self.sell_below:
                qty = _dynamic_qty(price)
                if qty <= 0:
                    return None
                self.in_position = True
                self.side        = "SELL"
                self.entry_price = price
                self.qty         = qty
                log.info("SELL %s @ %.2f  qty=%d  x=%.6f", self.symbol, price, qty, self.x / self.prev_close)
            return None

        # In position — check exits
        ep  = self.entry_price
        qty = self.qty

        if self.side == "BUY":
            # Targets T1-T5
            for i, tgt in enumerate(self.t):
                if tgt > ep and price >= tgt:
                    return self._exit(f"T{i+1}", tgt, qty, ts, now)
            # Retreat: x * 0.25 * qty (always positive per spec)
            lvl_25 = self.buy_above + 0.25 * self.step
            if hasattr(self, '_peak_buy') and self._peak_buy and price <= lvl_25:
                gross = self.x * 0.25 * qty
                return self._exit_gross("RETREAT", price, qty, gross, ts, now)
            if price >= self.buy_above + 0.65 * self.step:
                self._peak_buy = True
            # SL
            if price <= self.buy_sl:
                return self._exit("BUY_SL", price, qty, ts, now)

        else:  # SELL
            # Targets ST1-ST5
            for i, tgt in enumerate(self.st):
                if (ep == 0 or tgt < ep) and price <= tgt:
                    return self._exit(f"ST{i+1}", tgt, qty, ts, now)
            # Retreat
            lvl_25 = self.sell_below - 0.25 * self.step
            if hasattr(self, '_peak_sell') and self._peak_sell and price >= lvl_25:
                gross = self.x * 0.25 * qty
                return self._exit_gross("RETREAT", price, qty, gross, ts, now)
            if price <= self.sell_below - 0.65 * self.step:
                self._peak_sell = True
            # SL
            if price >= self.sell_sl:
                return self._exit("SELL_SL", price, qty, ts, now)

        return None

    def eod_exit(self, price: float, now: datetime) -> Optional[dict]:
        if not self.in_position or self.qty == 0:
            return None
        return self._exit("EOD", price, self.qty, now.strftime("%H:%M:%S"), now)

    def _exit(self, exit_type: str, exit_price: float, qty: int,
              ts: str, now: datetime) -> dict:
        ep = self.entry_price
        if self.side == "BUY":
            gross = (exit_price - ep) * qty
        else:
            gross = (ep - exit_price) * qty
        return self._record(exit_type, exit_price, qty, gross, ts, now)

    def _exit_gross(self, exit_type: str, exit_price: float, qty: int,
                    gross: float, ts: str, now: datetime) -> dict:
        return self._record(exit_type, exit_price, qty, gross, ts, now)

    def _record(self, exit_type: str, exit_price: float, qty: int,
                gross: float, ts: str, now: datetime) -> dict:
        net = gross - BROKERAGE
        trade = {
            "time":        ts,
            "symbol":      self.symbol,
            "side":        self.side,
            "entry":       round(self.entry_price, 2),
            "exit":        round(exit_price, 2),
            "qty":         qty,
            "exit_type":   exit_type,
            "gross":       round(gross, 2),
            "net":         round(net, 2),
            "x_used":      round(self.x / self.prev_close, 8),
            "date":        now.strftime("%Y-%m-%d"),
        }
        self.trades.append(trade)
        self.in_position  = False
        self.side         = None
        self.entry_price  = 0.0
        self.qty          = 0
        self.exited_today = True
        self._peak_buy    = False
        self._peak_sell   = False
        log.info("EXIT  %s  %s @ %.2f  gross=%.2f  net=%.2f",
                 self.symbol, exit_type, exit_price, gross, net)
        return trade


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE
# ══════════════════════════════════════════════════════════════════════════════

class BestXTrader:
    def __init__(self) -> None:
        self._stop      = threading.Event()
        self._lock      = threading.Lock()
        self._states:   Dict[str, SymState] = {}
        self._trades:   List[dict] = []
        self._best_x    = CURRENT_X_DEFAULT
        self._best_x_at_state_build = 0.0
        self._x_age     = 0.0
        self._price_age = 9999.0
        self._state_day = ""

    def start(self) -> None:
        os.makedirs(TRADES_DIR, exist_ok=True)
        t = threading.Thread(target=self._loop, daemon=True, name="BestXTrader")
        t.start()
        log.info("BestXTrader started (budget=Rs%.0f)", BUDGET)

    def stop(self) -> None:
        self._stop.set()

    # ── Public read (for dashboard) ───────────────────────────────────────────

    def get_state(self) -> dict:
        with self._lock:
            states = list(self._states.values())
            trades = list(self._trades)
        prices = _load_live_prices()
        open_pos = [
            {"symbol": s.symbol, "side": s.side,
             "entry": s.entry_price, "qty": s.qty,
             "live": prices.get(s.symbol, 0),
             "upnl": round(s.calc_upnl(prices.get(s.symbol, s.entry_price)), 2)}
            for s in states if s.in_position
        ]
        day_net   = sum(t["net"]   for t in trades)
        day_gross = sum(t["gross"] for t in trades)
        return {
            "best_x":    round(self._best_x, 8),
            "x_age_s":   round(time.monotonic() - self._x_age, 0) if self._x_age else 9999,
            "price_age": round(self._price_age, 1),
            "open":      open_pos,
            "trades":    trades[-50:],     # last 50 for display
            "day_gross": round(day_gross, 2),
            "day_net":   round(day_net, 2),
            "n_trades":  len(trades),
        }

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _loop(self) -> None:
        last_x_refresh  = 0.0
        last_xlsx_write = 0.0
        eod_done        = False

        while not self._stop.is_set():
            now = _now()
            ds = now.strftime("%Y%m%d")

            # Reload best X every 30s
            if time.monotonic() - last_x_refresh > 30:
                new_x = _load_best_x()
                with self._lock:
                    self._best_x = new_x
                self._x_age      = time.monotonic()
                last_x_refresh   = time.monotonic()

            # Rebuild states on first run, day-roll, or meaningful best-X change.
            x_changed = abs(float(self._best_x) - float(self._best_x_at_state_build or 0.0)) > 0.00005
            if not self._states or self._state_day != ds or x_changed:
                self._rebuild_states()
                self._state_day = ds
                eod_done = False

            # EOD at 15:11
            if now.hour == 15 and now.minute >= 11 and not eod_done:
                prices = _load_live_prices()
                new_trades = []
                with self._lock:
                    for sym, st in self._states.items():
                        px = prices.get(sym, st.last_price)
                        if px:
                            t = st.eod_exit(px, now)
                            if t:
                                new_trades.append(t)
                    self._trades.extend(new_trades)
                eod_done = True
                self._write_xlsx()
                log.info("EOD square-off complete. Trades today: %d", len(self._trades))

            if not _in_trading(now):
                self._write_state()
                self._stop.wait(5)
                continue

            # Price tick
            prices = _load_live_prices()
            if not prices:
                self._stop.wait(2)
                continue

            # Detect price file age
            try:
                self._price_age = time.time() - os.path.getmtime(LIVE_PRICES_JSON)
            except Exception:
                self._price_age = 9999.0

            new_trades = []
            with self._lock:
                for sym, st in self._states.items():
                    px = prices.get(sym)
                    if px is None:
                        continue
                    trade = st.on_price(px, now)
                    if trade:
                        new_trades.append(trade)
                self._trades.extend(new_trades)

            if new_trades:
                self._write_xlsx()

            # Write state every 5s, xlsx every 60s
            self._write_state()
            if time.monotonic() - last_xlsx_write > 60:
                self._write_xlsx()
                last_xlsx_write = time.monotonic()

            self._stop.wait(2)

    def _rebuild_states(self) -> None:
        """Build SymState for every symbol using best X and today's prev closes."""
        pcs = _load_prev_closes()
        if not pcs:
            log.warning("No prev closes yet — waiting for Algofinal to write them")
            return
        x_mult = self._best_x
        new_states: Dict[str, SymState] = {}
        for sym, pc in pcs.items():
            x_rs = pc * x_mult
            if x_rs <= 0:
                continue
            new_states[sym] = SymState(sym, pc, x_rs)
        with self._lock:
            self._states = new_states
            self._best_x_at_state_build = float(x_mult)
        log.info("Rebuilt %d symbols with Best X=%.6f", len(new_states), x_mult)

    def _write_state(self) -> None:
        try:
            state = self.get_state()
            tmp   = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(state, f, separators=(",", ":"))
            os.replace(tmp, STATE_FILE)
        except Exception:
            pass

    def _write_xlsx(self) -> None:
        try:
            if not self._trades:
                return
            ds   = _now().strftime("%Y%m%d")
            path = os.path.join(TRADES_DIR, f"best_x_trades_{ds}.xlsx")
            df   = pd.DataFrame(self._trades)

            # Summary sheet
            summary_rows = []
            for sym in df["symbol"].unique():
                sdf = df[df["symbol"] == sym]
                summary_rows.append({
                    "Symbol":   sym,
                    "Trades":   len(sdf),
                    "Wins":     int((sdf["net"] > 0).sum()),
                    "Losses":   int((sdf["net"] <= 0).sum()),
                    "Gross":    round(sdf["gross"].sum(), 2),
                    "Net":      round(sdf["net"].sum(), 2),
                    "Win%":     round((sdf["net"] > 0).mean() * 100, 1),
                    "X Used":   round(float(sdf["x_used"].iloc[0]), 8),
                })
            summary_rows.append({
                "Symbol": "TOTAL", "Trades": len(df),
                "Wins": int((df["net"] > 0).sum()),
                "Losses": int((df["net"] <= 0).sum()),
                "Gross": round(df["gross"].sum(), 2),
                "Net": round(df["net"].sum(), 2),
                "Win%": round((df["net"] > 0).mean() * 100, 1),
                "X Used": round(self._best_x, 8),
            })
            summary_df = pd.DataFrame(summary_rows)

            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="All Trades", index=False)
                summary_df.to_excel(writer, sheet_name="Summary", index=False)
            log.debug("Wrote %s (%d trades)", path, len(df))
        except Exception as e:
            log.debug("xlsx write error: %s", e)


# ── Module-level singleton accessed by unified_dash_v3 ───────────────────────
_TRADER: Optional[BestXTrader] = None

def get_trader() -> BestXTrader:
    global _TRADER
    if _TRADER is None:
        _TRADER = BestXTrader()
        _TRADER.start()
    return _TRADER


if __name__ == "__main__":
    trader = get_trader()
    log.info("BestX trader running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(10)
            s = trader.get_state()
            log.info("BestX=%.6f  open=%d  trades=%d  net=Rs%.2f",
                     s["best_x"], len(s["open"]), s["n_trades"], s["day_net"])
    except KeyboardInterrupt:
        trader.stop()
