# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v9.0 | Author: Ridhaant Ajoy Thackur
# commodity_scanner1.py — MCX Narrow Sweep (1K variations/symbol, 5K total)
# ═══════════════════════════════════════════════════════════════════════
"""
commodity_scanner1.py — MCX Narrow Sweep  v9.0
================================================
5 symbols × 1,000 X variations = 5,000 total variations/day
X range: 0.65× to 1.35× of calibrated COMM_X multiplier per symbol

Mirrors equity scanner1.py exactly, adapted for MCX:
  ✓ Premarket level adjustment  09:00–09:30 IST
  ✓ 09:30 re-anchor             recalc levels at actual 09:30 price
  ✓ 09:30–09:35 blackout        no new entries
  ✓ Re-entry watch              threshold + retouch (from sweep_core)
  ✓ Retreat 65/45/25            peak guard → locked gain → 25% exit
  ✓ T1–T5 / ST1–ST5 targets
  ✓ SL exits
  ✓ EOD square-off at 23:30 IST (NOT 15:11)
  ✓ RAM spill every 5 min
  ✓ Atomic live_state.json dump every 30s
  ✓ ZMQ topic: "commodity"
"""
from __future__ import annotations

import json
import logging
import multiprocessing
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pytz
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

from sweep_core import (
    StockSweep, PriceStore, PriceSubscriber,
    save_results, spill_trade_logs_to_disk,
    read_prev_closes_from_algofinal,
    now_ist, in_premarket, after_930, in_trading_for,
    in_commodity_session, is_commodity_eod,
    IST, BROKERAGE_PER_SIDE, CPU_WORKERS,
    CUDA_AVAILABLE, GPU_NAME,
)
from config import cfg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CommS1] %(levelname)s — %(message)s",
)
log = logging.getLogger("commodity_scanner1")

# ── Configuration ─────────────────────────────────────────────────────────────
SCANNER_NAME = "Comm-Scanner-1 Narrow"
SCANNER_ID   = "comm1"
SYMBOLS: List[str] = ["GOLD", "SILVER", "CRUDE", "NATURALGAS", "COPPER"]
N_VALUES = 1_000

# X ranges: 0.65× to 1.35× of calibrated COMM_X multiplier
COMM_NARROW: Dict[str, Tuple[float, float]] = {
    "GOLD":       (0.002230, 0.004631),
    "SILVER":     (0.003344, 0.006946),
    "NATURALGAS": (0.000557, 0.001157),
    "CRUDE":      (0.000391, 0.000813),
    "COPPER":     (0.002600, 0.005400),
}
X_ARRAYS: Dict[str, np.ndarray] = {
    sym: np.linspace(lo, hi, N_VALUES)
    for sym, (lo, hi) in COMM_NARROW.items()
}

RESULTS_DIR          = os.path.join("sweep_results", "commodity_scanner1")
ZMQ_TOPIC            = b"commodity"
PRICE_FETCH_INTERVAL = 1.0
STATE_DUMP_INTERVAL  = 30

CURRENT_X = cfg.COMM_X   # {sym: multiplier}


# ════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ════════════════════════════════════════════════════════════════════════════

def _tg_async(text: str) -> None:
    try:
        from tg_async import send_alert
        send_alert(text, asset_class="commodity")
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════
# RICH TABLE
# ════════════════════════════════════════════════════════════════════════════

def _build_table(
    sweeps: Dict[str, StockSweep],
    now: datetime,
    sweep_ms: float,
    updated: int,
    ticks: int,
) -> Table:
    t = Table(
        title=(
            f"[bold cyan]{SCANNER_NAME} — {now.strftime('%H:%M:%S IST')}  "
            f"sweep={sweep_ms:.1f}ms  updated={updated}/{len(sweeps)}  ticks={ticks}[/]"
        ),
        show_header=True, header_style="bold yellow",
        border_style="dim", expand=False,
    )
    t.add_column("Symbol",       style="bold white", width=14)
    t.add_column("Prev Close",   style="dim",        width=12, justify="right")
    t.add_column("Last Price",   style="cyan",       width=12, justify="right")
    t.add_column("Best X",       style="green",      width=10, justify="right")
    t.add_column("vs Live X",    style="yellow",     width=11, justify="right")
    t.add_column("P&L",          width=13,           justify="right")
    t.add_column("Win%",         width=7,            justify="right")
    t.add_column("Trades",       width=7,            justify="right")
    t.add_column("W/L",          width=7,            justify="right")
    t.add_column("Last Breached", width=26)

    for sym, sw in sweeps.items():
        r = sw.row_data()
        # P&L colouring
        pnl_txt = Text("—", style="dim")
        if r["total_pnl"] != "—":
            val     = float(r["total_pnl"].replace("₹", "").replace(",", ""))
            pnl_txt = Text(r["total_pnl"], style="green" if val >= 0 else "red")
        # vs current X
        vs_txt = Text("—", style="dim")
        if r["has_data"]:
            try:
                bx  = float(r["best_x"])
                lx  = CURRENT_X.get(sym, cfg.CURRENT_X_MULTIPLIER)
                pct = (bx - lx) / lx * 100
                vs_txt = Text(f"{pct:+.2f}%", style="yellow")
            except Exception:
                pass
        # W/L ratio
        wl = "—"
        if r["win_count"] != "—" and r["loss_count"] != "—":
            wl = f"{r['win_count']}/{r['loss_count']}"
        # Last event colouring
        lv = r["last_event"]
        lv_col = "dim"
        if lv != "—":
            if any(k in lv for k in ("SL", "RETREAT")):   lv_col = "red"
            elif any(k in lv for k in ("T1","T2","T3","T4","T5","ST")): lv_col = "green"
            elif "ENTRY"   in lv:  lv_col = "cyan"
            elif "EOD"     in lv:  lv_col = "yellow"
            elif "REENTRY" in lv:  lv_col = "magenta"
        t.add_row(
            r["symbol"], r["prev_close"], r["last_price"],
            Text(r["best_x"],    style="green"  if r["has_data"] else "dim"),
            vs_txt,
            pnl_txt,
            Text(r["win_rate_pct"], style="cyan"  if r["has_data"] else "dim"),
            Text(r["trade_count"],  style="white" if r["has_data"] else "dim"),
            Text(wl,                style="white" if r["has_data"] else "dim"),
            Text(lv,                style=lv_col),
        )
    return t


# ════════════════════════════════════════════════════════════════════════════
# STATE DUMP
# ════════════════════════════════════════════════════════════════════════════

def _dump_live_state(sweeps: Dict[str, StockSweep], date_str: str, out_dir: str) -> None:
    state = {
        "scanner":    SCANNER_NAME,
        "scanner_id": SCANNER_ID,
        "asset_class": "commodity",
        "date":       date_str,
        "written_at": now_ist().isoformat(),
        "sweeps":     {sym: sw.dump_state() for sym, sw in sweeps.items()},
    }
    path = os.path.join(out_dir, "live_state.json")
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, separators=(",", ":"))
    os.replace(tmp, path)


# ════════════════════════════════════════════════════════════════════════════
# PREV-CLOSE LOADER  (commodity-specific: reads commodity_initial_levels)
# ════════════════════════════════════════════════════════════════════════════

def _load_prev_closes(date_str: str) -> Dict[str, float]:
    """Load MCX prev_closes from commodity_engine JSON, then yfinance fallback."""
    result: Dict[str, float] = {}
    stale_levels_s = 36 * 3600.0  # ignore old anchors >36h
    now_ts = time.time()

    # 1. Commodity engine's initial levels JSON
    for fname in (
        f"commodity_initial_levels_{date_str}.json",
        f"commodity_prev_closes_{date_str}.json",
    ):
        path = os.path.join("levels", fname)
        if os.path.exists(path):
            age_s = now_ts - float(os.path.getmtime(path))
            if age_s > stale_levels_s:
                log.warning("  Ignoring stale %s (age=%.0fs > %.0fs)", fname, age_s, stale_levels_s)
                continue
            try:
                data = json.load(open(path, encoding="utf-8"))
                lvs  = data.get("levels", data)
                for sym in SYMBOLS:
                    entry = lvs.get(sym, {})
                    pc    = (entry.get("prev_close") or entry.get("anchor")
                             if isinstance(entry, dict) else entry)
                    if pc and float(pc) > 0:
                        result[sym] = float(pc)
                if result:
                    log.info("Loaded MCX prev_closes from %s (%d syms)", fname, len(result))
                    return result
            except Exception as exc:
                log.debug("Load %s: %s", fname, exc)

    # 2. Algofinal persistent cache (may have commodity symbols)
    af_pcs = read_prev_closes_from_algofinal(date_str)
    for sym in SYMBOLS:
        if sym.upper() in af_pcs and af_pcs[sym.upper()] > 0:
            result[sym] = af_pcs[sym.upper()]

    # 3. yfinance fallback
    _YF = {"GOLD": "GC=F", "SILVER": "SI=F", "CRUDE": "CL=F",
           "NATURALGAS": "NG=F", "COPPER": "HG=F"}
    for sym in SYMBOLS:
        if sym in result:
            continue
        try:
            import yfinance as yf
            t  = yf.Ticker(_YF.get(sym, f"{sym}=F"))
            df = t.history(period="5d", interval="1d")
            if df is not None and not df.empty:
                result[sym] = float(df["Close"].iloc[-1])
                log.info("  %s prev_close=%.2f (yfinance)", sym, result[sym])
        except Exception as e:
            log.warning("  %s yfinance fallback failed: %s", sym, e)

    # 4. live_prices.json fallback (last-known USDT prices from CommodityEngine)
    #    Helps when initial_levels files are missing/stale.
    try:
        lp_path = os.path.join("levels", "live_prices.json")
        if os.path.exists(lp_path):
            lp_age_s = now_ts - float(os.path.getmtime(lp_path))
            if lp_age_s > 120:
                log.warning("  live_prices.json is stale (age=%.0fs)", lp_age_s)
            data = json.load(open(lp_path, encoding="utf-8"))
            comm = data.get("commodity_prices", {}) or {}
            for sym in SYMBOLS:
                if result.get(sym, 0.0) and result.get(sym, 0.0) > 0:
                    continue
                v = comm.get(sym) or comm.get(sym.upper()) or 0
                if v and float(v) > 0:
                    result[sym] = float(v)
                    log.warning("  %s prev_close fallback from live_prices.json=%.4f (USDT)", sym, result[sym])
    except Exception as e:
        log.debug("live_prices.json fallback failed: %s", e)

    missing = [sym for sym in SYMBOLS if sym not in result or result.get(sym, 0.0) <= 0]
    if missing:
        log.warning("  Prev_close anchors missing for %s", ", ".join(missing))

    return result


def _fetch_930_price_comm(sym: str, prev_close: float) -> Optional[float]:
    """Fetch commodity 09:30 IST price for re-anchoring."""
    # Check shared market data written by commodity_engine
    date_str = now_ist().strftime("%Y%m%d")
    try:
        for fname in (
            f"commodity_initial_levels_{date_str}.json",
            f"commodity_930_prices_{date_str}.json",
        ):
            path = os.path.join("levels", fname)
            if os.path.exists(path):
                data = json.load(open(path, encoding="utf-8"))
                lvs  = data.get("levels_930", data.get("levels", {}))
                entry = lvs.get(sym, {})
                p = (entry.get("buy_above") or entry.get("prev_close")
                     if isinstance(entry, dict) else entry)
                if p and float(p) > 0:
                    # back-calculate prev_close from buy_above if needed
                    if entry.get("buy_above") and entry.get("x_val"):
                        return float(entry["buy_above"]) - float(entry["x_val"])
                    return float(p)
    except Exception:
        pass
    # Fallback: use live price from ZMQ store snapshot if within 5% of prev_close
    return None


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    from market_calendar import MarketCalendar
    now = now_ist()

    log.info("═" * 66)
    log.info("%s — 5 symbols × %d variations = %d total", SCANNER_NAME, N_VALUES, N_VALUES * 5)
    log.info("X ranges: per-symbol 0.65×–1.35× of calibrated multiplier")
    log.info("Session: 09:00–23:30 IST | EOD: 23:30 IST | ZMQ topic: commodity")
    log.info("Logic: StockSweep (re-entry, retreat, T1-T5, SL, premarket, 09:30 re-anchor)")
    log.info("GPU: %s  |  CPU workers: %d", GPU_NAME if CUDA_AVAILABLE else "CPU", CPU_WORKERS)
    log.info("═" * 66)

    if not MarketCalendar.is_trading_day(now):
        # v10.6 FIX: sleep-wait until next MCX trading day instead of exiting.
        # This keeps the process alive so autohealer doesn't spam restarts.
        while not MarketCalendar.is_trading_day(now_ist()):
            nd = MarketCalendar.next_trading_day(now_ist())
            log.info("MCX Scanner1 standby — not a trading day. Next: %s  (sleeping 5 min)",
                     nd.strftime("%a %d %b %Y"))
            time.sleep(300)
        log.info("MCX Scanner1 — trading day detected, starting sweep...")
        now = now_ist()  # refresh after sleep

    date_str = now.strftime("%Y%m%d")
    out_dir  = os.path.join(RESULTS_DIR, date_str)
    os.makedirs(out_dir, exist_ok=True)

    # ── Load prev closes ──────────────────────────────────────────────────────
    log.info("Loading MCX previous closes...")
    prev_closes = _load_prev_closes(date_str)

    sweeps: Dict[str, StockSweep] = {}
    for sym in SYMBOLS:
        pc = prev_closes.get(sym, 0.0)
        if pc <= 0:
            log.warning("  SKIP %-14s — no prev_close", sym)
            continue
        xv = X_ARRAYS[sym]
        sw = StockSweep(sym, pc, xv, is_commodity=True)
        sweeps[sym] = sw
        log.info("  OK   %-14s  prev_close=%.2f  X=[%.6f–%.6f]  live_x=%.6f",
                 sym, pc, float(xv[0]), float(xv[-1]), CURRENT_X.get(sym, 0))

    if not sweeps:
        log.error("No symbols loaded — ensure commodity_engine.py is running")
        sys.exit(1)

    # ── ZMQ price subscriber ──────────────────────────────────────────────────
    csv_path  = os.path.join(out_dir, "prices.csv")
    store     = PriceStore()
    collector = PriceSubscriber(store, csv_path, topic=ZMQ_TOPIC)
    collector.start()
    log.info("Price subscriber started — topic=%s", ZMQ_TOPIC.decode())

    # ── Loop state ────────────────────────────────────────────────────────────
    sweep_ms        = 0.0
    updated         = 0
    eod_done        = False
    anchor_done     = False
    last_state_dump = 0.0
    console         = Console()

    try:
        with Live(
            _build_table(sweeps, now, 0.0, 0, 0),
            console=console, refresh_per_second=2,
        ) as live:
            while True:
                now = now_ist()

                # ── Periodic state dump ───────────────────────────────────────
                if time.monotonic() - last_state_dump > STATE_DUMP_INTERVAL:
                    try:
                        _dump_live_state(sweeps, date_str, out_dir)
                        last_state_dump = time.monotonic()
                    except Exception as exc:
                        log.debug("State dump error: %s", exc)

                # ── EOD: MCX 23:30 ───────────────────────────────────────────
                if is_commodity_eod(now) and not eod_done:
                    log.info("MCX EOD 23:30 — squaring off all positions")
                    prices = store.snapshot()
                    for sym, sw in sweeps.items():
                        px = prices.get(sym, sw.last_price)
                        if px and px > 0:
                            sw.eod_square_off(px, now)
                    save_results(sweeps, date_str, out_dir, scanner_name=SCANNER_NAME)
                    eod_done = True
                    _tg_async(f"✅ {SCANNER_NAME} MCX EOD 23:30 — results saved")
                    log.info("EOD complete. Scanner idling.")

                # ── Outside MCX session: idle ─────────────────────────────────
                if not in_commodity_session(now):
                    live.update(_build_table(sweeps, now, sweep_ms, updated, store.ticks))
                    time.sleep(30)
                    continue

                # ── Premarket: level adjustment 09:00–09:30 ───────────────────
                if in_premarket(now) and not anchor_done:
                    prices = store.snapshot()
                    if prices:
                        for sym, sw in sweeps.items():
                            px = prices.get(sym)
                            if px and px > 0:
                                sw.last_price = px
                                # Commodity premarket adjust — mirrors equity logic
                                sw.premarket_adjust(px)
                    live.update(_build_table(sweeps, now, sweep_ms, updated, store.ticks))
                    time.sleep(PRICE_FETCH_INTERVAL)
                    continue

                # ── 09:30 re-anchor (one-time) ────────────────────────────────
                if after_930(now) and not anchor_done:
                    log.info("09:30 re-anchor: fetching commodity 09:30 prices...")

                    def _anc(sym_sw: Tuple[str, StockSweep]):
                        sym, sw = sym_sw
                        # Try commodity-specific 09:30 price first
                        p930 = _fetch_930_price_comm(sym, sw.prev_close)
                        # Fallback: use current live price if close to prev_close
                        if p930 is None:
                            prices = store.snapshot()
                            px     = prices.get(sym)
                            if px and px > 0 and abs(px - sw.prev_close) / sw.prev_close < 0.05:
                                p930 = px
                        return sym, p930

                    with ThreadPoolExecutor(max_workers=min(5, len(sweeps))) as ex:
                        anchors = list(ex.map(_anc, sweeps.items()))

                    for sym, p930 in anchors:
                        if p930 and p930 > 0:
                            sweeps[sym].reanchor_at_930(p930)
                            log.info("  09:30 anchor: %-14s @ %.2f", sym, p930)
                        else:
                            sweeps[sym]._levels_locked = True
                            log.debug("  09:30 anchor: %-14s — using prev_close levels", sym)
                    anchor_done = True
                    log.info("09:30 re-anchor complete.")

                # ── Regular trading loop ──────────────────────────────────────
                prices = store.snapshot()
                if not prices:
                    live.update(_build_table(sweeps, now, sweep_ms, updated, store.ticks))
                    time.sleep(1)
                    continue

                t0 = time.perf_counter()
                updated = 0
                for sym, sw in sweeps.items():
                    if not in_trading_for(sym, now):
                        continue
                    px = prices.get(sym)
                    if px is not None and px > 0 and px != sw.last_price:
                        sw.last_price = px
                        sw.on_price(px, now)   # full logic: entries, T1-T5, retreat, SL, re-entry
                        updated += 1
                sweep_ms = (time.perf_counter() - t0) * 1000

                # ── RAM spill every 5 min ─────────────────────────────────────
                if now.second == 0 and now.minute % 5 == 0:
                    evicted = spill_trade_logs_to_disk(sweeps, date_str)
                    if evicted:
                        log.debug("RAM spill: %d entries evicted", evicted)

                live.update(_build_table(sweeps, now, sweep_ms, updated, store.ticks))
                time.sleep(PRICE_FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Interrupted — saving partial results...")
        if not eod_done:
            save_results(sweeps, date_str, out_dir, scanner_name=SCANNER_NAME)
    finally:
        collector.stop()
        log.info("%s stopped.", SCANNER_NAME)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
