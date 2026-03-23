# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v9.0 | Author: Ridhaant Ajoy Thackur
# commodity_scanner2.py — MCX Dual-Band Sweep (3K lower + 3.5K upper = 6.5K/sym, 32.5K total)
# ═══════════════════════════════════════════════════════════════════════
"""
commodity_scanner2.py — MCX Dual-Band Sweep  v9.0
===================================================
Lower band: conservative/tight X values below calibrated COMM_X
Upper band: aggressive/wide X values above calibrated COMM_X
5 symbols × 6,500 = 32,500 total variations/day

Full equity parity:
  ✓ Premarket adjust  ✓ 09:30 re-anchor  ✓ Re-entry  ✓ Retreat 65/45/25
  ✓ T1–T5 / ST1–ST5  ✓ SL exits          ✓ EOD 23:30  ✓ RAM spill
  ✓ Dual-band merged best (picks winner from lower vs upper per symbol)
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
    now_ist, in_premarket, after_930, in_trading_for,
    in_commodity_session, is_commodity_eod,
    IST, BROKERAGE_PER_SIDE, CPU_WORKERS, CUDA_AVAILABLE, GPU_NAME,
)
from config import cfg

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [CommS2] %(levelname)s — %(message)s")
log = logging.getLogger("commodity_scanner2")

SCANNER_NAME = "Comm-Scanner-2 Dual"
SCANNER_ID   = "comm2"
SYMBOLS      = ["GOLD", "SILVER", "CRUDE", "NATURALGAS", "COPPER"]
ZMQ_TOPIC    = b"commodity"
RESULTS_DIR  = os.path.join("sweep_results", "commodity_scanner2")
PRICE_FETCH_INTERVAL = 1.0
STATE_DUMP_INTERVAL  = 30

# Dual-band X ranges
COMM_LOWER: Dict[str, Tuple[float, float, int]] = {
    "GOLD":       (0.001000, 0.002400, 3_000),
    "SILVER":     (0.001500, 0.003600, 3_000),
    "NATURALGAS": (0.000250, 0.000600, 3_000),
    "CRUDE":      (0.000180, 0.000420, 3_000),
    "COPPER":     (0.001200, 0.002800, 3_000),
}
COMM_UPPER: Dict[str, Tuple[float, float, int]] = {
    "GOLD":       (0.004100, 0.006860, 3_500),
    "SILVER":     (0.006174, 0.010290, 3_500),
    "NATURALGAS": (0.001029, 0.001715, 3_500),
    "CRUDE":      (0.000722, 0.001204, 3_500),
    "COPPER":     (0.004800, 0.008000, 3_500),
}

CURRENT_X = cfg.COMM_X


def _tg_async(text: str) -> None:
    try:
        from tg_async import send_alert
        send_alert(text, asset_class="commodity")
    except Exception:
        pass


def _merged_best(
    sl: Dict[str, StockSweep], su: Dict[str, StockSweep]
) -> Dict[str, dict]:
    """Pick band with higher net P&L per symbol."""
    merged: Dict[str, dict] = {}
    for sym in set(sl) | set(su):
        dl = sl[sym].dump_state() if sym in sl else {}
        du = su[sym].dump_state() if sym in su else {}
        pl = float(dl.get("best_pnl", -1e9))
        pu = float(du.get("best_pnl", -1e9))
        if pl >= pu:
            merged[sym] = {
                "best_x": dl.get("best_x", 0), "pnl": pl, "band": "lower",
                "win_rate": dl.get("best_win_rate", 0),
                "trade_count": dl.get("best_trade_count", 0),
            }
        else:
            merged[sym] = {
                "best_x": du.get("best_x", 0), "pnl": pu, "band": "upper",
                "win_rate": du.get("best_win_rate", 0),
                "trade_count": du.get("best_trade_count", 0),
            }
    return merged


def _dump_live_state(
    sl: Dict[str, StockSweep], su: Dict[str, StockSweep],
    date_str: str, out_dir: str,
) -> None:
    merged = _merged_best(sl, su)
    state  = {
        "scanner":    SCANNER_NAME,
        "scanner_id": SCANNER_ID,
        "asset_class": "commodity",
        "date":       date_str,
        "written_at": now_ist().isoformat(),
        "total_variations": sum(
            COMM_LOWER[s][2] + COMM_UPPER[s][2] for s in SYMBOLS
        ),
        "merged_best": merged,
        "bands": {
            "lower": {"sweeps": {s: sw.dump_state() for s, sw in sl.items()}},
            "upper": {"sweeps": {s: sw.dump_state() for s, sw in su.items()}},
        },
    }
    path = os.path.join(out_dir, "live_state.json")
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, separators=(",", ":"))
    os.replace(tmp, path)


def _build_table(
    sl: Dict[str, StockSweep], su: Dict[str, StockSweep],
    now: datetime, sweep_ms: float, ticks: int,
) -> Table:
    merged = _merged_best(sl, su)
    t = Table(
        title=(
            f"[bold cyan]{SCANNER_NAME}  Lower:3K+Upper:3.5K=6.5K/sym → 32.5K total  "
            f"— {now.strftime('%H:%M:%S IST')}  sweep={sweep_ms:.1f}ms  ticks={ticks}[/]"
        ),
        show_header=True, header_style="bold yellow", border_style="dim", expand=False,
    )
    t.add_column("Symbol",      style="bold white", width=14)
    t.add_column("Prev Close",  style="dim",        width=12, justify="right")
    t.add_column("Last Price",  style="cyan",       width=12, justify="right")
    t.add_column("Best X",      style="green",      width=10, justify="right")
    t.add_column("Band",        style="magenta",    width=7)
    t.add_column("P&L",         width=13,           justify="right")
    t.add_column("Win%",        width=7,            justify="right")
    t.add_column("Trades",      width=7,            justify="right")
    t.add_column("vs Live X",   width=10,           justify="right")

    for sym in SYMBOLS:
        m  = merged.get(sym, {})
        sw = sl.get(sym) or su.get(sym)
        if not sw:
            continue
        pnl  = m.get("pnl", 0.0)
        bx   = m.get("best_x", 0.0)
        lx   = CURRENT_X.get(sym, cfg.CURRENT_X_MULTIPLIER)
        vs   = f"{(bx - lx)/lx*100:+.2f}%" if bx else "—"
        band = m.get("band", "—")
        t.add_row(
            sym,
            f"{sw.prev_close:,.2f}",
            f"{sw.last_price:,.2f}" if sw.last_price else "—",
            Text(f"{bx:.6f}" if bx else "—", style="green" if bx else "dim"),
            Text(band, style="cyan" if band == "upper" else "magenta"),
            Text(f"₹{pnl:+,.0f}", style="green" if pnl >= 0 else "red"),
            f"{m.get('win_rate', 0):.1f}%" if m.get("trade_count") else "—",
            str(m.get("trade_count", "—")),
            Text(vs, style="yellow"),
        )
    return t


def _load_prev_closes(date_str: str) -> Dict[str, float]:
    """Same loader as scanner1."""
    result: Dict[str, float] = {}
    stale_levels_s = 36 * 3600.0  # ignore old anchors >36h
    now_ts = time.time()
    for fname in (f"commodity_initial_levels_{date_str}.json",):
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
                    e  = lvs.get(sym, {})
                    pc = (e.get("prev_close") or e.get("anchor")) if isinstance(e, dict) else e
                    if pc and float(pc) > 0:
                        result[sym] = float(pc)
                if result:
                    return result
            except Exception:
                pass
    _YF = {"GOLD": "GC=F", "SILVER": "SI=F", "CRUDE": "CL=F",
           "NATURALGAS": "NG=F", "COPPER": "HG=F"}
    for sym in SYMBOLS:
        if sym in result:
            continue
        try:
            import yfinance as yf
            df = yf.Ticker(_YF.get(sym, f"{sym}=F")).history(period="5d", interval="1d")
            if df is not None and not df.empty:
                result[sym] = float(df["Close"].iloc[-1])
        except Exception:
            pass

    # 3. live_prices.json fallback (last-known USDT prices from CommodityEngine)
    #    Makes scanner resilient when initial_levels files are missing/stale.
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


def main() -> None:
    from market_calendar import MarketCalendar
    now = now_ist()

    log.info("═" * 68)
    log.info("%s | Lower:3K+Upper:3.5K per sym → 32.5K total", SCANNER_NAME)
    log.info("Logic: full re-entry, retreat, T1-T5, SL, premarket, 09:30 re-anchor")
    log.info("═" * 68)

    if not MarketCalendar.is_trading_day(now):
        # v10.6 FIX: sleep-wait until next MCX trading day instead of exiting.
        while not MarketCalendar.is_trading_day(now_ist()):
            nd = MarketCalendar.next_trading_day(now_ist())
            log.info("MCX Scanner2 standby — not a trading day. Next: %s  (sleeping 5 min)",
                     nd.strftime("%a %d %b %Y"))
            time.sleep(300)
        log.info("MCX Scanner2 — trading day detected, starting sweep...")
        now = now_ist()

    date_str = now.strftime("%Y%m%d")
    out_dir  = os.path.join(RESULTS_DIR, date_str)
    os.makedirs(out_dir, exist_ok=True)

    log.info("Loading MCX previous closes...")
    pcs = _load_prev_closes(date_str)

    sl: Dict[str, StockSweep] = {}
    su: Dict[str, StockSweep] = {}
    for sym in SYMBOLS:
        pc = pcs.get(sym, 0.0)
        if pc <= 0:
            log.warning("  SKIP %-14s", sym)
            continue
        lo, hi, nl = COMM_LOWER[sym]
        lo2, hi2, nu = COMM_UPPER[sym]
        sl[sym] = StockSweep(sym, pc, np.linspace(lo, hi, nl),  is_commodity=True)
        su[sym] = StockSweep(sym, pc, np.linspace(lo2, hi2, nu), is_commodity=True)
        log.info("  OK %-14s pc=%.2f L=[%.6f–%.6f] U=[%.6f–%.6f]",
                 sym, pc, lo, hi, lo2, hi2)

    if not sl:
        log.error("No symbols loaded — ensure commodity_engine.py is running")
        sys.exit(1)

    csv_path  = os.path.join(out_dir, "prices.csv")
    store     = PriceStore()
    collector = PriceSubscriber(store, csv_path, topic=ZMQ_TOPIC)
    collector.start()

    eod_done = False; anchor_done = False
    last_dump = 0.0; sweep_ms = 0.0; console = Console()

    try:
        with Live(
            _build_table(sl, su, now, 0.0, 0),
            console=console, refresh_per_second=2,
        ) as live:
            while True:
                now = now_ist()

                # State dump
                if time.monotonic() - last_dump > STATE_DUMP_INTERVAL:
                    try:
                        _dump_live_state(sl, su, date_str, out_dir)
                        last_dump = time.monotonic()
                    except Exception as exc:
                        log.debug("Dump error: %s", exc)

                # MCX EOD 23:30
                if is_commodity_eod(now) and not eod_done:
                    log.info("MCX EOD 23:30 — squaring off both bands")
                    prices = store.snapshot()
                    for sym in list(sl):
                        px = prices.get(sym, sl[sym].last_price)
                        if px and px > 0:
                            sl[sym].eod_square_off(px, now)
                            su[sym].eod_square_off(px, now)
                    save_results(sl, date_str, os.path.join(out_dir, "band_lower"),
                                 scanner_name=f"{SCANNER_NAME}—Lower")
                    save_results(su, date_str, os.path.join(out_dir, "band_upper"),
                                 scanner_name=f"{SCANNER_NAME}—Upper")
                    eod_done = True
                    _tg_async(f"✅ {SCANNER_NAME} MCX EOD 23:30 complete")

                # Off-hours idle
                if not in_commodity_session(now):
                    live.update(_build_table(sl, su, now, sweep_ms, store.ticks))
                    time.sleep(30)
                    continue

                # Premarket adjust 09:00–09:30
                if in_premarket(now) and not anchor_done:
                    prices = store.snapshot()
                    if prices:
                        for sym in sl:
                            px = prices.get(sym)
                            if px and px > 0:
                                sl[sym].last_price = px
                                su[sym].last_price = px
                                sl[sym].premarket_adjust(px)
                                su[sym].premarket_adjust(px)
                    live.update(_build_table(sl, su, now, sweep_ms, store.ticks))
                    time.sleep(PRICE_FETCH_INTERVAL)
                    continue

                # 09:30 re-anchor
                if after_930(now) and not anchor_done:
                    log.info("09:30 re-anchor (both bands)...")
                    prices = store.snapshot()
                    for sym in sl:
                        px = prices.get(sym)
                        if px and px > 0:
                            sl[sym].reanchor_at_930(px)
                            su[sym].reanchor_at_930(px)
                        else:
                            sl[sym]._levels_locked = True
                            su[sym]._levels_locked = True
                    anchor_done = True
                    log.info("09:30 re-anchor complete (both bands)")

                # Main sweep — both bands in parallel threads
                prices = store.snapshot()
                if prices:
                    t0 = time.perf_counter()

                    def _sweep_lower():
                        for sym, sw in sl.items():
                            if not in_trading_for(sym, now): continue
                            px = prices.get(sym)
                            if px and px > 0:
                                sw.last_price = px
                                sw.on_price(px, now)

                    def _sweep_upper():
                        for sym, sw in su.items():
                            if not in_trading_for(sym, now): continue
                            px = prices.get(sym)
                            if px and px > 0:
                                sw.last_price = px
                                sw.on_price(px, now)

                    tl = threading.Thread(target=_sweep_lower, daemon=True)
                    tu = threading.Thread(target=_sweep_upper, daemon=True)
                    tl.start(); tu.start(); tl.join(); tu.join()
                    sweep_ms = (time.perf_counter() - t0) * 1000

                # RAM spill
                if now.second == 0 and now.minute % 5 == 0:
                    evicted = spill_trade_logs_to_disk(sl, date_str) + \
                              spill_trade_logs_to_disk(su, date_str)
                    if evicted:
                        log.debug("RAM spill: %d entries", evicted)

                live.update(_build_table(sl, su, now, sweep_ms, store.ticks))
                time.sleep(PRICE_FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Stopped — saving partial results")
        save_results(sl, date_str, os.path.join(out_dir, "band_lower"),
                     scanner_name=f"{SCANNER_NAME}—Lower")
        save_results(su, date_str, os.path.join(out_dir, "band_upper"),
                     scanner_name=f"{SCANNER_NAME}—Upper")
    finally:
        collector.stop()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
