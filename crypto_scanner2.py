# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v9.0 | Author: Ridhaant Ajoy Thackur
# crypto_scanner2.py — Crypto Dual-Band Sweep (same ranges as equity scanner2)
# ═══════════════════════════════════════════════════════════════════════
"""
crypto_scanner2.py — Crypto Dual-Band Sweep  v9.0
===================================================
Lower: X ∈ [0.001, 0.007]  6,000 vars/coin — SAME as equity scanner2 lower
Upper: X ∈ [0.009, 0.016]  7,000 vars/coin — SAME as equity scanner2 upper
5 coins × 13,000 = 65,000 variations per 6h window (260,000/day)

Full equity parity (all StockSweep logic including re-entry, retreat,
T1-T5/ST1-ST5, SL, re-anchor). 24/7 with 6h window rotation.
"""
from __future__ import annotations

import json
import logging
import multiprocessing
import os
import sys
import threading
import time
from datetime import datetime, timedelta
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
    now_ist, in_trading_for,
    IST, BROKERAGE_PER_SIDE, CPU_WORKERS, CUDA_AVAILABLE, GPU_NAME,
)
from config import cfg

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [CrS2] %(levelname)s — %(message)s")
log = logging.getLogger("crypto_scanner2")

SCANNER_NAME = "Crypto-Scanner-2 Dual"
SCANNER_ID   = "crypto2"
SYMBOLS      = ["BTC", "ETH", "BNB", "SOL", "ADA"]
ZMQ_TOPIC    = b"crypto"

# Same ranges as equity scanner2
X_LOWER_MIN=0.001000; X_LOWER_MAX=0.007000; N_LOWER=6_000
X_UPPER_MIN=0.009000; X_UPPER_MAX=0.016000; N_UPPER=7_000
X_LOWER_NP = np.linspace(X_LOWER_MIN, X_LOWER_MAX, N_LOWER)
X_UPPER_NP = np.linspace(X_UPPER_MIN, X_UPPER_MAX, N_UPPER)

REANCHOR_HOURS      = 6
STATE_DUMP_INTERVAL = 30
PRICE_FETCH_INTERVAL = 1.0
USDT_TO_INR  = cfg.USDT_TO_INR
CURRENT_X    = cfg.CRYPTO_X_MULTIPLIER

BASE_RESULTS_DIR = os.path.join("sweep_results", "crypto_scanner2")


def _tg_async(text: str) -> None:
    try:
        from tg_async import send_alert; send_alert(text, asset_class="crypto")
    except Exception: pass


def _window_tag() -> str:
    return datetime.now(pytz.utc).strftime("%Y%m%d_%H%M")


def _get_anchor_prices() -> Dict[str, float]:
    """Read anchor prices from crypto_engine output files (cross-process safe)."""
    stale_initial_s = 2 * 3600.0
    stale_live_s    = 90.0
    now_ts = time.time()

    # 1) Always load live prices (for reconciliation/fallback)
    lp = os.path.join("levels", "live_prices.json")
    cp: Dict[str, float] = {}
    if os.path.exists(lp):
        try:
            lp_age_s = now_ts - float(os.path.getmtime(lp))
            if lp_age_s > stale_live_s:
                log.warning("  live_prices.json is stale (age=%.0fs)", lp_age_s)
            data = json.load(open(lp, encoding="utf-8"))
            cp_raw = data.get("crypto_prices", {}) or {}
            cp = {s: float(cp_raw.get(s, 0) or 0) for s in SYMBOLS}
        except Exception:
            cp = {}

    # 2) Read initial anchors (more stable than raw live ticks)
    anchors: Dict[str, float] = {}
    path = os.path.join("levels", "crypto_initial_levels_latest.json")
    if os.path.exists(path):
        try:
            age_s = now_ts - float(os.path.getmtime(path))
            if age_s <= stale_initial_s:
                d = json.load(open(path, encoding="utf-8"))
                lvs = d.get("levels", {}) or {}
                anchors = {s: float(lvs.get(s, {}).get("anchor", 0) or 0) for s in SYMBOLS}
            else:
                log.warning("  Ignoring stale crypto_initial_levels_latest.json (age=%.0fs)", age_s)
        except Exception:
            anchors = {}

    # 3) Cross-check anchor vs live; if mismatch >10% use live for that coin
    if anchors and cp:
        for sym in SYMBOLS:
            a = float(anchors.get(sym, 0) or 0)
            c = float(cp.get(sym, 0) or 0)
            if a > 0 and c > 0:
                dev = abs(c - a) / a
                if dev > 0.10:
                    log.warning("  Crypto anchor mismatch %s: anchor=$%.4f live=$%.4f (dev=%.1f%%) — using live",
                                sym, a, c, dev * 100.0)
                    anchors[sym] = c

    if anchors and any(v > 0 for v in anchors.values()):
        return anchors
    if cp and any(v > 0 for v in cp.values()):
        return {s: float(cp.get(s, 0) or 0) for s in SYMBOLS}
    return {}


def _merged_best(sl: Dict[str, StockSweep], su: Dict[str, StockSweep]) -> Dict[str, dict]:
    merged: Dict[str, dict] = {}
    for sym in set(sl) | set(su):
        dl = sl[sym].dump_state() if sym in sl else {}
        du = su[sym].dump_state() if sym in su else {}
        pl = float(dl.get("best_pnl", -1e9))
        pu = float(du.get("best_pnl", -1e9))
        if pl >= pu:
            merged[sym] = {"best_x": dl.get("best_x",0), "pnl": pl, "band": "lower",
                           "win_rate": dl.get("best_win_rate",0), "trade_count": dl.get("best_trade_count",0)}
        else:
            merged[sym] = {"best_x": du.get("best_x",0), "pnl": pu, "band": "upper",
                           "win_rate": du.get("best_win_rate",0), "trade_count": du.get("best_trade_count",0)}
    return merged


def _dump_live_state(sl, su, date_tag, out_dir):
    merged = _merged_best(sl, su)
    state  = {"scanner": SCANNER_NAME, "scanner_id": SCANNER_ID, "asset_class": "crypto",
              "date_tag": date_tag, "written_at": now_ist().isoformat(),
              "merged_best": merged,
              "bands": {"lower": {"x_min": X_LOWER_MIN, "x_max": X_LOWER_MAX, "n": N_LOWER,
                                  "sweeps": {s: sw.dump_state() for s,sw in sl.items()}},
                        "upper": {"x_min": X_UPPER_MIN, "x_max": X_UPPER_MAX, "n": N_UPPER,
                                  "sweeps": {s: sw.dump_state() for s,sw in su.items()}}}}
    path=os.path.join(out_dir,"live_state.json"); tmp=path+".tmp"
    with open(tmp,"w",encoding="utf-8") as f: json.dump(state,f,separators=(",",":"))
    os.replace(tmp,path)


def _build_table(sl, su, now, sweep_ms, ticks, date_tag, mins_to_anchor):
    merged = _merged_best(sl, su)
    anchor_warn = mins_to_anchor <= 5
    t = Table(
        title=(f"[bold cyan]{SCANNER_NAME}  L:6K+U:7K/coin → 65K/window  "
               f"window:{date_tag}  "
               f"{'[bold red]' if anchor_warn else ''}re-anchor:{mins_to_anchor}m{'[/]' if anchor_warn else ''}  "
               f"— {now.strftime('%H:%M:%S IST')}  sweep={sweep_ms:.1f}ms  ticks={ticks}[/]"),
        show_header=True, header_style="bold yellow", border_style="dim", expand=False)
    t.add_column("Coin",     style="bold white", width=7)
    t.add_column("Anchor($)", style="dim",       width=12, justify="right")
    t.add_column("Last($)",   style="cyan",      width=12, justify="right")
    t.add_column("Best X",    style="green",     width=10, justify="right")
    t.add_column("Band",      style="magenta",   width=7)
    t.add_column("P&L (Rs)",  width=13,          justify="right")
    t.add_column("Win%",      width=7,           justify="right")
    t.add_column("Trades",    width=7,           justify="right")
    t.add_column("vs Live X", width=10,          justify="right")

    for sym in SYMBOLS:
        m  = merged.get(sym, {})
        sw = sl.get(sym) or su.get(sym)
        if not sw: continue
        pnl_inr = m.get("pnl", 0) * USDT_TO_INR
        bx  = m.get("best_x", 0)
        vs  = f"{(bx-CURRENT_X)/CURRENT_X*100:+.2f}%" if bx else "—"
        band = m.get("band", "—")
        t.add_row(
            sym, f"${sw.prev_close:,.4f}",
            f"${sw.last_price:,.4f}" if sw.last_price else "—",
            Text(f"{bx:.6f}" if bx else "—", style="green" if bx else "dim"),
            Text(band, style="cyan" if band=="upper" else "magenta"),
            Text(f"₹{pnl_inr:+,.0f}", style="green" if pnl_inr>=0 else "red"),
            f"{m.get('win_rate',0):.1f}%" if m.get("trade_count") else "—",
            str(m.get("trade_count","—")),
            Text(vs, style="yellow"),
        )
    return t


def _build_sweeps(anchors):
    sl: Dict[str, StockSweep] = {}
    su: Dict[str, StockSweep] = {}
    for sym in SYMBOLS:
        a = anchors.get(sym, 0.0)
        if a <= 0: log.warning("SKIP %s", sym); continue
        sl[sym] = StockSweep(sym, a, X_LOWER_NP, is_crypto=True)
        su[sym] = StockSweep(sym, a, X_UPPER_NP, is_crypto=True)
        log.info("  %-6s anchor=$%.4f", sym, a)
    return sl, su


def _rotate_window(sl, su, store, collector, date_tag, out_dir):
    """Square off open positions, save results, start fresh window."""
    log.info("6H RE-ANCHOR starting...")
    now_d  = now_ist()
    prices = store.snapshot()
    for sym in list(sl):
        px = prices.get(sym, sl[sym].last_price)
        if px and px > 0:
            # v10.9: No force-square on re-anchor — let natural exits handle positions
            pass  # sl[sym].eod_square_off(px, now_d) — disabled
    try:
        save_results(sl, date_tag, os.path.join(out_dir,"band_lower"),
                     scanner_name=f"{SCANNER_NAME}-Lower")
        save_results(su, date_tag, os.path.join(out_dir,"band_upper"),
                     scanner_name=f"{SCANNER_NAME}-Upper")
        xl = os.path.join(out_dir,"band_lower",f"summary_{date_tag}.xlsx")
        if os.path.exists(xl):
            from tg_async import send_document_alert
            send_document_alert(xl, f"{SCANNER_NAME} 6h report {date_tag}", asset_class="crypto")
    except Exception as e: log.debug("6h save: %s", e)
    try: collector.stop()
    except Exception: pass
    new_tag     = _window_tag()
    new_out_dir = os.path.join(BASE_RESULTS_DIR, new_tag)
    os.makedirs(new_out_dir, exist_ok=True)
    time.sleep(1)
    anchors = _get_anchor_prices()
    if not any(v > 0 for v in anchors.values()):
        time.sleep(5); anchors = _get_anchor_prices()
    new_sl, new_su = _build_sweeps(anchors)
    new_store     = PriceStore()
    new_csv       = os.path.join(new_out_dir, "prices.csv")
    new_collector = PriceSubscriber(new_store, new_csv, topic=ZMQ_TOPIC)
    new_collector.start()
    log.info("6H RE-ANCHOR complete — window %s", new_tag)
    _tg_async(f"🔄 {SCANNER_NAME} re-anchor → window {new_tag}")
    return new_sl, new_su, new_store, new_collector, new_tag, new_out_dir


def main() -> None:
    log.info("═"*68)
    log.info("%s | L:6K+U:7K/coin → 65K/6h  24/7", SCANNER_NAME)
    log.info("Same X ranges as equity scanner2 | ZMQ topic: crypto")
    log.info("Full logic: re-entry, retreat 65/45/25, T1-T5, SL, 6h re-anchor")
    log.info("═"*68)

    anchors: Dict[str, float] = {}
    _deadline = time.monotonic() + 120
    log.info("Waiting for crypto_engine anchor prices (up to 120s)...")
    while time.monotonic() < _deadline:
        anchors = _get_anchor_prices()
        if any(v > 0 for v in anchors.values()): break
        time.sleep(5)
    if not any(v > 0 for v in anchors.values()):
        log.warning("No anchors after 120s — waiting indefinitely")
        while True:
            anchors = _get_anchor_prices()
            if any(v > 0 for v in anchors.values()): break
            time.sleep(10)

    date_tag = _window_tag()
    out_dir  = os.path.join(BASE_RESULTS_DIR, date_tag)
    os.makedirs(out_dir, exist_ok=True)

    sl, su = _build_sweeps(anchors)
    if not sl: log.error("No symbols loaded"); sys.exit(1)

    csv_path  = os.path.join(out_dir, "prices.csv")
    store     = PriceStore()
    collector = PriceSubscriber(store, csv_path, topic=ZMQ_TOPIC)
    collector.start()

    next_anchor = datetime.now(pytz.utc) + timedelta(hours=REANCHOR_HOURS)
    last_dump   = 0.0; sweep_ms = 0.0; console = Console()

    try:
        with Live(console=console, refresh_per_second=2) as live:
            while True:
                now_utc = datetime.now(pytz.utc)
                now_d   = now_ist()

                # 6h re-anchor
                if now_utc >= next_anchor:
                    sl, su, store, collector, date_tag, out_dir = _rotate_window(
                        sl, su, store, collector, date_tag, out_dir)
                    next_anchor = now_utc + timedelta(hours=REANCHOR_HOURS)
                    last_dump   = 0.0

                # State dump
                if time.monotonic() - last_dump > STATE_DUMP_INTERVAL:
                    try: _dump_live_state(sl, su, date_tag, out_dir); last_dump = time.monotonic()
                    except Exception as e: log.debug("Dump: %s", e)

                # Main sweep — both bands in parallel
                prices = store.snapshot()
                if prices and sl:
                    t0 = time.perf_counter()
                    def _lo():
                        for sym, sw in sl.items():
                            px = prices.get(sym)
                            if px and px > 0 and px != sw.last_price:
                                sw.last_price = px; sw.on_price(px, now_d)
                    def _hi():
                        for sym, sw in su.items():
                            px = prices.get(sym)
                            if px and px > 0 and px != sw.last_price:
                                sw.last_price = px; sw.on_price(px, now_d)
                    tl = threading.Thread(target=_lo, daemon=True)
                    tu = threading.Thread(target=_hi, daemon=True)
                    tl.start(); tu.start(); tl.join(); tu.join()
                    sweep_ms = (time.perf_counter() - t0) * 1000

                # RAM spill
                if now_d.second == 0 and now_d.minute % 5 == 0:
                    evicted = (spill_trade_logs_to_disk(sl, date_tag) +
                               spill_trade_logs_to_disk(su, date_tag))
                    if evicted: log.debug("RAM spill: %d", evicted)

                mins = max(0, int((next_anchor-now_utc).total_seconds()//60))
                live.update(_build_table(sl, su, now_d, sweep_ms, store.ticks, date_tag, mins))
                time.sleep(PRICE_FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Stopped")
        try:
            save_results(sl, date_tag, os.path.join(out_dir,"band_lower"), scanner_name=f"{SCANNER_NAME}-Lower")
            save_results(su, date_tag, os.path.join(out_dir,"band_upper"), scanner_name=f"{SCANNER_NAME}-Upper")
        except Exception: pass
    finally:
        collector.stop()


if __name__ == "__main__":
    multiprocessing.freeze_support(); main()
