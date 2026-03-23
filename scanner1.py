"""
scanner1.py — Narrow Sweep  (X: 0.0080 – 0.0090, 1 000 variations)
====================================================================
Run AFTER Algofinal.py is started.

Startup order:
    1. python Algofinal.py       (ZMQ publisher + unified dashboard on :8050)
    2. python scanner1.py        (this file)
    3. python scanner2.py
    4. python scanner3.py
    5. python x.py

What this scanner does:
  • Fine-tuning search: tests 1,000 X multipliers in the 0.0080–0.0090 band.
  • Identifies whether the live X=0.008575 is near-optimal or needs adjustment.
  • Receives live prices from Algofinal via ZMQ PUB (microsecond latency).
  • Writes live_state.json every 30 s → x.py reads it for cross-scanner ranking.
  • Saves per-X CSV + summary XLSX at 15:11 (equity) and 23:00 (commodity).

Public dashboard: single tunnel from unified_dash served by Algofinal on :8050.
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
import pandas as pd
import pytz
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

# ── Shared sweep engine ───────────────────────────────────────────────────────
from sweep_core import (
    StockSweep, PriceStore, PriceSubscriber,
    read_prev_closes_from_algofinal, fetch_prev_close, fetch_930_price,
    save_results, spill_trade_logs_to_disk,
    now_ist, in_session, in_premarket, after_930, in_trading_for,
    IST, BROKERAGE_PER_SIDE, STOCKS, COMMODITY_SYMBOLS,
    CPU_WORKERS, CUDA_AVAILABLE, GPU_NAME,
)

# ════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════

SCANNER_NAME  = "Scanner-1 Narrow"
SCANNER_ID    = 1

# ── X range for non-index equities ──────────────────────────────────────────
X_MIN         = 0.008000
X_MAX         = 0.009000
N_VALUES      = 1_000
X_VALUES_NP   = np.linspace(X_MIN, X_MAX, N_VALUES)

# ── Index symbols use a different X range (mirrors INDEX_X_MULTIPLIER=0.00343)
IX_MIN        = 0.003000
IX_MAX        = 0.004000
IX_VALUES     = 1_000
IX_VALUES_NP  = np.linspace(IX_MIN, IX_MAX, IX_VALUES)
INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY"}

# ── Commodity X ranges ────────────────────────────────────────────────────────
COMMODITY_X_RANGES: Dict[str, Tuple[float, float, int]] = {
    "GOLD":       (0.003000, 0.004000, 1_000),
    "SILVER":     (0.005000, 0.006000, 1_000),
    "NATURALGAS": (0.008000, 0.009000, 1_000),
    "CRUDE":      (0.006000, 0.007000, 1_000),
    "COPPER":     (0.004000, 0.005000, 1_000),
}
COMMODITY_X_NP: Dict[str, np.ndarray] = {
    sym: np.linspace(lo, hi, n)
    for sym, (lo, hi, n) in COMMODITY_X_RANGES.items()
}

RESULTS_DIR          = os.path.join("sweep_results", "scanner1_narrow_x0080_x0090")
PRICE_FETCH_INTERVAL = 1.0   # seconds between ticks
STATE_DUMP_INTERVAL  = 30    # seconds between live_state.json dumps

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7587307352:AAG6RaiF4gO5I_ZFZ_4b8Gj7dnsu4GtPWFw")
TELEGRAM_CHATS = [c for c in [
    os.getenv("TELEGRAM_CHAT_ID", "1376513391"),
    "793674804",
] if c]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S1-Narrow] %(levelname)s — %(message)s",
)
log = logging.getLogger("scanner1")


# ════════════════════════════════════════════════════════════════════════════
# TELEGRAM (async, non-blocking)
# ════════════════════════════════════════════════════════════════════════════

def _tg_async(text: str) -> None:
    """Send alert via unified equity Telegram bot."""
    try:
        from tg_async import send_alert
        send_alert(text, asset_class="equity")
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════
# RICH DISPLAY
# ════════════════════════════════════════════════════════════════════════════

def build_table(sweeps: Dict[str, StockSweep], now: datetime,
                sweep_ms: float, updated: int, ticks: int) -> Table:
    table = Table(
        title=(
            f"[bold cyan]{SCANNER_NAME} (X:{X_MIN:.3f}–{X_MAX:.3f}, "
            f"{N_VALUES:,} vars) — {now.strftime('%H:%M:%S IST')}  "
            f"sweep={sweep_ms:.1f}ms  updated={updated}/{len(sweeps)}  ticks={ticks}[/]"
        ),
        show_header=True, header_style="bold yellow",
        border_style="dim", expand=False,
    )
    table.add_column("Symbol",        style="bold white", width=14)
    table.add_column("Prev Close",    style="dim",        width=11, justify="right")
    table.add_column("Last Price",    style="cyan",       width=11, justify="right")
    table.add_column("Best X",        style="green",      width=10, justify="right")
    table.add_column("vs Current",    style="yellow",     width=11, justify="right")
    table.add_column("P&L",           width=12,           justify="right")
    table.add_column("Win%",          width=7,            justify="right")
    table.add_column("Trades",        width=7,            justify="right")
    table.add_column("W/L",           width=7,            justify="right")
    table.add_column("Last Breached", width=28)

    for sym, sw in sweeps.items():
        r   = sw.row_data()
        pnl = r["total_pnl"]
        if pnl != "—":
            val = float(pnl.replace("₹","").replace(",",""))
            pd_ = Text(pnl, style="green" if val >= 0 else "red")
        else:
            pd_ = Text("—", style="dim")
        wl = "—"
        if r["win_count"] != "—" and r["loss_count"] != "—":
            wl = f"{r['win_count']}/{r['loss_count']}"
        lv = r["last_event"]
        lv_style = "dim"
        if lv != "—":
            if "SL" in lv or "RETREAT" in lv: lv_style = "red"
            elif any(x in lv for x in ("T1","T2","T3","T4","T5","ST")): lv_style = "green"
            elif "ENTRY" in lv: lv_style = "cyan"
            elif "EOD" in lv:   lv_style = "yellow"
        table.add_row(
            r["symbol"], r["prev_close"], r["last_price"],
            Text(r["best_x"],       style="green" if r["has_data"] else "dim"),
            Text(r["vs_current"],   style="yellow" if r["has_data"] else "dim"),
            pd_,
            Text(r["win_rate_pct"], style="cyan"  if r["has_data"] else "dim"),
            Text(r["trade_count"],  style="white" if r["has_data"] else "dim"),
            Text(wl,                style="white" if r["has_data"] else "dim"),
            Text(lv,                style=lv_style),
        )
    return table


# ════════════════════════════════════════════════════════════════════════════
# STATE DUMP (for x.py)
# ════════════════════════════════════════════════════════════════════════════

def _dump_live_state(sweeps: Dict[str, StockSweep], date_str: str,
                     out_dir: str) -> None:
    state = {
        "scanner": SCANNER_NAME, "date": date_str,
        "written_at": now_ist().isoformat(),
        "sweeps": {sym: sw.dump_state() for sym, sw in sweeps.items()},
    }
    path = os.path.join(out_dir, "live_state.json")
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, separators=(",", ":"))
    os.replace(tmp, path)


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    # Pin to designated CPU core for optimal performance
    try:
        from process_affinity import pin_process
        pin_process("Scanner1")
    except Exception:
        pass
    log.info("═" * 64)
    log.info("%s — X: %.4f–%.4f  (%d variations)", SCANNER_NAME, X_MIN, X_MAX, N_VALUES)
    log.info("GPU: %s", GPU_NAME if CUDA_AVAILABLE else "CPU (NumPy)")
    log.info("CPU workers (EOD writing): %d", CPU_WORKERS)
    log.info("Price feed: ZMQ PUB from Algofinal → zero yfinance calls")
    log.info("═" * 64)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    date_str = now_ist().strftime("%Y%m%d")
    out_dir  = os.path.join(RESULTS_DIR, date_str)
    os.makedirs(out_dir, exist_ok=True)

    # ── 1. Prev-close loader ──────────────────────────────────────────────────
    log.info("Loading previous closes from Algofinal levels file...")
    algofinal_pcs = read_prev_closes_from_algofinal(date_str)

    def _get_pc(sym: str) -> Tuple[str, Optional[float], str]:
        pc = algofinal_pcs.get(sym.upper())
        if pc and pc > 0:
            return sym, pc, "levels"
        pc = fetch_prev_close(sym)
        return sym, pc, "yfinance"

    with ThreadPoolExecutor(max_workers=min(20, len(STOCKS) + len(COMMODITY_SYMBOLS))) as ex:
        all_syms = STOCKS + (COMMODITY_SYMBOLS if os.getenv("ENABLE_COMMODITIES") == "1" else [])
        results  = list(ex.map(_get_pc, all_syms))

    sweeps: Dict[str, StockSweep] = {}
    for sym, pc, src in results:
        if pc is None:
            log.warning("  SKIP %-14s — prev_close unavailable", sym)
            continue
        is_comm = sym in COMMODITY_SYMBOLS
        if is_comm:
            xv = COMMODITY_X_NP.get(sym, X_VALUES_NP)
        elif sym in INDEX_SYMBOLS:
            xv = IX_VALUES_NP
        else:
            xv = X_VALUES_NP
        sweeps[sym] = StockSweep(sym, pc, xv, is_commodity=is_comm)
        log.info("  OK   %-14s  prev_close=%.2f  x_range=[%.4f–%.4f]  [%s]",
                 sym, pc, float(xv[0]), float(xv[-1]), src)

    log.info("Sources: %d from levels file, %d from yfinance",
             sum(1 for *_, s in results if s == "levels"),
             sum(1 for *_, s in results if s == "yfinance"))

    if not sweeps:
        log.error("No symbols loaded. Start Algofinal.py first, then retry.")
        sys.exit(1)

    # ── 2. ZMQ/JSON price subscriber ─────────────────────────────────────────
    csv_path  = os.path.join(out_dir, "prices.csv")
    store     = PriceStore()
    collector = PriceSubscriber(store, csv_path)
    collector.start()

    log.info("Waiting for Algofinal to write live prices...")

    # ── 3. Main loop ──────────────────────────────────────────────────────────
    console          = Console()
    sweep_ms         = 0.0
    updated          = 0
    eod_done         = False
    commodity_eod    = False
    anchor_done      = False
    last_state_dump  = 0.0

    try:
        with Live(build_table(sweeps, now_ist(), 0.0, 0, 0),
                  console=console, refresh_per_second=2) as live:
            while True:
                now = now_ist()

                # ── Periodic live_state.json dump (for x.py) ─────────────────
                if time.monotonic() - last_state_dump > STATE_DUMP_INTERVAL:
                    try:
                        _dump_live_state(sweeps, date_str, out_dir)
                        last_state_dump = time.monotonic()
                    except Exception:
                        pass

                # ── EOD: equity 15:11 ─────────────────────────────────────────
                if now.hour == 15 and now.minute >= 11 and not eod_done:
                    log.info("Equity EOD: squaring off open positions at 15:11...")
                    prices = store.snapshot()
                    for sym, sw in sweeps.items():
                        if sw.is_commodity:
                            continue
                        px = prices.get(sym, sw.last_price)
                        if px:
                            sw.eod_square_off(px, now)
                    eq_sweeps = {s: sw for s, sw in sweeps.items() if not sw.is_commodity}
                    save_results(eq_sweeps, date_str, out_dir,
                                 scanner_name=SCANNER_NAME)
                    eod_done = True
                    _tg_async(f"✅ {SCANNER_NAME} Equity EOD complete. Results → {out_dir}")

                # ── EOD: commodity 23:00 ───────────────────────────────────────
                if now.hour == 23 and now.minute == 0 and not commodity_eod:
                    prices = store.snapshot()
                    comm_sweeps = {s: sw for s, sw in sweeps.items() if sw.is_commodity}
                    if comm_sweeps:
                        for sym, sw in comm_sweeps.items():
                            px = prices.get(sym, sw.last_price)
                            if px:
                                sw.eod_square_off(px, now)
                        comm_dir = os.path.join(out_dir, "commodities")
                        save_results(comm_sweeps, date_str, comm_dir,
                                     scanner_name=f"{SCANNER_NAME} (Commodity)")
                    commodity_eod = True

                # ── Outside session: idle (saves CPU) ─────────────────────────
                if not in_session(now):
                    live.update(build_table(sweeps, now, sweep_ms, updated, store.ticks))
                    time.sleep(30)
                    continue

                # ── Premarket: level adjustment 09:15–09:30 ───────────────────
                if in_premarket(now) and not anchor_done:
                    prices = store.snapshot()
                    if prices:
                        for sym, sw in sweeps.items():
                            px = prices.get(sym)
                            if px:
                                sw.last_price = px
                                sw.premarket_adjust(px)
                    live.update(build_table(sweeps, now, sweep_ms, updated, store.ticks))
                    time.sleep(PRICE_FETCH_INTERVAL)
                    continue

                # ── 09:30 re-anchor (one-time) ────────────────────────────────
                if after_930(now) and not anchor_done:
                    log.info("09:30 re-anchor: fetching actual 09:30 prices...")
                    def _anc(sym_sw):
                        sym, sw = sym_sw
                        p = fetch_930_price(sym, sw.prev_close)
                        return sym, p
                    with ThreadPoolExecutor(max_workers=min(20, len(sweeps))) as ex:
                        anchors = list(ex.map(_anc, sweeps.items()))
                    for sym, p930 in anchors:
                        if p930:
                            sweeps[sym].reanchor_at_930(p930)
                        else:
                            sweeps[sym]._levels_locked = True
                    anchor_done = True
                    log.info("09:30 re-anchor complete.")

                # ── Regular trading loop ───────────────────────────────────────
                prices = store.snapshot()
                if not prices:
                    live.update(build_table(sweeps, now, sweep_ms, updated, store.ticks))
                    time.sleep(1)
                    continue

                t0 = time.perf_counter()
                updated = 0
                for sym, sw in sweeps.items():
                    if not in_trading_for(sym, now):
                        continue
                    px = prices.get(sym)
                    if px is not None and px != sw.last_price:
                        sw.last_price = px
                        sw.on_price(px, now)
                        updated += 1
                sweep_ms = (time.perf_counter() - t0) * 1000

                # ── RAM spill every 5 min ─────────────────────────────────────
                if now.second == 0 and now.minute % 5 == 0:
                    evicted = spill_trade_logs_to_disk(sweeps, date_str)
                    if evicted:
                        log.debug("RAM spill: evicted %d old log entries", evicted)

                live.update(build_table(sweeps, now, sweep_ms, updated, store.ticks))
                time.sleep(PRICE_FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Interrupted — saving partial results...")
        eq_sweeps = {s: sw for s, sw in sweeps.items() if not sw.is_commodity}
        if not eod_done and eq_sweeps:
            save_results(eq_sweeps, date_str, out_dir, scanner_name=SCANNER_NAME)
        comm_sweeps = {s: sw for s, sw in sweeps.items() if sw.is_commodity}
        if not commodity_eod and comm_sweeps:
            save_results(comm_sweeps, date_str, os.path.join(out_dir, "commodities"),
                         scanner_name=f"{SCANNER_NAME} (Commodity)")
    finally:
        collector.stop()
        log.info("Scanner-1 stopped.")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
