# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v8.0 | Author: Ridhaant Ajoy Thackur
# scanner2.py — Dual-Band Medium Sweep (Low: 0.001–0.007 + High: 0.009–0.016)
# ═══════════════════════════════════════════════════════════════════════
"""
scanner2.py — Dual-Band Sweep  (13,000 total variations)
=========================================================
LOWER band: X ∈ [0.001000, 0.007000]  6,000 variations  (tight/conservative)
UPPER band: X ∈ [0.009000, 0.016000]  7,000 variations  (moderate/active)

Both bands share one ZMQ PriceSubscriber. After each sweep cycle, the
best X per-symbol is the winner across both bands.

live_state.json structure:
{
  "scanner": "Scanner-2 Dual",
  "bands": {
    "lower": {"x_min": 0.001, "x_max": 0.007, "n": 6000, "sweeps": {...}},
    "upper": {"x_min": 0.009, "x_max": 0.016, "n": 7000, "sweeps": {...}}
  },
  "merged_best": {"NIFTY": {"best_x": 0.0042, "pnl": 1250.0, "band": "lower"}, ...}
}

Startup order: Algofinal → Scanner1 → Scanner2 → Scanner3 → XOptimizer → BestXTrader
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

from sweep_core import (
    StockSweep, PriceStore, PriceSubscriber,
    read_prev_closes_from_algofinal, fetch_prev_close, fetch_930_price,
    save_results, spill_trade_logs_to_disk,
    now_ist, in_session, in_premarket, after_930, in_trading_for,
    IST, BROKERAGE_PER_SIDE, STOCKS, COMMODITY_SYMBOLS,
    CPU_WORKERS, CUDA_AVAILABLE, GPU_NAME,
)
from market_calendar import MarketCalendar

# ════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════

SCANNER_NAME  = "Scanner-2 Dual (Low+High)"
SCANNER_ID    = 2

# ── LOWER band: tight/conservative X values (below live ~0.008575) ───────────
X_LOWER_MIN   = 0.001000
X_LOWER_MAX   = 0.007000
N_LOWER       = 8_000
X_LOWER_NP    = np.linspace(X_LOWER_MIN, X_LOWER_MAX, N_LOWER)

# ── UPPER band: moderate/active X values (above live ~0.008575) ──────────────
X_UPPER_MIN   = 0.009000
X_UPPER_MAX   = 0.016000
N_UPPER       = 8_000
X_UPPER_NP    = np.linspace(X_UPPER_MIN, X_UPPER_MAX, N_UPPER)

TOTAL_VARIATIONS = N_LOWER + N_UPPER  # 16,000

# ── Index symbols: scaled ranges ─────────────────────────────────────────────
INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY"}
IX_LOWER_NP   = np.linspace(0.0008, 0.005, 5_000)
IX_UPPER_NP   = np.linspace(0.006,  0.013, 5_500)

# ── Commodity ranges per band ─────────────────────────────────────────────────
COMMODITY_LOWER: Dict[str, Tuple[float, float, int]] = {
    "GOLD":       (0.0010, 0.0050, 3_000),
    "SILVER":     (0.0010, 0.0060, 4_000),
    "NATURALGAS": (0.0020, 0.0060, 3_000),
    "CRUDE":      (0.0015, 0.0055, 3_000),
    "COPPER":     (0.0010, 0.0050, 3_000),
}
COMMODITY_UPPER: Dict[str, Tuple[float, float, int]] = {
    "GOLD":       (0.0080, 0.0120, 3_000),
    "SILVER":     (0.0090, 0.0130, 3_500),
    "NATURALGAS": (0.0090, 0.0160, 3_500),
    "CRUDE":      (0.0080, 0.0140, 3_500),
    "COPPER":     (0.0080, 0.0120, 3_000),
}

RESULTS_DIR          = os.path.join("sweep_results", "scanner2_dual_x0010_x0160")
PRICE_FETCH_INTERVAL = 1.0
STATE_DUMP_INTERVAL  = 30

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7587307352:AAG6RaiF4gO5I_ZFZ_4b8Gj7dnsu4GtPWFw")
TELEGRAM_CHATS = [c for c in [
    os.getenv("TELEGRAM_CHAT_ID", "1376513391"),
    "793674804",
] if c]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S2-Dual] %(levelname)s — %(message)s",
)
log = logging.getLogger("scanner2")


# ════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ════════════════════════════════════════════════════════════════════════════

def _tg_async(text: str) -> None:
    """Send alert via unified equity Telegram bot."""
    try:
        from tg_async import send_alert
        send_alert(text, asset_class="equity")
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════
# MERGED BEST — compute per-symbol winner across both bands
# ════════════════════════════════════════════════════════════════════════════

def _compute_merged_best(
    sweeps_lower: Dict[str, StockSweep],
    sweeps_upper: Dict[str, StockSweep],
) -> Dict[str, dict]:
    """
    For each symbol, pick the band that produced higher net P&L.
    Returns dict: {symbol: {"best_x": float, "pnl": float, "band": "lower"|"upper"}}
    """
    merged: Dict[str, dict] = {}
    all_syms = set(sweeps_lower) | set(sweeps_upper)
    for sym in all_syms:
        sw_l = sweeps_lower.get(sym)
        sw_u = sweeps_upper.get(sym)
        best_l = sw_l.dump_state() if sw_l else None
        best_u = sw_u.dump_state() if sw_u else None

        pnl_l = float(best_l.get("best_pnl", -1e9)) if best_l else -1e9
        pnl_u = float(best_u.get("best_pnl", -1e9)) if best_u else -1e9

        if pnl_l >= pnl_u and best_l:
            merged[sym] = {
                "best_x":    float(best_l.get("best_x", 0)),
                "pnl":       round(pnl_l, 2),
                "band":      "lower",
                "win_rate":  float(best_l.get("best_win_rate", 0)),
                "trade_count": int(best_l.get("best_trade_count", 0)),
            }
        elif best_u:
            merged[sym] = {
                "best_x":    float(best_u.get("best_x", 0)),
                "pnl":       round(pnl_u, 2),
                "band":      "upper",
                "win_rate":  float(best_u.get("best_win_rate", 0)),
                "trade_count": int(best_u.get("best_trade_count", 0)),
            }
    return merged


# ════════════════════════════════════════════════════════════════════════════
# STATE DUMP
# ════════════════════════════════════════════════════════════════════════════

def _dump_live_state(
    sweeps_lower: Dict[str, StockSweep],
    sweeps_upper: Dict[str, StockSweep],
    date_str: str,
    out_dir: str,
) -> None:
    merged = _compute_merged_best(sweeps_lower, sweeps_upper)
    state = {
        "scanner":    SCANNER_NAME,
        "scanner_id": SCANNER_ID,
        "date":       date_str,
        "written_at": now_ist().isoformat(),
        "author":     "Ridhaant Ajoy Thackur",
        "total_variations": TOTAL_VARIATIONS,
        "bands": {
            "lower": {
                "x_min": X_LOWER_MIN,
                "x_max": X_LOWER_MAX,
                "n":     N_LOWER,
                "sweeps": {sym: sw.dump_state() for sym, sw in sweeps_lower.items()},
            },
            "upper": {
                "x_min": X_UPPER_MIN,
                "x_max": X_UPPER_MAX,
                "n":     N_UPPER,
                "sweeps": {sym: sw.dump_state() for sym, sw in sweeps_upper.items()},
            },
        },
        "merged_best": merged,
    }
    path = os.path.join(out_dir, "live_state.json")
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, separators=(",", ":"))
    os.replace(tmp, path)


# ════════════════════════════════════════════════════════════════════════════
# RICH TABLE
# ════════════════════════════════════════════════════════════════════════════

def build_table(
    sweeps_lower: Dict[str, StockSweep],
    sweeps_upper: Dict[str, StockSweep],
    now: datetime,
    sweep_ms: float,
    ticks: int,
) -> Table:
    merged = _compute_merged_best(sweeps_lower, sweeps_upper)
    table = Table(
        title=(
            f"[bold cyan]{SCANNER_NAME}  |  "
            f"Lower: {N_LOWER:,} vars [{X_LOWER_MIN:.3f}–{X_LOWER_MAX:.3f}]  "
            f"Upper: {N_UPPER:,} vars [{X_UPPER_MIN:.3f}–{X_UPPER_MAX:.3f}]  "
            f"— {now.strftime('%H:%M:%S IST')}  sweep={sweep_ms:.1f}ms  ticks={ticks}[/]"
        ),
        show_header=True, header_style="bold yellow",
        border_style="dim", expand=False,
    )
    table.add_column("Symbol",      style="bold white", width=14)
    table.add_column("Prev Close",  style="dim",        width=11, justify="right")
    table.add_column("Last Price",  style="cyan",       width=11, justify="right")
    table.add_column("Best X",      style="green",      width=10, justify="right")
    table.add_column("Band",        style="magenta",    width=7)
    table.add_column("P&L",         width=12,           justify="right")
    table.add_column("Win%",        width=7,            justify="right")
    table.add_column("Trades",      width=7,            justify="right")

    all_syms = sorted(set(sweeps_lower) | set(sweeps_upper))
    for sym in all_syms:
        m = merged.get(sym, {})
        sw = sweeps_lower.get(sym) or sweeps_upper.get(sym)
        if not sw:
            continue
        r = sw.row_data()
        pnl = m.get("pnl", 0.0)
        pnl_s = Text(f"₹{pnl:+,.0f}", style="green" if pnl >= 0 else "red")
        band = m.get("band", "—")
        bx   = m.get("best_x", 0.0)
        wr   = m.get("win_rate", 0.0)
        tc   = m.get("trade_count", 0)
        table.add_row(
            sym,
            r["prev_close"],
            r["last_price"],
            Text(f"{bx:.6f}" if bx else "—", style="green" if bx else "dim"),
            Text(band, style="cyan" if band == "upper" else "magenta"),
            pnl_s,
            Text(f"{wr:.1f}%" if wr else "—", style="cyan"),
            Text(str(tc) if tc else "—", style="white"),
        )
    return table


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    # Pin to designated CPU core for optimal performance
    try:
        from process_affinity import pin_process
        pin_process("Scanner2")
    except Exception:
        pass
    log.info("═" * 70)
    log.info("%s — Total: %d variations", SCANNER_NAME, TOTAL_VARIATIONS)
    log.info("  LOWER band: %d vars  X=[%.4f–%.4f]  (tight/conservative)", N_LOWER, X_LOWER_MIN, X_LOWER_MAX)
    log.info("  UPPER band: %d vars  X=[%.4f–%.4f]  (moderate/active)",   N_UPPER, X_UPPER_MIN, X_UPPER_MAX)
    log.info("GPU: %s", GPU_NAME if CUDA_AVAILABLE else "CPU (NumPy)")
    log.info("Author: Ridhaant Ajoy Thackur")
    log.info("═" * 70)

    # Check trading day
    if not MarketCalendar.is_trading_day(now_ist()):
        log.info("Not a trading day — Scanner 2 entering dashboard-only mode.")
        nd = MarketCalendar.next_trading_day(now_ist())
        log.info("Next trading day: %s", nd.strftime("%A %d %b %Y"))

    os.makedirs(RESULTS_DIR, exist_ok=True)
    date_str = now_ist().strftime("%Y%m%d")
    out_dir  = os.path.join(RESULTS_DIR, date_str)
    os.makedirs(out_dir, exist_ok=True)

    # ── Load previous closes ──────────────────────────────────────────────────
    log.info("Loading previous closes from Algofinal levels file...")
    algofinal_pcs = read_prev_closes_from_algofinal(date_str)

    def _get_pc(sym):
        pc = algofinal_pcs.get(sym.upper())
        if pc and pc > 0:
            return sym, pc, "levels"
        return sym, fetch_prev_close(sym), "yfinance"

    all_syms = STOCKS + (COMMODITY_SYMBOLS if os.getenv("ENABLE_COMMODITIES") == "1" else [])
    with ThreadPoolExecutor(max_workers=min(20, len(all_syms))) as ex:
        results = list(ex.map(_get_pc, all_syms))

    # ── Initialise LOWER band sweeps ──────────────────────────────────────────
    sweeps_lower: Dict[str, StockSweep] = {}
    sweeps_upper: Dict[str, StockSweep] = {}

    for sym, pc, src in results:
        if pc is None:
            log.warning("  SKIP %-14s — prev_close unavailable", sym)
            continue
        is_comm = sym in COMMODITY_SYMBOLS

        # Determine X arrays for each band
        if is_comm:
            lo_cfg = COMMODITY_LOWER.get(sym)
            up_cfg = COMMODITY_UPPER.get(sym)
            xv_lower = np.linspace(*lo_cfg) if lo_cfg else X_LOWER_NP
            xv_upper = np.linspace(*up_cfg) if up_cfg else X_UPPER_NP
        elif sym in INDEX_SYMBOLS:
            xv_lower = IX_LOWER_NP
            xv_upper = IX_UPPER_NP
        else:
            xv_lower = X_LOWER_NP
            xv_upper = X_UPPER_NP

        sweeps_lower[sym] = StockSweep(sym, pc, xv_lower, is_commodity=is_comm)
        sweeps_upper[sym] = StockSweep(sym, pc, xv_upper, is_commodity=is_comm)
        log.info("  OK   %-14s  pc=%.2f  lower=[%.4f–%.4f]  upper=[%.4f–%.4f]  [%s]",
                 sym, pc,
                 float(xv_lower[0]), float(xv_lower[-1]),
                 float(xv_upper[0]), float(xv_upper[-1]),
                 src)

    if not sweeps_lower:
        log.error("No symbols loaded. Start Algofinal.py first, then retry.")
        sys.exit(1)

    log.info("Loaded %d symbols across 2 bands (%d + %d = %d total sweeps)",
             len(sweeps_lower), N_LOWER, N_UPPER, TOTAL_VARIATIONS)

    # ── ZMQ price subscriber (shared across both bands) ───────────────────────
    csv_path  = os.path.join(out_dir, "prices.csv")
    store     = PriceStore()
    collector = PriceSubscriber(store, csv_path)
    collector.start()
    log.info("Shared PriceSubscriber started (both bands use same feed).")

    # ── Main loop ─────────────────────────────────────────────────────────────
    console         = Console()
    sweep_ms        = 0.0
    eod_done        = False
    commodity_eod   = False
    anchor_done     = False
    last_dump       = 0.0

    try:
        with Live(
            build_table(sweeps_lower, sweeps_upper, now_ist(), 0.0, 0),
            console=console, refresh_per_second=2,
        ) as live:
            while True:
                now = now_ist()

                # ── Periodic state dump ───────────────────────────────────────
                if time.monotonic() - last_dump > STATE_DUMP_INTERVAL:
                    try:
                        _dump_live_state(sweeps_lower, sweeps_upper, date_str, out_dir)
                        last_dump = time.monotonic()
                    except Exception as exc:
                        log.debug("State dump error: %s", exc)

                # ── Equity EOD at 15:11 ───────────────────────────────────────
                if now.hour == 15 and now.minute >= 11 and not eod_done:
                    log.info("Equity EOD: squaring off both bands at 15:11...")
                    prices = store.snapshot()
                    for sym in list(sweeps_lower):
                        if not sweeps_lower[sym].is_commodity:
                            px = prices.get(sym, sweeps_lower[sym].last_price)
                            if px:
                                sweeps_lower[sym].eod_square_off(px, now)
                                sweeps_upper[sym].eod_square_off(px, now)
                    eq_lower = {s: sw for s, sw in sweeps_lower.items() if not sw.is_commodity}
                    eq_upper = {s: sw for s, sw in sweeps_upper.items() if not sw.is_commodity}
                    save_results(eq_lower, date_str, os.path.join(out_dir, "band_lower"),
                                 scanner_name=f"{SCANNER_NAME} — Lower Band")
                    save_results(eq_upper, date_str, os.path.join(out_dir, "band_upper"),
                                 scanner_name=f"{SCANNER_NAME} — Upper Band")
                    eod_done = True
                    _tg_async(f"✅ {SCANNER_NAME} Equity EOD complete. Both bands saved.")

                # ── Commodity EOD at 23:00 ────────────────────────────────────
                if now.hour == 23 and now.minute == 0 and not commodity_eod:
                    prices = store.snapshot()
                    for sym in list(sweeps_lower):
                        if sweeps_lower[sym].is_commodity:
                            px = prices.get(sym, sweeps_lower[sym].last_price)
                            if px:
                                sweeps_lower[sym].eod_square_off(px, now)
                                sweeps_upper[sym].eod_square_off(px, now)
                    commodity_eod = True

                # ── Off-hours: idle ───────────────────────────────────────────
                if not in_session(now):
                    live.update(build_table(sweeps_lower, sweeps_upper, now, sweep_ms, store.ticks))
                    time.sleep(30)
                    continue

                # ── Premarket adjustment ──────────────────────────────────────
                if in_premarket(now) and not anchor_done:
                    prices = store.snapshot()
                    if prices:
                        for sym in sweeps_lower:
                            px = prices.get(sym)
                            if px:
                                sweeps_lower[sym].last_price = px
                                sweeps_upper[sym].last_price = px
                                sweeps_lower[sym].premarket_adjust(px)
                                sweeps_upper[sym].premarket_adjust(px)
                    live.update(build_table(sweeps_lower, sweeps_upper, now, sweep_ms, store.ticks))
                    time.sleep(PRICE_FETCH_INTERVAL)
                    continue

                # ── 09:30 re-anchor ───────────────────────────────────────────
                if after_930(now) and not anchor_done:
                    def _anc(sym):
                        p930 = fetch_930_price(sym, sweeps_lower[sym].prev_close)
                        if p930:
                            sweeps_lower[sym].reanchor_at_930(p930)
                            sweeps_upper[sym].reanchor_at_930(p930)
                    with ThreadPoolExecutor(max_workers=min(20, len(sweeps_lower))) as ex:
                        list(ex.map(_anc, list(sweeps_lower)))
                    anchor_done = True
                    log.info("09:30 re-anchor complete (both bands).")

                # ── Main sweep: update BOTH bands with current prices ──────────
                prices = store.snapshot()
                if prices:
                    t0 = time.monotonic()

                    def _sweep_lower():
                        for sym, sw in sweeps_lower.items():
                            px = prices.get(sym)
                            if px and in_trading_for(sym, now):
                                sw.on_price(px, now)

                    def _sweep_upper():
                        for sym, sw in sweeps_upper.items():
                            px = prices.get(sym)
                            if px and in_trading_for(sym, now):
                                sw.on_price(px, now)

                    # Run both bands in parallel threads
                    tl = threading.Thread(target=_sweep_lower, daemon=True)
                    tu = threading.Thread(target=_sweep_upper, daemon=True)
                    tl.start(); tu.start()
                    tl.join(); tu.join()

                    sweep_ms = (time.monotonic() - t0) * 1000

                live.update(build_table(sweeps_lower, sweeps_upper, now, sweep_ms, store.ticks))
                time.sleep(PRICE_FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Scanner 2 stopped by user.")
        collector.stop()

    except Exception as exc:
        log.error("Fatal error: %s", exc, exc_info=True)
        _tg_async(f"❌ {SCANNER_NAME} CRASHED: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
