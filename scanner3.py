# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v8.0 | Author: Ridhaant Ajoy Thackur
# scanner3.py — WideDual Sweep + Cross-Scanner Fusion (31,000 variations)
# ═══════════════════════════════════════════════════════════════════════
"""
scanner3.py — Scanner-3 WideDual (15K Lower + 16K Upper)
=========================================================
LOWER band: X ∈ [0.001000, 0.016000]   15,000 variations
UPPER band: X ∈ [0.016000, 0.032000]   16,000 variations
Total S3: 31,000 variations

CROSS-SCANNER FUSION: reads Scanner1 + Scanner2 live_state.json every 60s
and computes the global best X across all 5 sources:
  S1 (1K) + S2-lower (6K) + S2-upper (7K) + S3-lower (15K) + S3-upper (16K)
                                                    = 49,000 total variations

live_state.json structure:
{
  "scanner": "Scanner-3 WideDual",
  "bands": {"lower": {...}, "upper": {...}},
  "merged_best": {...},
  "cross_scanner_best": {
    "NIFTY": {"best_x": 0.00857, "pnl": 2400.0, "source": "scanner1"}
  },
  "sources_used": ["scanner1","scanner2_lower","scanner2_upper","scanner3_lower","scanner3_upper"]
}
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
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pytz
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

from sweep_core import (
    StockSweep, PriceStore, PriceSubscriber,
    read_prev_closes_from_algofinal, fetch_prev_close, fetch_930_price,
    save_results,
    now_ist, in_session, in_premarket, after_930, in_trading_for,
    IST, BROKERAGE_PER_SIDE, STOCKS, COMMODITY_SYMBOLS,
    CPU_WORKERS, CUDA_AVAILABLE, GPU_NAME,
)
from market_calendar import MarketCalendar

# ════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════

SCANNER_NAME  = "Scanner-3 WideDual (15K+16K)"
SCANNER_ID    = 3

# ── LOWER band: wide overlap with S1/S2 (confirms or contradicts) ─────────────
X_LOWER_MIN   = 0.001000
X_LOWER_MAX   = 0.016000
N_LOWER       = 16_000
X_LOWER_NP    = np.linspace(X_LOWER_MIN, X_LOWER_MAX, N_LOWER)

# ── UPPER band: beyond both S2 bands (finds aggressive X values) ─────────────
X_UPPER_MIN   = 0.016000
X_UPPER_MAX   = 0.032000
N_UPPER       = 16_000
X_UPPER_NP    = np.linspace(X_UPPER_MIN, X_UPPER_MAX, N_UPPER)

TOTAL_VARIATIONS = N_LOWER + N_UPPER   # 32,000

# ── Index symbol X ranges ─────────────────────────────────────────────────────
INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY"}
IX_LOWER_NP   = np.linspace(0.001, 0.012, 12_000)
IX_UPPER_NP   = np.linspace(0.012, 0.025, 13_000)

# ── Commodity ranges ─────────────────────────────────────────────────────────
COMMODITY_LOWER: Dict[str, Tuple[float, float, int]] = {
    "GOLD":       (0.001, 0.016, 7_000),
    "SILVER":     (0.001, 0.016, 7_000),
    "NATURALGAS": (0.002, 0.016, 6_500),
    "CRUDE":      (0.001, 0.016, 7_000),
    "COPPER":     (0.001, 0.016, 7_000),
}
COMMODITY_UPPER: Dict[str, Tuple[float, float, int]] = {
    "GOLD":       (0.016, 0.030, 7_000),
    "SILVER":     (0.016, 0.032, 8_000),
    "NATURALGAS": (0.016, 0.035, 8_500),
    "CRUDE":      (0.016, 0.032, 8_000),
    "COPPER":     (0.016, 0.030, 7_000),
}

# ── Cross-scanner fusion ──────────────────────────────────────────────────────
S1_STATE_DIR = os.path.join("sweep_results", "scanner1_narrow_x0080_x0090")
S2_STATE_DIR = os.path.join("sweep_results", "scanner2_dual_x0010_x0160")
CROSS_FUSION_INTERVAL = 60   # seconds between cross-scanner reads

RESULTS_DIR          = os.path.join("sweep_results", "scanner3_widedual_x0010_x0320")
PRICE_FETCH_INTERVAL = 1.0
STATE_DUMP_INTERVAL  = 30

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7587307352:AAG6RaiF4gO5I_ZFZ_4b8Gj7dnsu4GtPWFw")
TELEGRAM_CHATS = [c for c in [
    os.getenv("TELEGRAM_CHAT_ID", "1376513391"),
    "793674804",
] if c]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S3-WideDual] %(levelname)s — %(message)s",
)
log = logging.getLogger("scanner3")


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
# CROSS-SCANNER FUSION
# ════════════════════════════════════════════════════════════════════════════

def _load_scanner_state(path: str) -> Optional[dict]:
    """Load a live_state.json from another scanner. Returns None on error."""
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _extract_best_per_sym(state: Optional[dict], source_label: str) -> Dict[str, dict]:
    """
    Extract per-symbol best X from a scanner's live_state.json.
    Handles both single-band (S1) and dual-band (S2) formats.
    Returns {symbol: {"best_x": float, "pnl": float, "source": str}}
    """
    result: Dict[str, dict] = {}
    if not state:
        return result

    # S2/S3 dual-band: has "merged_best"
    if "merged_best" in state:
        for sym, m in state["merged_best"].items():
            result[sym.upper()] = {
                "best_x": float(m.get("best_x", 0)),
                "pnl":    float(m.get("pnl", -1e9)),
                "source": source_label,
                "band":   m.get("band"),
                "win_rate":    float(m.get("win_rate", 0)),
                "trade_count": int(m.get("trade_count", 0)),
            }
    # S1 single-band: has "sweeps"
    elif "sweeps" in state:
        for sym, sw in state["sweeps"].items():
            result[sym.upper()] = {
                "best_x": float(sw.get("best_x", 0)),
                "pnl":    float(sw.get("best_pnl", -1e9)),
                "source": source_label,
                "band":   None,
                "win_rate":    float(sw.get("best_win_rate", 0)),
                "trade_count": int(sw.get("best_trade_count", 0)),
            }

    return result


def _compute_cross_scanner_best(
    sweeps_lower: Dict[str, StockSweep],
    sweeps_upper: Dict[str, StockSweep],
    s1_state: Optional[dict],
    s2_state: Optional[dict],
) -> Tuple[Dict[str, dict], List[str]]:
    """
    Compute global best X per symbol across all 5 sources:
    S1 | S2-lower | S2-upper | S3-lower | S3-upper

    Returns (cross_scanner_best dict, sources_used list)
    """
    sources_used: List[str] = []

    # Collect per-source data
    s1_best  = _extract_best_per_sym(s1_state, "scanner1")
    s2_best  = _extract_best_per_sym(s2_state, "scanner2")  # merged_best from S2

    if s1_state:  sources_used.append("scanner1")
    if s2_state:
        sources_used.append("scanner2_lower")
        sources_used.append("scanner2_upper")

    # S3 own bands
    s3_lower_best: Dict[str, dict] = {}
    s3_upper_best: Dict[str, dict] = {}
    for sym, sw in sweeps_lower.items():
        d = sw.dump_state()
        s3_lower_best[sym] = {
            "best_x": float(d.get("best_x", 0)),
            "pnl":    float(d.get("best_pnl", -1e9)),
            "source": "scanner3_lower",
            "band":   "lower",
            "win_rate":    float(d.get("best_win_rate", 0)),
            "trade_count": int(d.get("best_trade_count", 0)),
        }
    for sym, sw in sweeps_upper.items():
        d = sw.dump_state()
        s3_upper_best[sym] = {
            "best_x": float(d.get("best_x", 0)),
            "pnl":    float(d.get("best_pnl", -1e9)),
            "source": "scanner3_upper",
            "band":   "upper",
            "win_rate":    float(d.get("best_win_rate", 0)),
            "trade_count": int(d.get("best_trade_count", 0)),
        }
    sources_used += ["scanner3_lower", "scanner3_upper"]

    # For each symbol, pick the source with the highest P&L
    all_syms = set(s1_best) | set(s2_best) | set(s3_lower_best) | set(s3_upper_best)
    cross_best: Dict[str, dict] = {}

    for sym in all_syms:
        candidates = []
        for d in (s1_best.get(sym), s2_best.get(sym),
                  s3_lower_best.get(sym), s3_upper_best.get(sym)):
            if d and d.get("best_x", 0) > 0:
                candidates.append(d)

        if not candidates:
            continue
        winner = max(candidates, key=lambda d: d["pnl"])
        cross_best[sym] = winner

    return cross_best, sources_used


# ════════════════════════════════════════════════════════════════════════════
# MERGED BEST (S3 bands only)
# ════════════════════════════════════════════════════════════════════════════

def _compute_merged_best(
    sweeps_lower: Dict[str, StockSweep],
    sweeps_upper: Dict[str, StockSweep],
) -> Dict[str, dict]:
    merged: Dict[str, dict] = {}
    for sym in set(sweeps_lower) | set(sweeps_upper):
        sl = sweeps_lower.get(sym)
        su = sweeps_upper.get(sym)
        dl = sl.dump_state() if sl else {}
        du = su.dump_state() if su else {}
        pl = float(dl.get("best_pnl", -1e9))
        pu = float(du.get("best_pnl", -1e9))
        if pl >= pu:
            merged[sym] = {"best_x": float(dl.get("best_x", 0)), "pnl": round(pl, 2),
                           "band": "lower", "win_rate": float(dl.get("best_win_rate", 0)),
                           "trade_count": int(dl.get("best_trade_count", 0))}
        else:
            merged[sym] = {"best_x": float(du.get("best_x", 0)), "pnl": round(pu, 2),
                           "band": "upper", "win_rate": float(du.get("best_win_rate", 0)),
                           "trade_count": int(du.get("best_trade_count", 0))}
    return merged


# ════════════════════════════════════════════════════════════════════════════
# STATE DUMP
# ════════════════════════════════════════════════════════════════════════════

def _dump_live_state(
    sweeps_lower: Dict[str, StockSweep],
    sweeps_upper: Dict[str, StockSweep],
    cross_best: Dict[str, dict],
    sources_used: List[str],
    date_str: str,
    out_dir: str,
) -> None:
    merged = _compute_merged_best(sweeps_lower, sweeps_upper)
    state: Dict[str, Any] = {
        "scanner":    SCANNER_NAME,
        "scanner_id": SCANNER_ID,
        "date":       date_str,
        "written_at": now_ist().isoformat(),
        "author":     "Ridhaant Ajoy Thackur",
        "total_variations": TOTAL_VARIATIONS,
        "total_all_scanners": 1_000 + 16_000 + TOTAL_VARIATIONS,  # 49,000
        "bands": {
            "lower": {
                "x_min": X_LOWER_MIN, "x_max": X_LOWER_MAX, "n": N_LOWER,
                "sweeps": {sym: sw.dump_state() for sym, sw in sweeps_lower.items()},
            },
            "upper": {
                "x_min": X_UPPER_MIN, "x_max": X_UPPER_MAX, "n": N_UPPER,
                "sweeps": {sym: sw.dump_state() for sym, sw in sweeps_upper.items()},
            },
        },
        "merged_best":        merged,
        "cross_scanner_best": cross_best,
        "sources_used":       sources_used,
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
    cross_best: Dict[str, dict],
    now: datetime,
    sweep_ms: float,
    ticks: int,
) -> Table:
    table = Table(
        title=(
            f"[bold cyan]{SCANNER_NAME}  |  "
            f"Lower: {N_LOWER:,} [{X_LOWER_MIN:.3f}–{X_LOWER_MAX:.3f}]  "
            f"Upper: {N_UPPER:,} [{X_UPPER_MIN:.3f}–{X_UPPER_MAX:.3f}]  "
            f"Cross-fusion: 45K total  "
            f"— {now.strftime('%H:%M:%S IST')}  sweep={sweep_ms:.1f}ms  ticks={ticks}[/]"
        ),
        show_header=True, header_style="bold yellow",
        border_style="dim", expand=False,
    )
    table.add_column("Symbol",        style="bold white", width=14)
    table.add_column("S3 Best X",     style="green",      width=10, justify="right")
    table.add_column("Band",          style="magenta",    width=7)
    table.add_column("S3 P&L",        width=12,           justify="right")
    table.add_column("Global Best X", style="cyan",       width=12, justify="right")
    table.add_column("Source",        style="yellow",     width=14)
    table.add_column("Global P&L",    width=12,           justify="right")

    merged = _compute_merged_best(sweeps_lower, sweeps_upper)
    for sym in sorted(merged):
        m  = merged[sym]
        cb = cross_best.get(sym, {})
        s3_pnl = m.get("pnl", 0.0)
        gl_pnl = cb.get("pnl", 0.0)
        s3_pnl_t = Text(f"₹{s3_pnl:+,.0f}", style="green" if s3_pnl >= 0 else "red")
        gl_pnl_t = Text(f"₹{gl_pnl:+,.0f}", style="green" if gl_pnl >= 0 else "red")
        src = cb.get("source", "—")
        src_style = "cyan" if "scanner1" in src else ("magenta" if "scanner2" in src else "yellow")
        table.add_row(
            sym,
            Text(f"{m.get('best_x', 0):.6f}", style="green") if m.get("best_x") else Text("—", style="dim"),
            Text(m.get("band", "—"), style="magenta" if m.get("band") == "lower" else "cyan"),
            s3_pnl_t,
            Text(f"{cb.get('best_x', 0):.6f}", style="cyan") if cb.get("best_x") else Text("—", style="dim"),
            Text(src, style=src_style),
            gl_pnl_t,
        )
    return table


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    # Pin to designated CPU core for optimal performance
    try:
        from process_affinity import pin_process
        pin_process("Scanner3")
    except Exception:
        pass
    log.info("═" * 72)
    log.info("%s", SCANNER_NAME)
    log.info("  LOWER: %d vars  X=[%.4f–%.4f]", N_LOWER, X_LOWER_MIN, X_LOWER_MAX)
    log.info("  UPPER: %d vars  X=[%.4f–%.4f]", N_UPPER, X_UPPER_MIN, X_UPPER_MAX)
    log.info("  Cross-fusion reads S1 + S2 every %ds → 49,000 total variations", CROSS_FUSION_INTERVAL)
    log.info("GPU: %s", GPU_NAME if CUDA_AVAILABLE else "CPU (NumPy)")
    log.info("Author: Ridhaant Ajoy Thackur")
    log.info("═" * 72)

    if not MarketCalendar.is_trading_day(now_ist()):
        log.info("Not a trading day — Scanner 3 entering dashboard-only mode.")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    date_str = now_ist().strftime("%Y%m%d")
    out_dir  = os.path.join(RESULTS_DIR, date_str)
    os.makedirs(out_dir, exist_ok=True)

    # ── Load previous closes ──────────────────────────────────────────────────
    log.info("Loading previous closes...")
    algofinal_pcs = read_prev_closes_from_algofinal(date_str)

    def _get_pc(sym):
        pc = algofinal_pcs.get(sym.upper())
        if pc and pc > 0:
            return sym, pc, "levels"
        return sym, fetch_prev_close(sym), "yfinance"

    all_syms = STOCKS + (COMMODITY_SYMBOLS if os.getenv("ENABLE_COMMODITIES") == "1" else [])
    with ThreadPoolExecutor(max_workers=min(20, len(all_syms))) as ex:
        results = list(ex.map(_get_pc, all_syms))

    # ── Build sweeps for both bands ───────────────────────────────────────────
    sweeps_lower: Dict[str, StockSweep] = {}
    sweeps_upper: Dict[str, StockSweep] = {}

    for sym, pc, src in results:
        if pc is None:
            log.warning("  SKIP %-14s", sym)
            continue
        is_comm = sym in COMMODITY_SYMBOLS

        if is_comm:
            lo_cfg = COMMODITY_LOWER.get(sym)
            up_cfg = COMMODITY_UPPER.get(sym)
            xv_l = np.linspace(*lo_cfg) if lo_cfg else X_LOWER_NP
            xv_u = np.linspace(*up_cfg) if up_cfg else X_UPPER_NP
        elif sym in INDEX_SYMBOLS:
            xv_l, xv_u = IX_LOWER_NP, IX_UPPER_NP
        else:
            xv_l, xv_u = X_LOWER_NP, X_UPPER_NP

        sweeps_lower[sym] = StockSweep(sym, pc, xv_l, is_commodity=is_comm)
        sweeps_upper[sym] = StockSweep(sym, pc, xv_u, is_commodity=is_comm)
        log.info("  OK  %-14s  pc=%.2f  L=[%.4f–%.4f]  U=[%.4f–%.4f]  [%s]",
                 sym, pc, float(xv_l[0]), float(xv_l[-1]),
                 float(xv_u[0]), float(xv_u[-1]), src)

    if not sweeps_lower:
        log.error("No symbols loaded. Start Algofinal.py first.")
        sys.exit(1)

    # ── ZMQ price feed ────────────────────────────────────────────────────────
    csv_path  = os.path.join(out_dir, "prices.csv")
    store     = PriceStore()
    collector = PriceSubscriber(store, csv_path)
    collector.start()

    # ── Shared cross-scanner state ────────────────────────────────────────────
    cross_best:   Dict[str, dict] = {}
    sources_used: List[str]       = ["scanner3_lower", "scanner3_upper"]
    last_fusion   = 0.0
    last_dump     = 0.0

    # ── Shared control flags ──────────────────────────────────────────────────
    eod_done        = False
    commodity_eod   = False
    anchor_done     = False
    sweep_ms        = 0.0

    console = Console()

    try:
        with Live(
            build_table(sweeps_lower, sweeps_upper, cross_best, now_ist(), 0.0, 0),
            console=console, refresh_per_second=2,
        ) as live:
            while True:
                now = now_ist()

                # ── Cross-scanner fusion ──────────────────────────────────────
                if time.monotonic() - last_fusion > CROSS_FUSION_INTERVAL:
                    try:
                        s1_path = os.path.join(S1_STATE_DIR, date_str, "live_state.json")
                        s2_path = os.path.join(S2_STATE_DIR, date_str, "live_state.json")
                        s1_st = _load_scanner_state(s1_path)
                        s2_st = _load_scanner_state(s2_path)
                        cross_best, sources_used = _compute_cross_scanner_best(
                            sweeps_lower, sweeps_upper, s1_st, s2_st
                        )
                        log.debug("Cross-scanner fusion: %d symbols, sources=%s",
                                  len(cross_best), sources_used)
                        last_fusion = time.monotonic()
                    except Exception as exc:
                        log.debug("Fusion error: %s", exc)

                # ── State dump ────────────────────────────────────────────────
                if time.monotonic() - last_dump > STATE_DUMP_INTERVAL:
                    try:
                        _dump_live_state(sweeps_lower, sweeps_upper,
                                         cross_best, sources_used, date_str, out_dir)
                        last_dump = time.monotonic()
                    except Exception as exc:
                        log.debug("State dump error: %s", exc)

                # ── Equity EOD ────────────────────────────────────────────────
                if now.hour == 15 and now.minute >= 11 and not eod_done:
                    log.info("Equity EOD: squaring off both bands...")
                    prices = store.snapshot()
                    for sym in sweeps_lower:
                        if not sweeps_lower[sym].is_commodity:
                            px = prices.get(sym, sweeps_lower[sym].last_price)
                            if px:
                                sweeps_lower[sym].eod_square_off(px, now)
                                sweeps_upper[sym].eod_square_off(px, now)
                    eq_l = {s: sw for s, sw in sweeps_lower.items() if not sw.is_commodity}
                    eq_u = {s: sw for s, sw in sweeps_upper.items() if not sw.is_commodity}
                    save_results(eq_l, date_str, os.path.join(out_dir, "band_lower"),
                                 scanner_name=f"{SCANNER_NAME} — Lower")
                    save_results(eq_u, date_str, os.path.join(out_dir, "band_upper"),
                                 scanner_name=f"{SCANNER_NAME} — Upper")
                    eod_done = True
                    _tg_async(f"✅ {SCANNER_NAME} Equity EOD complete.")

                # ── Commodity EOD ─────────────────────────────────────────────
                if now.hour == 23 and now.minute == 0 and not commodity_eod:
                    prices = store.snapshot()
                    for sym in sweeps_lower:
                        if sweeps_lower[sym].is_commodity:
                            px = prices.get(sym, sweeps_lower[sym].last_price)
                            if px:
                                sweeps_lower[sym].eod_square_off(px, now)
                                sweeps_upper[sym].eod_square_off(px, now)
                    commodity_eod = True

                # ── Off-hours ─────────────────────────────────────────────────
                if not in_session(now):
                    live.update(build_table(sweeps_lower, sweeps_upper,
                                            cross_best, now, sweep_ms, store.ticks))
                    time.sleep(30)
                    continue

                # ── Premarket ─────────────────────────────────────────────────
                if in_premarket(now) and not anchor_done:
                    prices = store.snapshot()
                    for sym in sweeps_lower:
                        px = prices.get(sym)
                        if px:
                            sweeps_lower[sym].last_price = px
                            sweeps_upper[sym].last_price = px
                            sweeps_lower[sym].premarket_adjust(px)
                            sweeps_upper[sym].premarket_adjust(px)
                    live.update(build_table(sweeps_lower, sweeps_upper,
                                            cross_best, now, sweep_ms, store.ticks))
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

                # ── Main sweep: both bands in parallel ────────────────────────
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

                    tl = threading.Thread(target=_sweep_lower, daemon=True)
                    tu = threading.Thread(target=_sweep_upper, daemon=True)
                    tl.start(); tu.start()
                    tl.join(); tu.join()

                    sweep_ms = (time.monotonic() - t0) * 1000

                live.update(build_table(sweeps_lower, sweeps_upper,
                                        cross_best, now, sweep_ms, store.ticks))
                time.sleep(PRICE_FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Scanner 3 stopped by user.")
        collector.stop()
    except Exception as exc:
        log.error("Fatal error: %s", exc, exc_info=True)
        _tg_async(f"❌ {SCANNER_NAME} CRASHED: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
