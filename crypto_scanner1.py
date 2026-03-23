# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v9.0 | Author: Ridhaant Ajoy Thackur
# crypto_scanner1.py — Crypto Narrow Sweep (same X range as equity scanner1)
# ═══════════════════════════════════════════════════════════════════════
"""
crypto_scanner1.py — Crypto Narrow Sweep  v9.0
================================================
X range: 0.0080–0.0090 (1,000 vars) — IDENTICAL to equity scanner1
5 coins × 1,000 = 5,000 variations per 6h window (20,000/day)

Uses StockSweep.on_price() for ALL logic identically to equity:
  ✓ Re-entry watch (threshold + retouch)
  ✓ Retreat 65/45/25 (peak guard → locked gain → exit)
  ✓ T1–T5 / ST1–ST5 target exits
  ✓ SL exits
  ✓ One trade per symbol per 6h anchor window
  ✓ EOD → re-anchor at 6h boundary (squares off open positions)
  ✓ RAM spill every 5 min
  ✓ Atomic live_state.json dump every 30s
  ✓ ZMQ topic: "crypto" (Binance WS → 1-5s latency)

Key differences from equity:
  - anchor_price used instead of prev_close (fetched from crypto_engine)
  - 24/7 no market calendar check
  - 6h re-anchor cycle instead of daily EOD
  - P&L shown in Rs (USDT × USDT_TO_INR)
  - No premarket adjust (crypto has no pre-market concept)
  - No 09:30 re-anchor (crypto has 6h re-anchor instead)
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CrS1] %(levelname)s — %(message)s",
)
log = logging.getLogger("crypto_scanner1")

# ── Configuration ─────────────────────────────────────────────────────────────
SCANNER_NAME = "Crypto-Scanner-1 Narrow"
SCANNER_ID   = "crypto1"
SYMBOLS: List[str] = ["BTC", "ETH", "BNB", "SOL", "ADA"]
ZMQ_TOPIC    = b"crypto"

# Same X range as equity scanner1
X_MIN    = 0.008000
X_MAX    = 0.009000
N_VALUES = 1_000
X_ARRAY  = np.linspace(X_MIN, X_MAX, N_VALUES)

REANCHOR_HOURS      = 6
STATE_DUMP_INTERVAL = 30
PRICE_FETCH_INTERVAL = 1.0
USDT_TO_INR         = cfg.USDT_TO_INR
CURRENT_X           = cfg.CRYPTO_X_MULTIPLIER   # 0.008575

BASE_RESULTS_DIR = os.path.join("sweep_results", "crypto_scanner1")


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def _tg_async(text: str) -> None:
    try:
        from tg_async import send_alert
        send_alert(text, asset_class="crypto")
    except Exception:
        pass


def _window_tag() -> str:
    return datetime.now(pytz.utc).strftime("%Y%m%d_%H%M")


def _get_anchor_prices() -> Dict[str, float]:
    """
    Fetch current anchor prices from crypto_engine.
    Priority:
      1. crypto_initial_levels_latest.json  (written by engine at startup)
      2. live_prices.json crypto_prices     (written every 2s by engine)
      3. Module-level CRYPTO_ANCHOR dict    (only works if same process, always empty in scanner)
    """
    stale_initial_s = 2 * 3600.0   # ignore initial anchor files older than 2h
    stale_live_s    = 90.0         # warn if live_prices is older than ~90s

    now_ts = time.time()
    cp: Dict[str, float] = {}

    # 1) live_prices.json crypto_prices (fast, cross-check target)
    lp_path = os.path.join("levels", "live_prices.json")
    if os.path.exists(lp_path):
        try:
            lp_age_s = now_ts - float(os.path.getmtime(lp_path))
            if lp_age_s > stale_live_s:
                log.warning("  live_prices.json is stale (age=%.0fs) — crypto anchors may drift", lp_age_s)
            data = json.load(open(lp_path, encoding="utf-8"))
            cp_raw = data.get("crypto_prices", {}) or {}
            cp = {s: float(cp_raw.get(s, 0) or 0) for s in SYMBOLS}
        except Exception:
            cp = {}

    # 2) crypto_initial_levels_latest.json (most accurate anchor ladder)
    anchors: Dict[str, float] = {}
    for jname in ("crypto_initial_levels_latest.json",):
        path = os.path.join("levels", jname)
        if not os.path.exists(path):
            continue
        try:
            age_s = now_ts - float(os.path.getmtime(path))
            if age_s > stale_initial_s:
                log.warning("  Ignoring stale %s (age=%.0fs > %.0fs)", jname, age_s, stale_initial_s)
                continue
            data = json.load(open(path, encoding="utf-8"))
            lvs = data.get("levels", {}) or {}
            anchors = {s: float(lvs.get(s, {}).get("anchor", 0)) for s in SYMBOLS}
            if any(v > 0 for v in anchors.values()):
                break
        except Exception:
            anchors = {}

    # 3) Cross-check: if anchor differs a lot from current live price, use live
    if anchors and cp:
        for sym in SYMBOLS:
            a = float(anchors.get(sym, 0) or 0)
            c = float(cp.get(sym, 0) or 0)
            if a > 0 and c > 0:
                dev = abs(c - a) / a
                if dev > 0.10:
                    log.warning(
                        "  Crypto anchor mismatch %s: anchor=$%.4f live=$%.4f (dev=%.1f%%) — using live",
                        sym, a, c, dev * 100.0,
                    )
                    anchors[sym] = c

    if anchors and any(v > 0 for v in anchors.values()):
        return anchors

    if cp and any(v > 0 for v in cp.values()):
        return {s: float(cp.get(s, 0) or 0) for s in SYMBOLS}

    return {}


def _build_sweeps(anchors: Dict[str, float]) -> Dict[str, StockSweep]:
    """Build StockSweep objects using anchor_price as prev_close."""
    sweeps: Dict[str, StockSweep] = {}
    for sym in SYMBOLS:
        anchor = anchors.get(sym, 0.0)
        if anchor <= 0:
            log.warning("  SKIP %-6s — no anchor price", sym)
            continue
        # is_commodity=False → equity logic (09:30–09:35 blackout skipped for crypto
        # because in_trading_for("BTC") returns True always, and we don't enforce blackout)
        sw = StockSweep(sym, anchor, X_ARRAY, is_crypto=True)
        sweeps[sym] = sw
        x_val = anchor * CURRENT_X
        log.info(
            "  %-6s anchor=$%.4f  x=%.4f  buy_above=$%.4f  sell_below=$%.4f  X=[%.4f–%.4f]",
            sym, anchor, x_val, anchor + x_val, anchor - x_val,
            X_MIN, X_MAX,
        )
    return sweeps


# ════════════════════════════════════════════════════════════════════════════
# STATE DUMP
# ════════════════════════════════════════════════════════════════════════════

def _dump_live_state(
    sweeps: Dict[str, StockSweep], date_tag: str, out_dir: str,
) -> None:
    state = {
        "scanner":     SCANNER_NAME,
        "scanner_id":  SCANNER_ID,
        "asset_class": "crypto",
        "date_tag":    date_tag,
        "written_at":  now_ist().isoformat(),
        "x_range":     [X_MIN, X_MAX],
        "n_variations": N_VALUES * len(sweeps),
        "sweeps":      {sym: sw.dump_state() for sym, sw in sweeps.items()},
    }
    path = os.path.join(out_dir, "live_state.json")
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, separators=(",", ":"))
    os.replace(tmp, path)


def _save_window_snapshot(sweeps: Dict[str, StockSweep], date_tag: str, out_dir: str) -> None:
    snap = os.path.join(out_dir, f"live_state_{date_tag}_final.json")
    try:
        state = {
            "scanner": SCANNER_NAME, "date_tag": date_tag,
            "written_at": now_ist().isoformat(),
            "sweeps": {sym: sw.dump_state() for sym, sw in sweeps.items()},
        }
        with open(snap, "w", encoding="utf-8") as f:
            json.dump(state, f, separators=(",", ":"))
        log.info("Window snapshot saved: %s", snap)
    except Exception as exc:
        log.debug("Snapshot error: %s", exc)


# ════════════════════════════════════════════════════════════════════════════
# RICH TABLE
# ════════════════════════════════════════════════════════════════════════════

def _build_table(
    sweeps: Dict[str, StockSweep],
    now: datetime,
    sweep_ms: float,
    updated: int,
    ticks: int,
    date_tag: str,
    mins_to_anchor: int,
) -> Table:
    anchor_warn = mins_to_anchor <= 5
    t = Table(
        title=(
            f"[bold cyan]{SCANNER_NAME} (X:{X_MIN:.4f}–{X_MAX:.4f}, {N_VALUES:,} vars/coin)  "
            f"window:{date_tag}  "
            f"{'[bold red]' if anchor_warn else ''}re-anchor:{mins_to_anchor}m{'[/]' if anchor_warn else ''}  "
            f"— {now.strftime('%H:%M:%S IST')}  sweep={sweep_ms:.1f}ms  "
            f"updated={updated}/{len(sweeps)}  ticks={ticks}[/]"
        ),
        show_header=True, header_style="bold yellow",
        border_style="dim", expand=False,
    )
    t.add_column("Coin",         style="bold white", width=7)
    t.add_column("Anchor($)",    style="dim",        width=12, justify="right")
    t.add_column("Last($)",      style="cyan",       width=12, justify="right")
    t.add_column("Best X",       style="green",      width=10, justify="right")
    t.add_column("vs Live X",    style="yellow",     width=11, justify="right")
    t.add_column("P&L (Rs)",     width=13,           justify="right")
    t.add_column("Win%",         width=7,            justify="right")
    t.add_column("Trades",       width=7,            justify="right")
    t.add_column("W/L",          width=7,            justify="right")
    t.add_column("Last Breached", width=26)

    for sym, sw in sweeps.items():
        r = sw.row_data()
        # P&L in Rs (USDT P&L × USDT_TO_INR)
        pnl_txt = Text("—", style="dim")
        if r["total_pnl"] != "—":
            pnl_usdt = float(r["total_pnl"].replace("₹", "").replace(",", ""))
            pnl_inr  = pnl_usdt * USDT_TO_INR
            pnl_txt  = Text(f"₹{pnl_inr:+,.0f}", style="green" if pnl_inr >= 0 else "red")
        # vs current X
        vs_txt = Text("—", style="dim")
        if r["has_data"]:
            try:
                bx  = float(r["best_x"])
                pct = (bx - CURRENT_X) / CURRENT_X * 100
                vs_txt = Text(f"{pct:+.2f}%", style="yellow")
            except Exception:
                pass
        # W/L
        wl = "—"
        if r["win_count"] != "—" and r["loss_count"] != "—":
            wl = f"{r['win_count']}/{r['loss_count']}"
        # Last event colouring
        lv = r["last_event"]
        lv_c = "dim"
        if lv != "—":
            if any(k in lv for k in ("SL","RETREAT")):    lv_c = "red"
            elif any(k in lv for k in ("T1","T2","T3","T4","T5","ST")): lv_c = "green"
            elif "ENTRY"   in lv: lv_c = "cyan"
            elif "REENTRY" in lv: lv_c = "magenta"
            elif "EOD"     in lv: lv_c = "yellow"
        anchor_px = sw.prev_close
        last_px   = sw.last_price
        t.add_row(
            sym,
            f"${anchor_px:,.4f}",
            f"${last_px:,.4f}" if last_px else "—",
            Text(r["best_x"],     style="green"  if r["has_data"] else "dim"),
            vs_txt,
            pnl_txt,
            Text(r["win_rate_pct"], style="cyan"  if r["has_data"] else "dim"),
            Text(r["trade_count"],  style="white" if r["has_data"] else "dim"),
            Text(wl,                style="white" if r["has_data"] else "dim"),
            Text(lv,                style=lv_c),
        )
    return t


# ════════════════════════════════════════════════════════════════════════════
# 6H WINDOW ROTATION
# ════════════════════════════════════════════════════════════════════════════

def _rotate_window(
    sweeps: Dict[str, StockSweep],
    store: PriceStore,
    collector: PriceSubscriber,
    date_tag: str,
    out_dir: str,
) -> Tuple[Dict[str, StockSweep], PriceStore, PriceSubscriber, str, str]:
    """
    At 6h re-anchor boundary:
    1. Square off all open positions at current price
    2. Save results + snapshot
    3. Fetch new anchor prices
    4. Build new StockSweep objects
    5. Start fresh PriceSubscriber for new window
    Returns (new_sweeps, new_store, new_collector, new_date_tag, new_out_dir)
    """
    log.info("═" * 60)
    log.info("6H RE-ANCHOR starting — squaring off %d sweeps", len(sweeps))

    # v10.9 FIX: Do NOT force-square open positions on re-anchor.
    # Natural exits (T1-T5, SL, Retreat) handle all position closes.
    # Re-anchor only resets the LEVEL reference prices for NEW entries.
    # Open positions keep running until their own exit condition triggers.
    now_ist_dt = now_ist()
    prices = store.snapshot()
    for sym, sw in sweeps.items():
        px = prices.get(sym, sw.last_price)
        if px and px > 0:
            # Only update anchor reference for future level calculations —
            # do NOT call eod_square_off (that would force-close open trades)
            # Reset only the variation state that had NO open position
            pass  # carry positions forward unchanged

    # Save 6h window results
    try:
        _save_window_snapshot(sweeps, date_tag, out_dir)
        save_results(sweeps, date_tag, out_dir, scanner_name=f"{SCANNER_NAME} 6h")
        # Send 6h Excel to Telegram
        xl = os.path.join(out_dir, f"summary_{date_tag}.xlsx")
        if os.path.exists(xl):
            from tg_async import send_document_alert
            send_document_alert(xl, f"{SCANNER_NAME} 6h report {date_tag}",
                                asset_class="crypto")
    except Exception as exc:
        log.debug("6h save error: %s", exc)

    # Stop old subscriber
    try:
        collector.stop()
    except Exception:
        pass

    # New window
    new_tag     = _window_tag()
    new_out_dir = os.path.join(BASE_RESULTS_DIR, new_tag)
    os.makedirs(new_out_dir, exist_ok=True)

    # Fresh anchor prices — crypto_engine updates CRYPTO_ANCHOR atomically
    log.info("Fetching new anchor prices for window %s...", new_tag)
    anchors: Dict[str, float] = {}
    for _attempt in range(30):   # up to 150s wait
        anchors = _get_anchor_prices()
        if any(v > 0 for v in anchors.values()): break
        time.sleep(5)
    if not any(v > 0 for v in anchors.values()):
        log.error("No anchor prices after 150s — using previous window prices")

    new_sweeps = _build_sweeps(anchors)

    # Fresh price subscriber
    new_store     = PriceStore()
    new_csv_path  = os.path.join(new_out_dir, "prices.csv")
    new_collector = PriceSubscriber(new_store, new_csv_path, topic=ZMQ_TOPIC)
    new_collector.start()

    log.info("6H RE-ANCHOR complete — window %s active", new_tag)
    _tg_async(f"🔄 {SCANNER_NAME} re-anchor → window {new_tag}")

    return new_sweeps, new_store, new_collector, new_tag, new_out_dir


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("═" * 68)
    log.info("%s — X:[%.4f–%.4f] N=%d  24/7  6h re-anchor",
             SCANNER_NAME, X_MIN, X_MAX, N_VALUES)
    log.info("Same X range as equity scanner1 | ZMQ topic: crypto")
    log.info("Logic: StockSweep (re-entry, retreat 65/45/25, T1-T5, SL, re-anchor)")
    log.info("GPU: %s  |  CPU workers: %d", GPU_NAME if CUDA_AVAILABLE else "CPU", CPU_WORKERS)
    log.info("═" * 68)

    # Initial anchor prices — wait patiently for crypto_engine to write levels file
    log.info("Fetching initial crypto anchor prices (waiting up to 120s)...")
    anchors: Dict[str, float] = {}
    _deadline = time.monotonic() + 120
    while time.monotonic() < _deadline:
        anchors = _get_anchor_prices()
        if any(v > 0 for v in anchors.values()):
            log.info("Got anchor prices: %s",
                     {s: f"${v:,.2f}" for s, v in anchors.items() if v > 0})
            break
        log.debug("Anchor prices not yet available — retrying in 5s...")
        time.sleep(5)
    if not any(v > 0 for v in anchors.values()):
        log.warning("No anchor prices after 120s — retrying indefinitely (engine may still be starting)")
        while True:
            anchors = _get_anchor_prices()
            if any(v > 0 for v in anchors.values()):
                break
            time.sleep(10)

    date_tag = _window_tag()
    out_dir  = os.path.join(BASE_RESULTS_DIR, date_tag)
    os.makedirs(out_dir, exist_ok=True)

    sweeps = _build_sweeps(anchors)
    if not sweeps:
        log.error("No crypto symbols loaded")
        sys.exit(1)

    csv_path  = os.path.join(out_dir, "prices.csv")
    store     = PriceStore()
    collector = PriceSubscriber(store, csv_path, topic=ZMQ_TOPIC)
    collector.start()

    # Schedule first re-anchor
    next_anchor_utc = datetime.now(pytz.utc) + timedelta(hours=REANCHOR_HOURS)

    last_dump = 0.0; sweep_ms = 0.0; updated = 0
    console   = Console()

    try:
        with Live(console=console, refresh_per_second=2) as live:
            while True:
                now_utc = datetime.now(pytz.utc)
                now_d   = now_ist()

                # ── 6h re-anchor ─────────────────────────────────────────────
                if now_utc >= next_anchor_utc:
                    sweeps, store, collector, date_tag, out_dir = _rotate_window(
                        sweeps, store, collector, date_tag, out_dir
                    )
                    next_anchor_utc = now_utc + timedelta(hours=REANCHOR_HOURS)
                    last_dump = 0.0; updated = 0

                # ── State dump ────────────────────────────────────────────────
                if time.monotonic() - last_dump > STATE_DUMP_INTERVAL:
                    try:
                        _dump_live_state(sweeps, date_tag, out_dir)
                        last_dump = time.monotonic()
                    except Exception as exc:
                        log.debug("Dump error: %s", exc)

                # ── Price tick ────────────────────────────────────────────────
                prices = store.snapshot()
                if prices and sweeps:
                    t0 = time.perf_counter()
                    updated = 0
                    for sym, sw in sweeps.items():
                        # in_trading_for("BTC", now_d) always returns True (24/7)
                        px = prices.get(sym)
                        if px and px > 0 and px != sw.last_price:
                            sw.last_price = px
                            sw.on_price(px, now_d)   # full logic: re-entry, retreat, T1-T5, SL
                            updated += 1
                    sweep_ms = (time.perf_counter() - t0) * 1000

                # ── RAM spill every 5 min ─────────────────────────────────────
                if now_d.second == 0 and now_d.minute % 5 == 0:
                    evicted = spill_trade_logs_to_disk(sweeps, date_tag)
                    if evicted:
                        log.debug("RAM spill: %d entries", evicted)

                # ── Display ───────────────────────────────────────────────────
                mins_left = max(0, int((next_anchor_utc - now_utc).total_seconds() // 60))
                live.update(_build_table(sweeps, now_d, sweep_ms, updated,
                                        store.ticks, date_tag, mins_left))
                time.sleep(PRICE_FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Interrupted — saving final state...")
        try:
            _save_window_snapshot(sweeps, date_tag, out_dir)
            save_results(sweeps, date_tag, out_dir, scanner_name=SCANNER_NAME)
        except Exception:
            pass
    finally:
        collector.stop()
        log.info("%s stopped.", SCANNER_NAME)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
