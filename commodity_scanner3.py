# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v9.0 | Author: Ridhaant Ajoy Thackur
# commodity_scanner3.py — MCX Wide-Dual Sweep + Cross-Scanner Fusion (60K total)
# ═══════════════════════════════════════════════════════════════════════
"""
commodity_scanner3.py — MCX WideDual + Cross-Fusion  v9.0
==========================================================
Wide-Lower band: 0.20×–1.20× of calibrated COMM_X (conservative/deep)
Wide-Upper band: 1.50×–3.00× of calibrated COMM_X (aggressive/extreme)
5 symbols × 12,000 = 60,000 total variations/day

Cross-fusion reads CS1 + CS2 live_state.json every 60s and computes
the global best X across all 3 commodity scanners (97,500 total).

Full equity parity (all StockSweep logic including re-entry, retreat,
T1–T5/ST1–ST5, SL, premarket adjust, 09:30 re-anchor, EOD 23:30).
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
    save_results, spill_trade_logs_to_disk,
    now_ist, in_premarket, after_930, in_trading_for,
    in_commodity_session, is_commodity_eod,
    IST, BROKERAGE_PER_SIDE, CPU_WORKERS, CUDA_AVAILABLE, GPU_NAME,
)
from config import cfg

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [CommS3] %(levelname)s — %(message)s")
log = logging.getLogger("commodity_scanner3")

SCANNER_NAME = "Comm-Scanner-3 WideDual+Fusion"
SCANNER_ID   = "comm3"
SYMBOLS      = ["GOLD", "SILVER", "CRUDE", "NATURALGAS", "COPPER"]
ZMQ_TOPIC    = b"commodity"
RESULTS_DIR  = os.path.join("sweep_results", "commodity_scanner3")
PRICE_FETCH_INTERVAL  = 1.0
STATE_DUMP_INTERVAL   = 30
CROSS_FUSION_INTERVAL = 60

COMM_WIDE_LOWER: Dict[str, Tuple[float, float, int]] = {
    "GOLD":       (0.000686, 0.004116, 6_000),
    "SILVER":     (0.001029, 0.006174, 6_000),
    "NATURALGAS": (0.000171, 0.001029, 6_000),
    "CRUDE":      (0.000120, 0.000722, 6_000),
    "COPPER":     (0.000800, 0.004800, 6_000),
}
COMM_WIDE_UPPER: Dict[str, Tuple[float, float, int]] = {
    "GOLD":       (0.005145, 0.010290, 6_000),
    "SILVER":     (0.007718, 0.015435, 6_000),
    "NATURALGAS": (0.001286, 0.002572, 6_000),
    "CRUDE":      (0.000903, 0.001806, 6_000),
    "COPPER":     (0.006000, 0.012000, 6_000),
}

CS1_DIR = os.path.join("sweep_results", "commodity_scanner1")
CS2_DIR = os.path.join("sweep_results", "commodity_scanner2")
CURRENT_X = cfg.COMM_X


def _tg_async(text: str) -> None:
    try:
        from tg_async import send_alert
        send_alert(text, asset_class="commodity")
    except Exception:
        pass


def _load_state(dir_path: str, date_str: str) -> Optional[dict]:
    path = os.path.join(dir_path, date_str, "live_state.json")
    try:
        if os.path.exists(path):
            return json.load(open(path, encoding="utf-8"))
    except Exception:
        pass
    return None


def _merged_best(sl: Dict[str, StockSweep], su: Dict[str, StockSweep]) -> Dict[str, dict]:
    merged: Dict[str, dict] = {}
    for sym in set(sl) | set(su):
        dl = sl[sym].dump_state() if sym in sl else {}
        du = su[sym].dump_state() if sym in su else {}
        pl = float(dl.get("best_pnl", -1e9))
        pu = float(du.get("best_pnl", -1e9))
        if pl >= pu:
            merged[sym] = {"best_x": dl.get("best_x", 0), "pnl": pl,
                           "band": "lower", "source": "cs3_lower",
                           "win_rate": dl.get("best_win_rate", 0),
                           "trade_count": dl.get("best_trade_count", 0)}
        else:
            merged[sym] = {"best_x": du.get("best_x", 0), "pnl": pu,
                           "band": "upper", "source": "cs3_upper",
                           "win_rate": du.get("best_win_rate", 0),
                           "trade_count": du.get("best_trade_count", 0)}
    return merged


def _cross_fusion(
    sl: Dict[str, StockSweep], su: Dict[str, StockSweep],
    cs1_state: Optional[dict], cs2_state: Optional[dict],
) -> Dict[str, dict]:
    """Compute global best X per symbol across CS1 + CS2 + CS3."""
    cross: Dict[str, dict] = {}
    for sym in SYMBOLS:
        candidates: List[dict] = []
        # CS1 single-band
        if cs1_state and "sweeps" in cs1_state:
            d = cs1_state["sweeps"].get(sym, {})
            if d.get("best_x", 0) > 0:
                candidates.append({"best_x": d["best_x"], "pnl": d.get("best_pnl", -1e9),
                                    "source": "cs1", "win_rate": d.get("best_win_rate", 0),
                                    "trade_count": d.get("best_trade_count", 0)})
        # CS2 merged_best
        if cs2_state and "merged_best" in cs2_state:
            d = cs2_state["merged_best"].get(sym, {})
            if d.get("best_x", 0) > 0:
                candidates.append({"best_x": d["best_x"], "pnl": d.get("pnl", -1e9),
                                    "source": f"cs2_{d.get('band','?')}",
                                    "win_rate": d.get("win_rate", 0),
                                    "trade_count": d.get("trade_count", 0)})
        # CS3 own bands
        for sw, label in ((sl.get(sym), "cs3_lower"), (su.get(sym), "cs3_upper")):
            if sw:
                d = sw.dump_state()
                if d.get("best_x", 0) > 0:
                    candidates.append({"best_x": d["best_x"], "pnl": d.get("best_pnl", -1e9),
                                       "source": label, "win_rate": d.get("best_win_rate", 0),
                                       "trade_count": d.get("best_trade_count", 0)})
        if candidates:
            cross[sym] = max(candidates, key=lambda c: c["pnl"])
    return cross


def _dump_live_state(
    sl: Dict[str, StockSweep], su: Dict[str, StockSweep],
    cross: Dict[str, dict], date_str: str, out_dir: str,
) -> None:
    merged = _merged_best(sl, su)
    state: Dict[str, Any] = {
        "scanner":    SCANNER_NAME,
        "scanner_id": SCANNER_ID,
        "asset_class": "commodity",
        "date":       date_str,
        "written_at": now_ist().isoformat(),
        "total_variations": sum(COMM_WIDE_LOWER[s][2] + COMM_WIDE_UPPER[s][2] for s in SYMBOLS),
        "total_all_comm_scanners": 5_000 + 32_500 + 60_000,   # CS1+CS2+CS3
        "merged_best":        merged,
        "cross_scanner_best": cross,
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
    cross: Dict[str, dict], now: datetime, sweep_ms: float, ticks: int,
) -> Table:
    t = Table(
        title=(
            f"[bold cyan]{SCANNER_NAME}  6K+6K/sym → 60K  "
            f"Cross-fusion:{len(cross)} syms  "
            f"— {now.strftime('%H:%M:%S IST')}  sweep={sweep_ms:.1f}ms  ticks={ticks}[/]"
        ),
        show_header=True, header_style="bold yellow", border_style="dim", expand=False,
    )
    t.add_column("Symbol",       style="bold white", width=14)
    t.add_column("S3 Best X",    style="green",      width=10, justify="right")
    t.add_column("Band",         style="magenta",    width=7)
    t.add_column("S3 P&L",       width=13,           justify="right")
    t.add_column("Global Best X", style="cyan",      width=12, justify="right")
    t.add_column("Source",       style="yellow",     width=12)
    t.add_column("Global P&L",   width=13,           justify="right")

    merged = _merged_best(sl, su)
    for sym in SYMBOLS:
        m  = merged.get(sym, {})
        cb = cross.get(sym, {})
        s3_pnl = m.get("pnl", 0.0)
        gl_pnl = cb.get("pnl", 0.0)
        src    = cb.get("source", "—")
        src_c  = "cyan" if "cs1" in src else ("magenta" if "cs2" in src else "yellow")
        t.add_row(
            sym,
            Text(f"{m.get('best_x',0):.6f}", style="green") if m.get("best_x") else Text("—", style="dim"),
            Text(m.get("band", "—"), style="magenta" if m.get("band") == "lower" else "cyan"),
            Text(f"₹{s3_pnl:+,.0f}", style="green" if s3_pnl >= 0 else "red"),
            Text(f"{cb.get('best_x',0):.6f}", style="cyan") if cb.get("best_x") else Text("—", style="dim"),
            Text(src, style=src_c),
            Text(f"₹{gl_pnl:+,.0f}", style="green" if gl_pnl >= 0 else "red"),
        )
    return t


def _load_prev_closes(date_str: str) -> Dict[str, float]:
    result: Dict[str, float] = {}
    stale_levels_s = 36 * 3600.0  # ignore old anchors >36h
    now_ts = time.time()
    path = os.path.join("levels", f"commodity_initial_levels_{date_str}.json")
    if os.path.exists(path):
        age_s = now_ts - float(os.path.getmtime(path))
        if age_s > stale_levels_s:
            log.warning("  Ignoring stale commodity_initial_levels (%s) age=%.0fs > %.0fs",
                        os.path.basename(path), age_s, stale_levels_s)
            path = ""
        try:
            if not path:
                raise FileNotFoundError
            data = json.load(open(path, encoding="utf-8"))
            lvs  = data.get("levels", data)
            for sym in SYMBOLS:
                e  = lvs.get(sym, {})
                pc = (e.get("prev_close") or e.get("anchor")) if isinstance(e, dict) else e
                if pc and float(pc) > 0:
                    result[sym] = float(pc)
            if result: return result
        except Exception: pass
    _YF = {"GOLD":"GC=F","SILVER":"SI=F","CRUDE":"CL=F","NATURALGAS":"NG=F","COPPER":"HG=F"}
    for sym in SYMBOLS:
        if sym in result: continue
        try:
            import yfinance as yf
            df = yf.Ticker(_YF.get(sym, f"{sym}=F")).history(period="5d", interval="1d")
            if df is not None and not df.empty:
                result[sym] = float(df["Close"].iloc[-1])
        except Exception: pass

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

    log.info("═" * 70)
    log.info("%s", SCANNER_NAME)
    log.info("  Wide-Lower: 6K/sym  Wide-Upper: 6K/sym  Total: 60K")
    log.info("  Cross-fusion: reads CS1+CS2 every %ds → 97.5K global", CROSS_FUSION_INTERVAL)
    log.info("  Full logic: re-entry, retreat, T1-T5, SL, premarket, 09:30 re-anchor")
    log.info("═" * 70)

    if not MarketCalendar.is_trading_day(now):
        # v10.6 FIX: sleep-wait until next MCX trading day instead of exiting.
        while not MarketCalendar.is_trading_day(now_ist()):
            nd = MarketCalendar.next_trading_day(now_ist())
            log.info("MCX Scanner3 standby — not a trading day. Next: %s  (sleeping 5 min)",
                     nd.strftime("%a %d %b %Y"))
            time.sleep(300)
        log.info("MCX Scanner3 — trading day detected, starting sweep...")
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
        lo, hi, nl = COMM_WIDE_LOWER[sym]
        lo2, hi2, nu = COMM_WIDE_UPPER[sym]
        sl[sym] = StockSweep(sym, pc, np.linspace(lo, hi, nl),  is_commodity=True)
        su[sym] = StockSweep(sym, pc, np.linspace(lo2, hi2, nu), is_commodity=True)
        log.info("  OK %-14s pc=%.2f L=[%.6f–%.6f] U=[%.6f–%.6f]",
                 sym, pc, lo, hi, lo2, hi2)

    if not sl:
        log.error("No symbols loaded")
        sys.exit(1)

    csv_path  = os.path.join(out_dir, "prices.csv")
    store     = PriceStore()
    collector = PriceSubscriber(store, csv_path, topic=ZMQ_TOPIC)
    collector.start()

    eod_done = False; anchor_done = False
    last_dump = 0.0; last_fusion = 0.0; sweep_ms = 0.0
    cross: Dict[str, dict] = {}; console = Console()

    try:
        with Live(
            _build_table(sl, su, cross, now, 0.0, 0),
            console=console, refresh_per_second=2,
        ) as live:
            while True:
                now = now_ist()

                # Cross-scanner fusion
                if time.monotonic() - last_fusion > CROSS_FUSION_INTERVAL:
                    try:
                        cs1 = _load_state(CS1_DIR, date_str)
                        cs2 = _load_state(CS2_DIR, date_str)
                        cross = _cross_fusion(sl, su, cs1, cs2)
                        last_fusion = time.monotonic()
                        log.debug("Cross-fusion: %d symbols updated", len(cross))
                    except Exception as exc:
                        log.debug("Fusion error: %s", exc)

                # State dump
                if time.monotonic() - last_dump > STATE_DUMP_INTERVAL:
                    try:
                        _dump_live_state(sl, su, cross, date_str, out_dir)
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
                    live.update(_build_table(sl, su, cross, now, sweep_ms, store.ticks))
                    time.sleep(30)
                    continue

                # Premarket adjust
                if in_premarket(now) and not anchor_done:
                    prices = store.snapshot()
                    for sym in sl:
                        px = prices.get(sym)
                        if px and px > 0:
                            sl[sym].last_price = px
                            su[sym].last_price = px
                            sl[sym].premarket_adjust(px)
                            su[sym].premarket_adjust(px)
                    live.update(_build_table(sl, su, cross, now, sweep_ms, store.ticks))
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

                # Main sweep — both bands parallel
                prices = store.snapshot()
                if prices:
                    t0 = time.perf_counter()
                    def _lo():
                        for sym, sw in sl.items():
                            if not in_trading_for(sym, now): continue
                            px = prices.get(sym)
                            if px and px > 0:
                                sw.last_price = px; sw.on_price(px, now)
                    def _hi():
                        for sym, sw in su.items():
                            if not in_trading_for(sym, now): continue
                            px = prices.get(sym)
                            if px and px > 0:
                                sw.last_price = px; sw.on_price(px, now)
                    tl = threading.Thread(target=_lo, daemon=True)
                    tu = threading.Thread(target=_hi, daemon=True)
                    tl.start(); tu.start(); tl.join(); tu.join()
                    sweep_ms = (time.perf_counter() - t0) * 1000

                # RAM spill
                if now.second == 0 and now.minute % 5 == 0:
                    evicted = (spill_trade_logs_to_disk(sl, date_str) +
                               spill_trade_logs_to_disk(su, date_str))
                    if evicted:
                        log.debug("RAM spill: %d entries", evicted)

                live.update(_build_table(sl, su, cross, now, sweep_ms, store.ticks))
                time.sleep(PRICE_FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Stopped")
    finally:
        collector.stop()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
