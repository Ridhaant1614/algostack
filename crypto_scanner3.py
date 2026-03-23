# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v9.0 | Author: Ridhaant Ajoy Thackur
# crypto_scanner3.py — Crypto Wide-Dual Sweep + Cross-Fusion (same ranges as equity scanner3)
# ═══════════════════════════════════════════════════════════════════════
"""
crypto_scanner3.py — Crypto WideDual + Cross-Fusion  v9.0
==========================================================
Wide-Lower: X ∈ [0.001, 0.016]  18,000 vars/coin — SAME as equity scanner3 lower
Wide-Upper: X ∈ [0.016, 0.032]  17,000 vars/coin — SAME as equity scanner3 upper
5 coins × 35,000 = 175,000 variations per 6h window (700,000/day)

Cross-fusion reads CR1 + CR2 live_state.json every 60s.
Full equity parity: re-entry, retreat 65/45/25, T1-T5/ST1-ST5, SL.
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
    now_ist, in_trading_for,
    IST, BROKERAGE_PER_SIDE, CPU_WORKERS, CUDA_AVAILABLE, GPU_NAME,
)
from config import cfg

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [CrS3] %(levelname)s — %(message)s")
log = logging.getLogger("crypto_scanner3")

SCANNER_NAME = "Crypto-Scanner-3 WideDual+Fusion"
SCANNER_ID   = "crypto3"
SYMBOLS      = ["BTC", "ETH", "BNB", "SOL", "ADA"]
ZMQ_TOPIC    = b"crypto"

# Same ranges as equity scanner3
X_LOWER_MIN=0.001000; X_LOWER_MAX=0.016000; N_LOWER=18_000
X_UPPER_MIN=0.016000; X_UPPER_MAX=0.032000; N_UPPER=17_000
X_LOWER_NP = np.linspace(X_LOWER_MIN, X_LOWER_MAX, N_LOWER)
X_UPPER_NP = np.linspace(X_UPPER_MIN, X_UPPER_MAX, N_UPPER)

REANCHOR_HOURS       = 6
STATE_DUMP_INTERVAL  = 30
CROSS_FUSION_INTERVAL= 60
PRICE_FETCH_INTERVAL = 1.0
USDT_TO_INR  = cfg.USDT_TO_INR
CURRENT_X    = cfg.CRYPTO_X_MULTIPLIER

BASE_RESULTS_DIR = os.path.join("sweep_results", "crypto_scanner3")
CR1_DIR = os.path.join("sweep_results", "crypto_scanner1")
CR2_DIR = os.path.join("sweep_results", "crypto_scanner2")


def _tg_async(text):
    try:
        from tg_async import send_alert; send_alert(text, asset_class="crypto")
    except Exception: pass


def _window_tag():
    return datetime.now(pytz.utc).strftime("%Y%m%d_%H%M")


def _get_anchor_prices():
    """Read anchor prices from crypto_engine output files (cross-process safe)."""
    stale_initial_s = 2 * 3600.0
    stale_live_s    = 90.0
    now_ts = time.time()

    # 1) Load live prices for fallback / reconciliation
    lp = os.path.join("levels", "live_prices.json")
    cp: Dict[str, float] = {}
    if os.path.exists(lp):
        try:
            lp_age_s = now_ts - float(os.path.getmtime(lp))
            if lp_age_s > stale_live_s:
                # Avoid noisy logs here; this runs at re-anchor boundaries
                log.warning("  live_prices.json is stale (age=%.0fs)", lp_age_s)
            data = json.load(open(lp, encoding="utf-8"))
            cp_raw = data.get("crypto_prices", {}) or {}
            cp = {s: float(cp_raw.get(s, 0) or 0) for s in SYMBOLS}
        except Exception:
            cp = {}

    # 2) Read initial anchors
    path = os.path.join("levels", "crypto_initial_levels_latest.json")
    anchors: Dict[str, float] = {}
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

    # 3) Cross-check mismatch >10% → use live for that symbol
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


def _load_state(dir_path, date_tag):
    """Load live_state.json from most recent window in dir_path."""
    # Try exact tag first, then newest available
    path = os.path.join(dir_path, date_tag, "live_state.json")
    if os.path.exists(path):
        try: return json.load(open(path,encoding="utf-8"))
        except Exception: pass
    try:
        if os.path.isdir(dir_path):
            wins = sorted([d for d in os.listdir(dir_path)
                           if os.path.isdir(os.path.join(dir_path,d))], reverse=True)
            for w in wins[:3]:
                p = os.path.join(dir_path,w,"live_state.json")
                if os.path.exists(p):
                    try: return json.load(open(p,encoding="utf-8"))
                    except Exception: pass
    except Exception: pass
    return None


def _merged_best(sl, su):
    merged = {}
    for sym in set(sl)|set(su):
        dl=sl[sym].dump_state() if sym in sl else {}
        du=su[sym].dump_state() if sym in su else {}
        pl=float(dl.get("best_pnl",-1e9)); pu=float(du.get("best_pnl",-1e9))
        if pl>=pu: merged[sym]={"best_x":dl.get("best_x",0),"pnl":pl,"band":"lower","source":"cr3_lower",
                                 "win_rate":dl.get("best_win_rate",0),"trade_count":dl.get("best_trade_count",0)}
        else:      merged[sym]={"best_x":du.get("best_x",0),"pnl":pu,"band":"upper","source":"cr3_upper",
                                 "win_rate":du.get("best_win_rate",0),"trade_count":du.get("best_trade_count",0)}
    return merged


def _cross_fusion(sl, su, cr1_state, cr2_state):
    cross = {}
    for sym in SYMBOLS:
        candidates = []
        # CR1 single-band sweeps
        if cr1_state and "sweeps" in cr1_state:
            d = cr1_state["sweeps"].get(sym, {})
            if d.get("best_x",0) > 0:
                candidates.append({"best_x":d["best_x"],"pnl":d.get("best_pnl",-1e9),
                                    "source":"cr1","win_rate":d.get("best_win_rate",0),
                                    "trade_count":d.get("best_trade_count",0)})
        # CR2 merged_best
        if cr2_state and "merged_best" in cr2_state:
            d = cr2_state["merged_best"].get(sym, {})
            if d.get("best_x",0) > 0:
                candidates.append({"best_x":d["best_x"],"pnl":d.get("pnl",-1e9),
                                    "source":f"cr2_{d.get('band','?')}",
                                    "win_rate":d.get("win_rate",0),"trade_count":d.get("trade_count",0)})
        # CR3 own bands
        for sw, label in ((sl.get(sym),"cr3_lower"),(su.get(sym),"cr3_upper")):
            if sw:
                d = sw.dump_state()
                if d.get("best_x",0) > 0:
                    candidates.append({"best_x":d["best_x"],"pnl":d.get("best_pnl",-1e9),
                                        "source":label,"win_rate":d.get("best_win_rate",0),
                                        "trade_count":d.get("best_trade_count",0)})
        if candidates:
            cross[sym] = max(candidates, key=lambda c: c["pnl"])
    return cross


def _dump_live_state(sl, su, cross, date_tag, out_dir):
    merged = _merged_best(sl, su)
    state: Dict[str,Any] = {
        "scanner": SCANNER_NAME, "scanner_id": SCANNER_ID, "asset_class": "crypto",
        "date_tag": date_tag, "written_at": now_ist().isoformat(),
        "total_variations": (N_LOWER+N_UPPER)*len(sl),
        "merged_best": merged, "cross_scanner_best": cross,
        "bands": {"lower": {"sweeps": {s:sw.dump_state() for s,sw in sl.items()}},
                  "upper": {"sweeps": {s:sw.dump_state() for s,sw in su.items()}}}}
    path=os.path.join(out_dir,"live_state.json"); tmp=path+".tmp"
    with open(tmp,"w",encoding="utf-8") as f: json.dump(state,f,separators=(",",":"))
    os.replace(tmp,path)


def _build_table(sl, su, cross, now, sweep_ms, ticks, date_tag, mins):
    merged = _merged_best(sl, su)
    aw = mins <= 5
    t = Table(
        title=(f"[bold cyan]{SCANNER_NAME}  L:18K+U:17K/coin → 175K/window  "
               f"Cross:{len(cross)} syms  window:{date_tag}  "
               f"{'[bold red]' if aw else ''}re-anchor:{mins}m{'[/]' if aw else ''}  "
               f"— {now.strftime('%H:%M:%S IST')}  sweep={sweep_ms:.1f}ms  ticks={ticks}[/]"),
        show_header=True, header_style="bold yellow", border_style="dim", expand=False)
    t.add_column("Coin",      style="bold white", width=7)
    t.add_column("S3 Best X", style="green",      width=10, justify="right")
    t.add_column("Band",      style="magenta",    width=7)
    t.add_column("S3 P&L(Rs)",width=13,           justify="right")
    t.add_column("Global X",  style="cyan",       width=10, justify="right")
    t.add_column("Source",    style="yellow",     width=12)
    t.add_column("Global P&L(Rs)", width=13,      justify="right")
    for sym in SYMBOLS:
        m=merged.get(sym,{}); cb=cross.get(sym,{})
        s3_pnl=m.get("pnl",0)*USDT_TO_INR; gl_pnl=cb.get("pnl",0)*USDT_TO_INR
        src=cb.get("source","—")
        src_c = "cyan" if "cr1" in src else ("magenta" if "cr2" in src else "yellow")
        t.add_row(
            sym,
            Text(f"{m.get('best_x',0):.6f}",style="green") if m.get("best_x") else Text("—",style="dim"),
            Text(m.get("band","—"),style="magenta" if m.get("band")=="lower" else "cyan"),
            Text(f"₹{s3_pnl:+,.0f}",style="green" if s3_pnl>=0 else "red"),
            Text(f"{cb.get('best_x',0):.6f}",style="cyan") if cb.get("best_x") else Text("—",style="dim"),
            Text(src,style=src_c),
            Text(f"₹{gl_pnl:+,.0f}",style="green" if gl_pnl>=0 else "red"),
        )
    return t


def _build_sweeps(anchors):
    sl: Dict[str,StockSweep]={};  su: Dict[str,StockSweep]={}
    for sym in SYMBOLS:
        a=anchors.get(sym,0.0)
        if a<=0: log.warning("SKIP %s",sym); continue
        sl[sym]=StockSweep(sym,a,X_LOWER_NP,is_crypto=True)
        su[sym]=StockSweep(sym,a,X_UPPER_NP,is_crypto=True)
        log.info("  %-6s anchor=$%.4f",sym,a)
    return sl,su


def _rotate_window(sl,su,store,collector,date_tag,out_dir):
    log.info("6H RE-ANCHOR...")
    now_d=now_ist(); prices=store.snapshot()
    for sym in list(sl):
        px=prices.get(sym,sl[sym].last_price)
        if px and px>0: pass  # v10.9: no force-square on re-anchor
    try:
        save_results(sl,date_tag,os.path.join(out_dir,"band_lower"),scanner_name=f"{SCANNER_NAME}-Lower")
        save_results(su,date_tag,os.path.join(out_dir,"band_upper"),scanner_name=f"{SCANNER_NAME}-Upper")
        xl=os.path.join(out_dir,"band_lower",f"summary_{date_tag}.xlsx")
        if os.path.exists(xl):
            from tg_async import send_document_alert
            send_document_alert(xl,f"{SCANNER_NAME} 6h report {date_tag}",asset_class="crypto")
    except Exception as e: log.debug("6h save: %s",e)
    try: collector.stop()
    except Exception: pass
    new_tag=_window_tag(); new_dir=os.path.join(BASE_RESULTS_DIR,new_tag)
    os.makedirs(new_dir,exist_ok=True)
    anchors: Dict[str,float]={}
    for _att in range(30):
        anchors=_get_anchor_prices()
        if any(v>0 for v in anchors.values()): break
        time.sleep(5)
    new_sl,new_su=_build_sweeps(anchors)
    new_store=PriceStore(); new_csv=os.path.join(new_dir,"prices.csv")
    new_coll=PriceSubscriber(new_store,new_csv,topic=ZMQ_TOPIC); new_coll.start()
    log.info("6H RE-ANCHOR complete — window %s",new_tag)
    _tg_async(f"🔄 {SCANNER_NAME} re-anchor → window {new_tag}")
    return new_sl,new_su,new_store,new_coll,new_tag,new_dir


def main():
    log.info("═"*70)
    log.info("%s | L:18K+U:17K/coin → 175K/6h  24/7",SCANNER_NAME)
    log.info("Same X ranges as equity scanner3 | Cross-fusion: CR1+CR2+CR3")
    log.info("Full logic: re-entry, retreat 65/45/25, T1-T5, SL, 6h re-anchor")
    log.info("═"*70)

    anchors: Dict[str,float] = {}
    _deadline = time.monotonic() + 120
    log.info("Waiting for crypto_engine anchor prices (up to 120s)...")
    while time.monotonic() < _deadline:
        anchors = _get_anchor_prices()
        if any(v>0 for v in anchors.values()): break
        time.sleep(5)
    if not any(v>0 for v in anchors.values()):
        log.warning("No anchors after 120s — waiting indefinitely")
        while True:
            anchors = _get_anchor_prices()
            if any(v>0 for v in anchors.values()): break
            time.sleep(10)

    date_tag=_window_tag(); out_dir=os.path.join(BASE_RESULTS_DIR,date_tag)
    os.makedirs(out_dir,exist_ok=True)
    sl,su=_build_sweeps(anchors)
    if not sl: log.error("No symbols"); sys.exit(1)

    csv_path=os.path.join(out_dir,"prices.csv"); store=PriceStore()
    collector=PriceSubscriber(store,csv_path,topic=ZMQ_TOPIC); collector.start()

    next_anchor=datetime.now(pytz.utc)+timedelta(hours=REANCHOR_HOURS)
    last_dump=0.0; last_fusion=0.0; sweep_ms=0.0; cross={}; console=Console()

    try:
        with Live(console=console,refresh_per_second=2) as live:
            while True:
                now_utc=datetime.now(pytz.utc); now_d=now_ist()

                # Cross-fusion
                if time.monotonic()-last_fusion>CROSS_FUSION_INTERVAL:
                    try:
                        cr1=_load_state(CR1_DIR,date_tag); cr2=_load_state(CR2_DIR,date_tag)
                        cross=_cross_fusion(sl,su,cr1,cr2); last_fusion=time.monotonic()
                        log.debug("Cross-fusion: %d symbols",len(cross))
                    except Exception as e: log.debug("Fusion: %s",e)

                # State dump
                if time.monotonic()-last_dump>STATE_DUMP_INTERVAL:
                    try: _dump_live_state(sl,su,cross,date_tag,out_dir); last_dump=time.monotonic()
                    except Exception as e: log.debug("Dump: %s",e)

                # 6h re-anchor
                if now_utc>=next_anchor:
                    sl,su,store,collector,date_tag,out_dir=_rotate_window(
                        sl,su,store,collector,date_tag,out_dir)
                    next_anchor=now_utc+timedelta(hours=REANCHOR_HOURS)
                    last_dump=0.0; cross={}

                # Main sweep — both bands parallel
                prices=store.snapshot()
                if prices and sl:
                    t0=time.perf_counter()
                    def _lo():
                        for sym,sw in sl.items():
                            px=prices.get(sym)
                            if px and px>0 and px!=sw.last_price:
                                sw.last_price=px; sw.on_price(px,now_d)
                    def _hi():
                        for sym,sw in su.items():
                            px=prices.get(sym)
                            if px and px>0 and px!=sw.last_price:
                                sw.last_price=px; sw.on_price(px,now_d)
                    tl=threading.Thread(target=_lo,daemon=True)
                    tu=threading.Thread(target=_hi,daemon=True)
                    tl.start();tu.start();tl.join();tu.join()
                    sweep_ms=(time.perf_counter()-t0)*1000

                # RAM spill
                if now_d.second==0 and now_d.minute%5==0:
                    ev=(spill_trade_logs_to_disk(sl,date_tag)+
                        spill_trade_logs_to_disk(su,date_tag))
                    if ev: log.debug("RAM spill: %d",ev)

                mins=max(0,int((next_anchor-now_utc).total_seconds()//60))
                live.update(_build_table(sl,su,cross,now_d,sweep_ms,store.ticks,date_tag,mins))
                time.sleep(PRICE_FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Stopped")
        try:
            save_results(sl,date_tag,os.path.join(out_dir,"band_lower"),scanner_name=f"{SCANNER_NAME}-Lower")
            save_results(su,date_tag,os.path.join(out_dir,"band_upper"),scanner_name=f"{SCANNER_NAME}-Upper")
        except Exception: pass
    finally:
        collector.stop()


if __name__=="__main__":
    multiprocessing.freeze_support(); main()
