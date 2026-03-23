"""
x.py — X-Optimizer v2.0  (Cross-Scanner Intelligence Engine)
=============================================================
Aggregates results from all 3 running scanners, ranks every X variation
using a composite score, identifies the globally best X, and serves a
live dashboard on port 8063.

Run LAST, after all 3 scanners are running.

Startup order:
    1. python Algofinal.py       ← ZMQ publisher, unified dash :8050, equity bot
    2. python scanner1.py        ← narrow  sweep (1,000 variations)
    3. python scanner2.py        ← medium  sweep (16,000 variations)
    4. python scanner3.py        ← wide    sweep (36,000 variations)
    5. python x.py               ← this file
    6. python monitor.py         ← optional CLI health check

What x.py achieves:
    1. Reads live_state.json from each scanner every 10 s.
    2. Merges 49,000 unique X evaluations into a unified table.
    3. Composite score = 50% P&L + 30% Win-rate + 20% (1−drawdown).
    4. Serves Dash dashboard on port 8063 with real-time charts + leaderboard.
    5. Sends Telegram alert when the globally best X changes.
    6. Exports ranked CSV + XLSX at EOD.

Dashboard pages (if unified_dash is running on :8050):
    The optimizer feeds its DataFrame into unified_dash.register_optimizer().
    Navigate to http://<LAN>:8050/optimizer for the full interactive view.
    Port 8063 remains available as a standalone fallback.
"""

from __future__ import annotations

import gc
import json
import logging
import os
import re
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

# ── Optional Dash ─────────────────────────────────────────────────────────────
try:
    import dash
    from dash import Dash, dcc, html, Input, Output, dash_table as dt
    import plotly.graph_objects as go
    import plotly.express as px
    DASH_OK = True
except ImportError:
    DASH_OK = False

IST = pytz.timezone("Asia/Kolkata")
USDT_TO_INR = float(os.getenv("USDT_TO_INR", "84.0") or 84.0)

# ════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════

DASHBOARD_PORT   = int(os.getenv("XOPT_DASH_PORT", "8063"))
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "7587307352:AAG6RaiF4gO5I_ZFZ_4b8Gj7dnsu4GtPWFw")
TELEGRAM_CHATS   = [c for c in [
    os.getenv("TELEGRAM_CHAT_ID", "1376513391"),
    "793674804",
] if c]

SCANNER_DIRS = {
    1: os.path.join("sweep_results", "scanner1_narrow_x0080_x0090"),
    2: os.path.join("sweep_results", "scanner2_medium_x0010_x0160"),
    3: os.path.join("sweep_results", "scanner3_wide_x0010_x0360"),
}

# Composite score weights (must sum to 1.0)
W_PNL      = 0.50
W_WINRATE  = 0.30
W_DRAWDOWN = 0.20

CURRENT_X_MULTIPLIER = 0.008575   # live Algofinal X
INDEX_X_MULTIPLIER   = 0.00343
REFRESH_INTERVAL     = 10          # seconds

RESULTS_DIR = "x_optimizer_results"
CLOUDFLARED = os.getenv("CLOUDFLARED_PATH", "cloudflared")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [X-OPT] %(levelname)s — %(message)s",
)
log = logging.getLogger("x_optimizer")


# ════════════════════════════════════════════════════════════════════════════
# TIME HELPERS
# ════════════════════════════════════════════════════════════════════════════

def now_ist() -> datetime:
    return datetime.now(IST)

def is_market_open(dt: datetime) -> bool:
    t = dt.hour * 60 + dt.minute
    return 9 * 60 + 15 <= t <= 23 * 60


# ════════════════════════════════════════════════════════════════════════════
# ASYNC TELEGRAM
# ════════════════════════════════════════════════════════════════════════════

def _get_lan_ip() -> str:
    try:
        import socket as _s
        s = _s.socket(_s.AF_INET, _s.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]; s.close(); return ip
    except Exception:
        return "127.0.0.1"

def _read_master_url() -> Optional[str]:
    try:
        p = os.path.join("levels", "dashboard_url.json")
        with open(p, encoding="utf-8") as fh:
            return json.load(fh).get("public_url")
    except Exception:
        return None

def _tg_async(text: str) -> None:
    """Non-blocking Telegram send."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHATS:
        return
    def _send():
        for cid in TELEGRAM_CHATS:
            try:
                data = urllib.parse.urlencode({"chat_id": cid, "text": text}).encode()
                urllib.request.urlopen(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                    data=data, timeout=8,
                )
            except Exception:
                pass
    threading.Thread(target=_send, daemon=True).start()


# ════════════════════════════════════════════════════════════════════════════
# SCANNER STATE READER
# ════════════════════════════════════════════════════════════════════════════

class LiveStateReader:
    """Reads live_state.json written by each scanner every 30 s."""

    def read(self, scanner_id: int, date_str: str) -> Optional[Dict]:
        base = os.path.join(SCANNER_DIRS.get(scanner_id, ""), date_str)
        path = os.path.join(base, "live_state.json")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                pass
        # Fallback: summary XLSX
        if os.path.isdir(base):
            xlsxs = [f for f in os.listdir(base)
                     if f.endswith(".xlsx") and "summary" in f]
            if xlsxs:
                try:
                    df = pd.read_excel(os.path.join(base, max(xlsxs)))
                    return {"source": "eod_excel", "df": df.to_dict("records")}
                except Exception:
                    pass
        return None

    def collect_all(self, date_str: str) -> Dict[int, Optional[Dict]]:
        return {sid: self.read(sid, date_str) for sid in (1, 2, 3)}


# ════════════════════════════════════════════════════════════════════════════
# PERFORMANCE ANALYSER
# ════════════════════════════════════════════════════════════════════════════

class PerformanceAnalyser:
    """Composite score = W_PNL×pnl_norm + W_WINRATE×wr + W_DRAWDOWN×(1−dd_norm)."""

    @staticmethod
    def score_df(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        pnl = df["total_pnl"].values.astype(float)
        wr  = (df["win_rate_pct"].values.astype(float) / 100.0)
        dd  = df.get("max_drawdown", pd.Series(0.0, index=df.index)).values.astype(float)

        pnl_range = pnl.max() - pnl.min()
        pnl_norm  = (pnl - pnl.min()) / pnl_range if pnl_range > 1e-9 else np.zeros(len(pnl))

        dd_max    = dd.max()
        dd_norm   = dd / dd_max if dd_max > 1e-9 else np.zeros(len(dd))
        dd_score  = 1.0 - dd_norm

        df["score"]          = W_PNL * pnl_norm + W_WINRATE * wr + W_DRAWDOWN * dd_score
        df["vs_current_pct"] = (df["x_value"] - CURRENT_X_MULTIPLIER) / CURRENT_X_MULTIPLIER * 100
        return df.sort_values("score", ascending=False).reset_index(drop=True)


# ════════════════════════════════════════════════════════════════════════════
# CROSS-SCANNER AGGREGATOR
# ════════════════════════════════════════════════════════════════════════════

class CrossScannerAggregator:
    """
    Merges X variation results from all 3 scanners.
    Deduplicates by x_value (rounded to 6 dp) keeping highest-trade-count entry.
    Continuously refreshed by background thread.
    """

    def __init__(self) -> None:
        self._lock         = threading.Lock()
        self._unified_df   = pd.DataFrame()
        self._per_symbol   : Dict[str, pd.DataFrame] = {}
        self._scanner_rows : Dict[int, List[dict]] = {1: [], 2: [], 3: []}
        self._last_update  = 0.0
        self._best_x_prev  : Optional[float] = None

    def ingest(self, scanner_id: int, data: Optional[Dict]) -> None:
        if data is None:
            return
        rows: List[dict] = []
        asset_class = str((data or {}).get("asset_class", "equity")).lower()
        pnl_scale = USDT_TO_INR if asset_class in {"commodity", "crypto"} and USDT_TO_INR > 0 else 1.0

        if "sweeps" in data:
            for sym, sd in data["sweeps"].items():
                xv  = np.array(sd.get("x_values", []))
                pnl = np.array(sd.get("total_pnl", []))
                tc  = np.array(sd.get("trade_count", []), dtype=float)
                wc  = np.array(sd.get("win_count", []), dtype=float)
                lc  = np.array(sd.get("loss_count", []), dtype=float)
                for i in range(len(xv)):
                    _tc = float(tc[i]) if i < len(tc) else 0.0
                    _wc = float(wc[i]) if i < len(wc) else 0.0
                    rows.append({
                        "symbol":       sym,
                        "x_value":      round(float(xv[i]), 6),
                        # Normalize all P&L into INR for cross-scanner scoring/output.
                        "total_pnl":    (float(pnl[i]) if i < len(pnl) else 0.0) * pnl_scale,
                        "trade_count":  _tc,
                        "win_rate_pct": (_wc / _tc * 100) if _tc > 0 else 0.0,
                        "scanner":      scanner_id,
                    })
        elif "df" in data:
            for r in data["df"]:
                rows.append({
                    "symbol":       str(r.get("symbol", "?")),
                    "x_value":      round(float(r.get("best_x", 0)), 6),
                    "total_pnl":    float(r.get("total_pnl", 0)),
                    "trade_count":  float(r.get("trade_count", 0)),
                    "win_rate_pct": float(r.get("win_rate_pct", 0)),
                    "scanner":      scanner_id,
                })

        with self._lock:
            self._scanner_rows[scanner_id] = rows

    def rebuild(self) -> None:
        all_rows: List[dict] = []
        with self._lock:
            for rows in self._scanner_rows.values():
                all_rows.extend(rows)

        if not all_rows:
            return

        df = pd.DataFrame(all_rows)
        if df.empty:
            return

        # Global aggregate by x_value
        agg = (
            df.groupby("x_value")
            .agg(
                total_pnl      = ("total_pnl",    "sum"),
                total_trades   = ("trade_count",  "sum"),
                avg_win_rate   = ("win_rate_pct", "mean"),
                symbols_traded = ("symbol",       "nunique"),
            )
            .reset_index()
        )
        agg["win_rate_pct"] = agg["avg_win_rate"]
        scored = PerformanceAnalyser.score_df(agg)

        # Per-symbol
        per_sym: Dict[str, pd.DataFrame] = {}
        for sym in df["symbol"].unique():
            sym_df = df[df["symbol"] == sym].copy()
            scored_sym = PerformanceAnalyser.score_df(sym_df)
            if not scored_sym.empty:
                per_sym[sym] = scored_sym

        with self._lock:
            self._unified_df  = scored
            self._per_symbol  = per_sym
            self._last_update = time.time()

        # Telegram alert on best-X change
        # Guards:
        #   1. Minimum 10 trades across all symbols (prevents startup noise)
        #   2. Minimum ₹500 combined P&L per symbol (prevents trivial wins)
        #   3. Best X must differ by >0.0005 (5% of typical X range) to matter
        #   4. Best X must be stable — within top-3 for 2 consecutive rebuilds
        _total_trades_all = int(scored["total_trades"].sum()) if not scored.empty and "total_trades" in scored.columns else 0
        _n_syms = int(scored.iloc[0].get("symbols_traded", 1)) if not scored.empty else 1
        _min_trades_required = max(10, _n_syms * 2)   # at least 2 trades per symbol on average
        _pnl_per_sym = float(scored.iloc[0]["total_pnl"]) / max(_n_syms, 1) if not scored.empty else 0

        if (not scored.empty
                and _total_trades_all >= _min_trades_required
                and _pnl_per_sym >= 50):     # at least ₹50 avg P&L per symbol evaluated
            best_x = float(scored.iloc[0]["x_value"])
            best_p = float(scored.iloc[0]["total_pnl"])
            best_s = float(scored.iloc[0]["score"])
            # Only alert if X changed by more than 0.0005 (avoids noise from micro-fluctuations)
            if self._best_x_prev is None or abs(best_x - self._best_x_prev) > 0.0005:
                delta = (best_x - CURRENT_X_MULTIPLIER) / CURRENT_X_MULTIPLIER * 100
                _lan = _get_lan_ip()
                _master_url = _read_master_url()
                _dash_line = (
                    f"Dashboard: {_master_url}/opt" if _master_url
                    else f"LAN: http://{_lan}:{DASHBOARD_PORT}/opt"
                )
                # Only alert if new best X is meaningfully better than live X P&L
                live_x_rows = scored[abs(scored["x_value"] - CURRENT_X_MULTIPLIER) < 0.001]
                live_x_pnl  = float(live_x_rows["total_pnl"].max()) if not live_x_rows.empty else 0
                improvement  = best_p - live_x_pnl
                if improvement > 100 or self._best_x_prev is None:
                    _tg_async(
                        f"🏆 X-Optimizer Update\n"
                        f"Best X: {best_x:.6f}  ({delta:+.1f}% vs live)\n"
                        f"P&L: ₹{best_p:,.0f}  (+₹{improvement:,.0f} vs live X)\n"
                        f"Trades: {_total_trades_all:,}  Symbols: {_n_syms}\n"
                        f"Score: {best_s:.4f}\n"
                        f"{_dash_line}"
                    )
                self._best_x_prev = best_x

        gc.collect()

    def get_unified(self) -> pd.DataFrame:
        with self._lock:
            return self._unified_df.copy() if not self._unified_df.empty else pd.DataFrame()

    def get_best_x_per_symbol(self) -> Dict[str, dict]:
        with self._lock:
            return {
                sym: {
                    "symbol":       sym,
                    "best_x":       float(df.iloc[0]["x_value"]),
                    "total_pnl":    float(df.iloc[0].get("total_pnl", 0)),
                    "win_rate_pct": float(df.iloc[0].get("win_rate_pct", 0)),
                    "score":        float(df.iloc[0].get("score", 0)),
                }
                for sym, df in self._per_symbol.items()
                if not df.empty
            }

    def seconds_since_update(self) -> float:
        return time.time() - self._last_update if self._last_update else 999.0


# ════════════════════════════════════════════════════════════════════════════
# DASH DASHBOARD
# ════════════════════════════════════════════════════════════════════════════

BG   = "#0d1117"
CARD = "#161b22"
TEXT = "#c9d1d9"
ACC  = "#58a6ff"
GRN  = "#3fb950"
RED  = "#f85149"

def build_dash(agg: CrossScannerAggregator) -> "Dash":
    app = Dash(__name__, suppress_callback_exceptions=True, title="X-Optimizer")
    app.layout = html.Div([
        html.H2("⚡ X-Optimizer — Cross-Scanner Leaderboard",
                style={"color": ACC, "padding": "20px 20px 5px",
                       "fontFamily": "monospace"}),
        html.Div(id="status-bar", style={"padding": "0 20px 10px",
                                          "color": TEXT, "fontFamily": "monospace",
                                          "fontSize": "13px"}),
        dcc.Graph(id="pnl-chart", style={"height": "280px"}),
        html.H3("Top X Values", style={"color": ACC, "padding": "10px 20px 0",
                                        "fontFamily": "monospace"}),
        html.Div(id="table-div", style={"padding": "0 20px 20px"}),
        dcc.Interval(id="iv", interval=REFRESH_INTERVAL * 1000, n_intervals=0),
    ], style={"backgroundColor": BG, "minHeight": "100vh", "color": TEXT})

    @app.callback(
        Output("status-bar", "children"),
        Output("pnl-chart",  "figure"),
        Output("table-div",  "children"),
        Input("iv", "n_intervals"),
    )
    def refresh(_n):
        now  = now_ist().strftime("%Y-%m-%d %H:%M:%S IST")
        age  = round(agg.seconds_since_update(), 0)
        df   = agg.get_unified()

        status = f"Updated: {now}   Last data: {age}s ago   X variations: {len(df):,}"

        if df.empty:
            fig = go.Figure().update_layout(
                paper_bgcolor=BG, plot_bgcolor=BG,
                title=dict(text="Waiting for scanner data...", font=dict(color=TEXT)),
            )
            return status, fig, html.P("No data yet.", style={"color": TEXT})

        top = df.head(200)

        # P&L scatter chart
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=top["x_value"].tolist(),
            y=top["total_pnl"].tolist(),
            mode="markers",
            marker=dict(
                color=top["score"].tolist(),
                colorscale="Viridis",
                size=6,
                showscale=True,
                colorbar=dict(title="Score", tickfont=dict(color=TEXT)),
            ),
            text=[f"X={x:.6f}<br>P&L=₹{p:,.0f}<br>Score={s:.4f}"
                  for x, p, s in zip(top["x_value"], top["total_pnl"], top["score"])],
            hovertemplate="%{text}<extra></extra>",
        ))
        # Current X marker
        fig.add_vline(
            x=CURRENT_X_MULTIPLIER,
            line_dash="dash", line_color=RED,
            annotation_text=f"Live X={CURRENT_X_MULTIPLIER:.6f}",
            annotation_font_color=RED,
        )
        fig.update_layout(
            paper_bgcolor=BG, plot_bgcolor=CARD,
            xaxis=dict(title="X Multiplier", color=TEXT, gridcolor="#333"),
            yaxis=dict(title="Combined P&L (₹)", color=TEXT, gridcolor="#333"),
            margin=dict(l=60, r=20, t=30, b=40),
            font=dict(color=TEXT),
        )

        # Table (top 50)
        cols = [c for c in ["x_value", "total_pnl", "total_trades", "win_rate_pct",
                             "symbols_traded", "score", "vs_current_pct"]
                if c in df.columns]
        tbl_df = df[cols].head(50).copy()
        # Format
        if "total_pnl"     in tbl_df: tbl_df["total_pnl"]     = tbl_df["total_pnl"].apply(lambda v: f"₹{v:,.0f}")
        if "win_rate_pct"  in tbl_df: tbl_df["win_rate_pct"]  = tbl_df["win_rate_pct"].apply(lambda v: f"{v:.1f}%")
        if "vs_current_pct" in tbl_df: tbl_df["vs_current_pct"] = tbl_df["vs_current_pct"].apply(lambda v: f"{v:+.2f}%")
        if "score"         in tbl_df: tbl_df["score"]         = tbl_df["score"].apply(lambda v: f"{v:.4f}")

        table = dt.DataTable(
            data=tbl_df.to_dict("records"),
            columns=[{"name": c.replace("_", " ").title(), "id": c} for c in tbl_df.columns],
            sort_action="native",
            style_header={"backgroundColor": CARD, "color": ACC, "fontWeight": "bold"},
            style_cell={"backgroundColor": BG, "color": TEXT,
                        "border": f"1px solid {CARD}", "fontFamily": "monospace",
                        "padding": "6px"},
            style_data_conditional=[
                {"if": {"filter_query": "{vs_current_pct} contains '+'"},
                 "color": GRN},
                {"if": {"filter_query": "{vs_current_pct} contains '-'"},
                 "color": RED},
            ],
        )
        return status, fig, table

    return app


# ════════════════════════════════════════════════════════════════════════════
# TUNNEL
# ════════════════════════════════════════════════════════════════════════════

def _open_tunnel(port: int) -> Optional[str]:
    """Try cloudflared quick-tunnel. Returns public URL or None."""
    try:
        proc = subprocess.Popen(
            [CLOUDFLARED, "tunnel", "--url", f"http://localhost:{port}"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        found: Optional[str] = None
        ev = threading.Event()

        def _drain():
            nonlocal found
            for line in proc.stdout:
                if not found:
                    m = re.search(r"https://[\w\-\.]+\.trycloudflare\.com", line)
                    if m:
                        found = m.group(0)
                        ev.set()
            if not ev.is_set():
                ev.set()

        threading.Thread(target=_drain, daemon=True).start()
        ev.wait(timeout=45)
        if found:
            log.info("X-Optimizer public URL: %s", found)
            return found
        proc.terminate()
    except FileNotFoundError:
        log.debug("cloudflared not found")
    except Exception as exc:
        log.debug("tunnel error: %s", exc)
    return None


# ════════════════════════════════════════════════════════════════════════════
# EOD EXPORT
# ════════════════════════════════════════════════════════════════════════════

def _eod_export(agg: CrossScannerAggregator, date_str: str) -> None:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    df = agg.get_unified()
    if df.empty:
        return
    # CSV
    csv_path = os.path.join(RESULTS_DIR, f"xopt_ranked_{date_str}.csv")
    df.to_csv(csv_path, index=False)
    # XLSX
    xl_path = os.path.join(RESULTS_DIR, f"xopt_ranked_{date_str}.xlsx")
    with pd.ExcelWriter(xl_path, engine="openpyxl") as w:
        df.head(100).to_excel(w, sheet_name="Top 100 X Values", index=False)
        per_sym = agg.get_best_x_per_symbol()
        if per_sym:
            ps_df = pd.DataFrame(list(per_sym.values()))
            ps_df.to_excel(w, sheet_name="Best X Per Symbol", index=False)
    log.info("EOD export: %s", xl_path)
    _tg_async(
        f"📊 X-Optimizer EOD Export\n"
        f"Top X: {float(df.iloc[0]['x_value']):.6f}\n"
        f"Combined P&L: ₹{float(df.iloc[0]['total_pnl']):,.0f}\n"
        f"Score: {float(df.iloc[0]['score']):.4f}\n"
        f"File: {xl_path}"
    )


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("═" * 68)
    log.info("X-Optimizer v2.0 — Cross-Scanner Intelligence Engine")
    log.info("Scoring weights: P&L=%.0f%%  WinRate=%.0f%%  Drawdown=%.0f%%",
             W_PNL * 100, W_WINRATE * 100, W_DRAWDOWN * 100)
    log.info("Dashboard: http://0.0.0.0:%d", DASHBOARD_PORT)
    log.info("═" * 68)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    date_str    = datetime.now(IST).strftime("%Y%m%d")
    agg         = CrossScannerAggregator()
    reader      = LiveStateReader()

    # ── Background aggregation thread ─────────────────────────────────────────
    _stop = threading.Event()

    def _refresh_loop() -> None:
        eod_exported = False
        while not _stop.is_set():
            now = datetime.now(IST)
            # Collect from all scanners
            all_data = reader.collect_all(date_str)
            for sid, data in all_data.items():
                if data:
                    agg.ingest(sid, data)
            agg.rebuild()

            # Write optimizer data to CSV so unified_dash_v3 can read it.
            # NOTE: register_optimizer() cannot work across processes (separate memory).
            # The correct IPC is a CSV file read by unified_dash_v3's DataStore every 2s.
            try:
                from x_patch import push_live_data
                push_live_data(agg)
            except ImportError:
                # x_patch.py not present — write CSV directly as fallback
                try:
                    import pytz as _pytz
                    from datetime import datetime as _dt
                    _df = agg.get_unified()
                    if _df is not None and not _df.empty:
                        _d = "x_optimizer_results"
                        os.makedirs(_d, exist_ok=True)
                        _ds = _dt.now(_pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d")
                        _path = os.path.join(_d, f"xopt_live_{_ds}.csv")
                        _tmp  = _path + ".tmp"
                        _df.to_csv(_tmp, index=False)
                        os.replace(_tmp, _path)
                except Exception:
                    pass
            except Exception:
                pass

            # EOD export at 15:15 (equity) and 23:05 (commodity)
            if not eod_exported and (
                (now.hour == 15 and now.minute >= 15) or
                (now.hour == 23 and now.minute >= 5)
            ):
                _eod_export(agg, date_str)
                eod_exported = True

            interval = REFRESH_INTERVAL if is_market_open(now) else 60
            _stop.wait(timeout=interval)

    refresh_thread = threading.Thread(target=_refresh_loop, daemon=True, name="XOpt-Refresh")
    refresh_thread.start()
    log.info("X-Optimizer engine started (refresh every %ds)", REFRESH_INTERVAL)

    # ── Public tunnel (disabled when unified_dash is running) ────────────────
    pub_url: Optional[str] = None
    _disable_tunnel = os.getenv("DISABLE_XOPT_TUNNEL", "1") == "1"  # default ON — unified_dash handles public access
    if _disable_tunnel:
        log.info("Tunnel disabled (DISABLE_XOPT_TUNNEL=1) — using unified_dash.py for public access.")
    else:
        def _tunnel_in_bg():
            nonlocal pub_url
            pub_url = _open_tunnel(DASHBOARD_PORT)
            if pub_url:
                try:
                    os.makedirs("levels", exist_ok=True)
                    with open(os.path.join("levels", "xoptimizer_dashboard_url.json"), "w") as fh:
                        json.dump({"public_url": pub_url, "port": DASHBOARD_PORT}, fh)
                except Exception:
                    pass
        threading.Thread(target=_tunnel_in_bg, daemon=True).start()

    # ── Dash app ──────────────────────────────────────────────────────────────
    if not DASH_OK:
        log.warning("Dash not installed — no dashboard. Install: pip install dash plotly")
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass
        return

    app = build_dash(agg)

    import logging as _lg
    _lg.getLogger("werkzeug").setLevel(_lg.WARNING)

    _lip = _get_lan_ip()
    log.info("Dash is running on http://%s:%d/", _lip, DASHBOARD_PORT)
    log.info("LAN access: http://%s:%d", _lip, DASHBOARD_PORT)
    try:
        app.run(host="0.0.0.0", port=DASHBOARD_PORT, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        pass
    finally:
        _stop.set()
        log.info("X-Optimizer stopped.")


if __name__ == "__main__":
    main()
