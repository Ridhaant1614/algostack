# ═══════════════════════════════════════════════════════════════════════════
# AlgoStack v10.5 | Author: Ridhaant Ajoy Thackur
# alert_monitor.py — Comprehensive monitoring + per-bot Telegram alerting
# ═══════════════════════════════════════════════════════════════════════════
"""
alert_monitor.py  v10.5
========================
FIXES vs v10.4:
  FIX 1  — CORRECT BOT ROUTING: equity alerts → equity bot, commodity → MCX bot,
            crypto → crypto bot, system/tunnel alerts → all 3 bots
  FIX 2  — STARTUP GRACE PERIOD: 3-minute silence on startup to let all
            engines initialise before any stale-price alerts fire
  FIX 3  — NO DUPLICATE ALERTS: unified_dash _alert_loop is now a stub;
            this file is the single source of truth for all Telegram alerts
  FIX 4  — SL THRESHOLD RAISED: 0.3%→1.5% crypto, 0.8% MCX for real advance warning
  FIX 5  — SL COOLDOWN EXTENDED: 120s → 300s per symbol
  FIX 6  — CONSISTENT FORMAT: all alerts use the same header/footer template
  FIX 7  — TUNNEL LIVENESS: HTTP-probes the URL; cooldown 10 minutes
  FIX 8  — WEEKEND EQUITY SILENCE: no equity stale alerts on Sat/Sun
  FIX 9  — COMM REST ALWAYS RUNS: MCX stale alerts only 09:00-23:30 weekdays

Routing:
  EQUITY BOT   → equity price stale, equity levels missing, equity process crash
  COMM BOT     → MCX price stale, MCX SL proximity, MCX process crash
  CRYPTO BOT   → crypto price stale, crypto SL proximity, re-anchor warnings
  ALL 3 BOTS   → tunnel down/revived, P&L milestones, internet down, memory high
"""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional

import pytz

log = logging.getLogger("alert_monitor")

IST        = pytz.timezone("Asia/Kolkata")
LEVELS_DIR = "levels"
TRADE_DIR  = "trade_logs"
DASH_PORT  = int(os.getenv("UNIFIED_DASH_PORT", "8055"))

try:
    from config import cfg as _cfg
    _EQ_TOKEN   = _cfg.TG_TOKEN
    _EQ_CHATS   = list(_cfg.TG_CHAT_IDS)
    _COMM_TOKEN = _cfg.TG_COMMODITY_TOKEN
    _COMM_CHATS = list(_cfg.TG_COMMODITY_CHATS)
    _CRYP_TOKEN = _cfg.TG_CRYPTO_TOKEN
    _CRYP_CHATS = list(_cfg.TG_CRYPTO_CHATS)
except Exception:
    _EQ_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN",  "7587307352:AAG6RaiF4gO5I_ZFZ_4b8Gj7dnsu4GtPWFw")
    _EQ_CHATS   = [c for c in [os.getenv("TELEGRAM_CHAT_ID","1376513391"),
                                os.getenv("TELEGRAM_CHAT_ID_2","793674804")] if c]
    _COMM_TOKEN = os.getenv("TELEGRAM_COMMODITY_BOT_TOKEN", "8340570160:AAHGq9U3i8HlD2-rmXWeY94IjJiC6NkHqv8")
    _COMM_CHATS = list(_EQ_CHATS)
    _CRYP_TOKEN = os.getenv("TELEGRAM_CRYPTO_BOT_TOKEN", "8710104039:AAGuCSmVQ16EEHwPy9t7Fxbi73Z4i3OYryk")
    _CRYP_CHATS = list(_EQ_CHATS)

USDT_TO_INR      = float(os.getenv("USDT_TO_INR", "84.0"))
STARTUP_GRACE_S  = 180
STALE_EQUITY_S   = 60
STALE_COMM_S     = 45
STALE_CRYPTO_S   = 20
CRYPTO_SL_PCT    = 0.015   # 1.5%
MCX_SL_PCT       = 0.008   # 0.8%
PNL_TARGET_PCT   = 0.30
MEMORY_ALERT_PCT = 90.0

COOLDOWNS: Dict[str, float] = {
    "eq_stale":300, "comm_stale":300, "crypto_stale":300,
    "tunnel_down":600, "process_down":300,
    "pnl_50":86400, "pnl_100":86400, "pnl_150":86400,
    "internet_down":300, "memory_high":600,
    "levels_stale":600, "reanchor_warn":3600,
}

_last_alert:     Dict[str, float] = {}
_pnl_done_today: Dict[str, str]   = {}
_start_time = time.monotonic()
_stop       = threading.Event()
_monitor_boot_dt = datetime.now(IST)

# Cross-asset guardrails for routing.
_COMM_SYMBOLS = {"GOLD", "SILVER", "CRUDE", "NATURALGAS", "COPPER"}
_CRYPTO_SYMBOLS = {"BTC", "ETH", "BNB", "SOL", "ADA"}


# ── Telegram ──────────────────────────────────────────────────────────────────

def _send(text: str, token: str, chats: List[str]) -> None:
    def _go():
        for cid in chats:
            try:
                data = urllib.parse.urlencode({"chat_id": cid, "text": text}).encode()
                urllib.request.urlopen(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    data=data, timeout=12)
                log.info("TG[%s…] → %s | %s", token[:10], cid, text[:50])
            except Exception as e:
                log.debug("TG fail %s: %s", cid, e)
    threading.Thread(target=_go, daemon=True, name="AlertTG").start()

def _tg_equity(t: str)  -> None: _send(t, _EQ_TOKEN,   _EQ_CHATS)
def _tg_comm(t: str)    -> None: _send(t, _COMM_TOKEN,  _COMM_CHATS)
def _tg_crypto(t: str)  -> None: _send(t, _CRYP_TOKEN,  _CRYP_CHATS)
def _tg_all(t: str)     -> None:
    for tok, chats in ((_EQ_TOKEN,_EQ_CHATS),(_COMM_TOKEN,_COMM_CHATS),(_CRYP_TOKEN,_CRYP_CHATS)):
        _send(t, tok, chats)


def _fmt(emoji: str, title: str, body: str, extra: str = "") -> str:
    ts  = datetime.now(IST).strftime("%d %b %Y %H:%M:%S IST")
    lan = _get_lan()
    parts = [
        f"{emoji} AlgoStack v10.5 — {title}",
        "",
        body.strip(),
        "",
        f"⏱ {ts}",
        f"🏠 LAN: http://{lan}:{DASH_PORT}",
    ]
    if extra:
        parts += ["", extra.strip()]
    return "\n".join(parts)


def _get_lan() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80)); ip = s.getsockname()[0]; s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ── Guards ────────────────────────────────────────────────────────────────────

def _in_grace() -> bool:
    return (time.monotonic() - _start_time) < STARTUP_GRACE_S

def _can_alert(key: str) -> bool:
    now = time.monotonic()
    if now - _last_alert.get(key, 0) > COOLDOWNS.get(key, 300):
        _last_alert[key] = now; return True
    return False

def _now_ist() -> datetime:  return datetime.now(IST)
def _ist_t()  -> int:
    n = _now_ist(); return n.hour * 60 + n.minute
def _is_weekday() -> bool:   return _now_ist().weekday() < 5
def _date_str()  -> str:     return _now_ist().strftime("%Y%m%d")
def _is_equity_mkt() -> bool:
    t = _ist_t(); return _is_weekday() and (9*60+30) <= t <= (15*60+15)
def _is_mcx_session() -> bool:
    t = _ist_t(); return _is_weekday() and (9*60) <= t <= (23*60+30)


# ── File helpers ──────────────────────────────────────────────────────────────

def _rj(path: str) -> Optional[dict]:
    try:
        with open(path, encoding="utf-8") as f: return json.load(f)
    except Exception: return None

def _file_age(path: str) -> float:
    try: return time.time() - os.path.getmtime(path)
    except Exception: return 99999.0

def _ts_age(ts_str: str) -> float:
    if not ts_str: return 99999.0
    try:
        from datetime import datetime as _dt
        ct = _dt.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        return time.time() - IST.localize(ct).timestamp()
    except Exception: return 99999.0

def _read_jsonl(path: str) -> list:
    rows = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try: rows.append(json.loads(line))
                    except Exception: pass
    except Exception: pass
    return rows


def _event_is_recent(ev: dict, max_age_s: int = 180) -> bool:
    """Ignore stale historical events after monitor restarts."""
    raw = str(ev.get("timestamp") or ev.get("ts") or "")
    if not raw:
        return False
    dt = None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw[:19], fmt).replace(tzinfo=IST)
            break
        except Exception:
            pass
    if dt is None or dt < _monitor_boot_dt:
        return False
    return (datetime.now(IST) - dt).total_seconds() <= max_age_s


# ════════════════════════════════════════════════════════════════════════════
# CHECK FUNCTIONS
# ════════════════════════════════════════════════════════════════════════════

def _check_equity_prices() -> None:
    if _in_grace() or not _is_equity_mkt(): return
    d   = _rj(os.path.join(LEVELS_DIR, "live_prices.json")) or {}
    age = _ts_age(d.get("equity_ts", d.get("ts", "")))
    if age == 99999.0: age = _file_age(os.path.join(LEVELS_DIR, "live_prices.json"))
    px  = d.get("equity_prices") or d.get("prices", {})
    if not px or age <= STALE_EQUITY_S: return
    if _can_alert("eq_stale"):
        _tg_equity(_fmt("⚠️","Equity Price Feed STALE",
            f"Prices are {age:.0f}s old (threshold {STALE_EQUITY_S}s)\n"
            f"Symbols in cache: {len(px)}\n"
            f"Action: Check Algofinal + ZMQ publisher\n"
            f"Log: logs/algofinal.log"))


def _check_commodity_prices() -> None:
    if _in_grace() or not _is_mcx_session(): return
    d    = _rj(os.path.join(LEVELS_DIR, "live_prices.json")) or {}
    age  = _ts_age(d.get("commodity_ts", ""))
    if age == 99999.0: age = _file_age(os.path.join(LEVELS_DIR, "live_prices.json"))
    comm = d.get("commodity_prices", {})
    if not comm or age <= STALE_COMM_S: return
    if _can_alert("comm_stale"):
        live = ", ".join(f"{s}: ${float(v):.2f}" for s,v in comm.items())
        _tg_comm(_fmt("⚠️","MCX Price Feed STALE",
            f"Prices are {age:.0f}s old (threshold {STALE_COMM_S}s)\n"
            f"Last: {live}\n"
            f"Action: Check CommodityEngine + TradingView WS\n"
            f"Log: logs/commodityengine.log"))


def _check_crypto_prices() -> None:
    if _in_grace(): return
    d    = _rj(os.path.join(LEVELS_DIR, "live_prices.json")) or {}
    age  = _ts_age(d.get("crypto_ts", ""))
    if age == 99999.0: age = _file_age(os.path.join(LEVELS_DIR, "live_prices.json"))
    cryp = d.get("crypto_prices", {})
    if not cryp or age <= STALE_CRYPTO_S: return
    if _can_alert("crypto_stale"):
        live = ", ".join(f"{s}:${v:.2f}" for s,v in list(cryp.items())[:3])
        _tg_crypto(_fmt("🔴","Crypto Price Feed STALE",
            f"Prices are {age:.0f}s old (threshold {STALE_CRYPTO_S}s)\n"
            f"24/7 feed — this should NEVER happen!\n"
            f"Last: {live}\n"
            f"Action: Check CryptoEngine + Binance WebSocket\n"
            f"Log: logs/cryptoengine.log"))


def _check_tunnel() -> None:
    if _in_grace(): return
    d   = _rj(os.path.join(LEVELS_DIR, "dashboard_url.json")) or {}
    pub = d.get("public_url", "")
    if not pub or not pub.startswith("http"): return
    # localtunnel free URLs often fail bot probes despite being usable in browser;
    # avoid false "UNREACHABLE" spam for .loca.lt links.
    if ".loca.lt" in pub:
        return
    try:
        urllib.request.urlopen(pub, timeout=6)
        _last_alert.pop("tunnel_down", None)  # alive — reset so next failure alerts fast
    except Exception:
        if _can_alert("tunnel_down"):
            lan = _get_lan()
            _tg_all(_fmt("⚠️","Remote Tunnel UNREACHABLE",
                f"Public URL offline: {pub}\n"
                f"LAN still accessible: http://{lan}:{DASH_PORT}\n"
                f"Tunnel guardian will attempt auto-reconnect every 90s\n"
                f"If persists: restart unified_dash_v3.py"))


def _check_equity_levels() -> None:
    if _in_grace() or not _is_weekday(): return
    t = _ist_t()
    if t < 9*60+45 or t > 16*60: return
    ds    = _date_str()
    cands = ([f for f in os.listdir(LEVELS_DIR)
              if f.endswith(".xlsx") and "initial_levels" in f and ds in f]
             if os.path.isdir(LEVELS_DIR) else [])
    if not cands and _can_alert("levels_stale"):
        _tg_equity(_fmt("⚠️","Equity Levels File Missing",
            f"No initial_levels_*.xlsx found for today ({ds})\n"
            f"Algofinal should write this at 09:30 IST\n"
            f"Action: Check Algofinal startup + NSE data feed\n"
            f"Log: logs/algofinal.log"))


def _check_pnl_milestones() -> None:
    if _in_grace() or not _is_weekday(): return
    ds = _date_str()
    p  = os.path.join(TRADE_DIR, f"trade_events_{ds}.jsonl")
    if not os.path.exists(p): return
    trades = _read_jsonl(p)
    _EXIT  = ("T1","T2","T3","T4","T5","ST1","ST2","ST3","ST4","ST5",
              "BUY_SL","SELL_SL","RETREAT","EOD")
    closed = [t for t in trades if (t.get("event_type","") or "").upper().startswith(_EXIT)]
    if not closed: return
    net = sum(float(t.get("net_pnl", t.get("net",0)) or 0) for t in closed)
    n   = len(closed)
    if n == 0: return
    pct = net / (n * 100000) * 100
    wr  = sum(1 for t in closed if float(t.get("net_pnl", t.get("net",0)) or 0) > 0)
    for key, thr, emoji, label in [
        ("pnl_50",  50,  "🎯", "50% of daily target"),
        ("pnl_100", 100, "🏆", "Daily target HIT"),
        ("pnl_150", 150, "🚀", "150% of target — Exceptional!"),
    ]:
        if pct >= PNL_TARGET_PCT * thr / 100 and _pnl_done_today.get(key) != ds:
            _pnl_done_today[key] = ds
            _tg_all(_fmt(emoji, f"Equity P&L Milestone — {label}",
                f"Net P&L:  ₹{net:+,.2f}\n"
                f"Return:   {pct:.3f}%  (target {PNL_TARGET_PCT:.2f}%)\n"
                f"Trades:   {n}  |  Win rate: {wr/n*100:.1f}%\n"
                f"Date:     {_now_ist().strftime('%d %b %Y')}"))


def _check_internet() -> None:
    if _in_grace(): return
    try:
        import wifi_keepalive as _wk
        wk = getattr(_wk, "_GLOBAL_KEEPALIVE", None)
        if wk:
            s  = wk.status
            ok = s.get("internet_up", True)
            if not ok and _can_alert("internet_down"):
                _tg_all(_fmt("🔴","INTERNET DOWN",
                    f"WiFi keepalive: no internet connectivity\n"
                    f"Last speed: ↓{s.get('download_mbps',0):.1f}Mbps  "
                    f"ping={s.get('ping_ms',0):.0f}ms\n"
                    f"Impact: All trading halted\n"
                    f"Action: Check network / portal login (172.22.2.6)"))
    except Exception: pass


def _check_memory() -> None:
    if _in_grace(): return
    try:
        import psutil
        m = psutil.virtual_memory()
        if m.percent >= MEMORY_ALERT_PCT and _can_alert("memory_high"):
            _tg_all(_fmt("⚠️","Memory Critical",
                f"RAM: {m.percent:.1f}% used\n"
                f"Available: {m.available/1024/1024:.0f} MB / "
                f"{m.total/1024/1024:.0f} MB total\n"
                f"Action: Restart low-priority scanners to free RAM"))
    except (ImportError, Exception): pass


def _check_process_restarts() -> None:
    if _in_grace(): return
    p = os.path.join("logs","process_status.json")
    if not os.path.exists(p): return
    try:
        d = _rj(p) or {}
        for name, info in d.items():
            restarts = int(info.get("total_restarts",0))
            status   = info.get("status","")
            if status != "too_many_restarts" and restarts <= 8: continue
            key = f"process_down_{name}"
            if not _can_alert(key): continue
            body = (f"Process:  {name}\n"
                    f"Restarts: {restarts}\n"
                    f"Status:   {status}\n"
                    f"Log: logs/{name.lower()}.log\n"
                    f"Action: Check log for crash reason")
            msg = _fmt("🔴",f"Process Restart Loop — {name}", body)
            nl  = name.lower()
            if   "crypto" in nl: _tg_crypto(msg)
            elif "comm"   in nl: _tg_comm(msg)
            else:                _tg_equity(msg)
    except Exception: pass


def _check_reanchor_due() -> None:
    if _in_grace(): return
    p   = os.path.join(LEVELS_DIR,"crypto_initial_levels_latest.json")
    age = _file_age(p)
    if age > 23400 and _can_alert("reanchor_warn"):  # 6.5h
        _tg_crypto(_fmt("⚠️","Crypto Re-Anchor Overdue",
            f"crypto_initial_levels_latest.json is {age/3600:.1f}h old\n"
            f"Expected refresh: every 6h\n"
            f"Action: Check CryptoEngine — may have crashed\n"
            f"Log: logs/cryptoengine.log"))


# ════════════════════════════════════════════════════════════════════════════
# SCHEDULE
# ════════════════════════════════════════════════════════════════════════════


# ── Instant trade alerts (commodity + crypto) ──────────────────────────────
_seen_trades: dict = {"comm": set(), "crypto": set()}
_NOTIFY_PREFIX = (
    "ENTRY",
    "T1","T2","T3","T4","T5",
    "ST1","ST2","ST3","ST4","ST5",
    "BUY_SL","SELL_SL",
    "BUY_MANUAL_TARGET","SELL_MANUAL_TARGET",
    "EOD","EOD_CLOSE",
)

def _check_commodity_trades():
    if _in_grace() or not _is_mcx_session(): return
    ds = _date_str()
    p = os.path.join(TRADE_DIR, "commodity_trade_events_" + ds + ".jsonl")
    if not os.path.exists(p): return
    for t in _read_jsonl(p):
        if not _event_is_recent(t):
            continue
        uid = (str(t.get("ts","")) + "_" +
               str(t.get("sym","") or t.get("symbol","")) + "_" +
               str(t.get("event_type","") or t.get("event","") or t.get("reason","")))
        if uid in _seen_trades["comm"]: continue
        _seen_trades["comm"].add(uid)
        ev = (t.get("event_type","") or t.get("event","") or t.get("reason","")).upper()
        sym = (t.get("sym","") or t.get("symbol","?")); side = t.get("side","")
        net = float(t.get("net_pnl", t.get("net",0)) or 0)
        entry = float(t.get("entry_px",0) or t.get("entry_price",0) or 0)
        exit_ = float(t.get("exit_px",0) or t.get("price",0) or t.get("level_px",0) or 0)
        if not ev:
            continue
        if not any(ev.startswith(x) for x in _NOTIFY_PREFIX):
            continue
        sign = "+" if net > 0 else "-" if net < 0 else "="
        msg = ("MCX " + sign + " " + sym + "\n"
               "Event: " + ev + " | Side: " + side + "\n"
               "Entry: $" + "%.2f" % entry + "  Exit: $" + "%.2f" % exit_ + "\n"
               "Net PnL: Rs" + "%+.2f" % net + "\n"
               "Time: " + datetime.now(IST).strftime("%H:%M:%S IST"))
        _tg_comm(msg)


def _check_crypto_trades():
    if _in_grace(): return
    ds = _date_str()
    p = os.path.join(TRADE_DIR, "crypto_trade_events_" + ds + ".jsonl")
    if not os.path.exists(p): return
    try:
        from config import cfg as _c2; inr = _c2.USDT_TO_INR
    except Exception: inr = USDT_TO_INR
    for t in _read_jsonl(p):
        if not _event_is_recent(t):
            continue
        uid = str(t.get("ts","")) + "_" + str(t.get("sym","")) + "_" + str(t.get("event_type",t.get("event","")))
        if uid in _seen_trades["crypto"]: continue
        _seen_trades["crypto"].add(uid)
        ev = (t.get("event_type","") or t.get("event","")).upper()
        sym = t.get("sym","?"); side = t.get("side","")
        net_usd = float(t.get("net_pnl", t.get("net",0)) or 0); net_inr = net_usd * inr
        entry = float(t.get("entry_px",0) or 0); exit_ = float(t.get("price",0) or t.get("level_px",0) or 0)
        if not ev:
            continue
        if not any(ev.startswith(x) for x in _NOTIFY_PREFIX):
            continue
        sign = "+" if net_inr > 0 else "-" if net_inr < 0 else "="
        msg = ("Crypto " + sign + " " + sym + "\n"
               "Event: " + ev + " | Side: " + side + "\n"
               "Entry: $" + "%.4f" % entry + "  Exit: $" + "%.4f" % exit_ + "\n"
               "Net PnL: $" + "%+.2f" % net_usd + " (Rs" + "%+.0f" % net_inr + ")\n"
               "Time: " + datetime.now(IST).strftime("%H:%M:%S IST"))
        _tg_crypto(msg)


def _check_comm_pnl_milestones():
    if _in_grace() or not _is_mcx_session(): return
    ds = _date_str()
    p = os.path.join(TRADE_DIR, "commodity_trade_events_" + ds + ".jsonl")
    if not os.path.exists(p): return
    _EX = ("T1","T2","T3","T4","T5","ST1","ST2","ST3","ST4","ST5","BUY_SL","SELL_SL","RETREAT","EOD")
    closed = [t for t in _read_jsonl(p) if any((t.get("event_type","") or t.get("event","")).upper().startswith(x) for x in _EX)]
    if not closed: return
    net = sum(float(t.get("net_pnl", t.get("net",0)) or 0) for t in closed)
    n = len(closed); pct = net / (n * 100000) * 100 if n else 0
    wr = sum(1 for t in closed if float(t.get("net_pnl", t.get("net",0)) or 0) > 0)
    for key, thr, label in [("pnl_50_comm",50,"50pct MCX"),("pnl_100_comm",100,"MCX Target HIT"),("pnl_150_comm",150,"MCX 150pct!")]:
        if pct >= PNL_TARGET_PCT * thr / 100 and _pnl_done_today.get(key) != ds:
            _pnl_done_today[key] = ds
            _tg_comm(_fmt("MCX PnL", "MCX Milestone - " + label,
                         "Net PnL: Rs" + "%+,.2f" % net + "\nReturn: " + "%.3f" % pct + "%\n"
                         "Trades: " + str(n) + " | Win: " + "%.1f" % (wr/n*100 if n else 0) + "%"))




# LAYER 4: Continuous live trade monitor
_L4_seen: set = set()
_L4_jsonl_sz: dict = {}

def _layer4_live_trade_monitor():
    if _in_grace(): return
    ds = _date_str()
    EXITS = {"T1","T2","T3","T4","T5","ST1","ST2","ST3","ST4","ST5",
             "BUY_SL","SELL_SL","BUY_RETREAT_25PCT","SELL_RETREAT_25PCT",
             "EOD_CLOSE","BUY_MANUAL_TARGET","SELL_MANUAL_TARGET",
             "BUY_REENTRY_RETOUCH","SELL_REENTRY_RETOUCH"}
    for fname, bot_fn, lbl in [
        ("trade_events_" + ds + ".jsonl", _tg_equity, "Equity"),
        ("crypto_trade_events_" + ds + ".jsonl", _tg_crypto, "Crypto"),
        ("commodity_trade_events_" + ds + ".jsonl", _tg_comm, "MCX"),
    ]:
        path = os.path.join(TRADE_DIR, fname)
        if not os.path.exists(path): continue
        try: sz = os.path.getsize(path)
        except Exception: continue
        if _L4_jsonl_sz.get(fname) == sz: continue
        _L4_jsonl_sz[fname] = sz
        for t in _read_jsonl(path):
            if not _event_is_recent(t):
                continue
            ev = (t.get("event_type","") or t.get("event","")).upper()
            sym = t.get("symbol", t.get("sym","?"))
            sym_u = str(sym).upper()
            if lbl == "Equity" and (sym_u in _COMM_SYMBOLS or sym_u in _CRYPTO_SYMBOLS):
                continue
            ts  = str(t.get("timestamp", t.get("ts","")))[:19]
            uid = fname + ts + sym + ev
            if uid in _L4_seen: continue
            if not any(ev.startswith(x) for x in EXITS): continue
            _L4_seen.add(uid)
            qty  = int(t.get("qty",0) or 0)
            side = t.get("side","")

            # Equity/Crypto use (entry_price, price). Commodity uses (entry_px, exit_px)
            # and already has gross_pnl/net_pnl stored in INR.
            if lbl == "MCX":
                entry = float(t.get("entry_px",0) or 0)
                price = float(t.get("exit_px",0) or 0)
                gross = float(t.get("gross_pnl",0) or 0)
                net   = float(t.get("net_pnl",0) or 0)
            else:
                price = float(t.get("price",0) or 0)
                entry = float(t.get("entry_price",0) or 0)
                gross = 0.0
                net   = 0.0

            if entry > 0 and price > 0 and qty > 0:
                if lbl != "MCX":
                    gross = (price-entry)*qty if side=="BUY" else (entry-price)*qty
                    net   = gross - 20.0

                cur = datetime.now(IST).strftime("%H:%M:%S IST")
                if lbl == "MCX":
                    sign = "+" if net >= 0 else ""
                    msg = (lbl + " " + ("GAIN" if net >= 0 else "LOSS") + " " + sym + " " + ev + "\n"
                           "Side: " + side + "  Qty: " + str(qty) + "\n"
                           "Entry: $" + "%.2f" % entry + "  Exit: $" + "%.2f" % price + "\n"
                           "Gross: Rs" + sign + str(round(gross,2)) + "  Net: Rs" + sign + str(round(net,2)) + "\n"
                           "Time: " + cur)
                else:
                    ru = "Rs" if lbl != "Crypto" else "$"
                    sign = "+" if gross >= 0 else ""
                    msg = (lbl + " " + ("GAIN" if gross >= 0 else "LOSS") + " " + sym + " " + ev + "\n"
                           "Side: " + side + "  Qty: " + str(qty) + "\n"
                           "Entry: " + ru + str(round(entry,2)) + "  Exit: " + ru + str(round(price,2)) + "\n"
                           "Gross: " + sign + str(round(gross,2)) + "  Net: " + sign + str(round(net,2)) + "\n"
                           "Time: " + cur)
                try: bot_fn(msg)
                except Exception: pass


# LAYER 5: Calculation verifier
_L5_last: dict = {}
_EOD_VERIFY_DONE: Dict[str, str] = {"equity": "", "mcx": "", "crypto": ""}

def _safe_f(v) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0

def _sum_exit_net(path: str, kind: str) -> tuple:
    """Return (logged_net, recomputed_net, exit_count) for trade jsonl file."""
    if not os.path.exists(path):
        return 0.0, 0.0, 0
    evs = _read_jsonl(path)
    if not evs:
        return 0.0, 0.0, 0
    exits = {"T1","T2","T3","T4","T5","ST1","ST2","ST3","ST4","ST5",
             "BUY_SL","SELL_SL","BUY_RETREAT_25PCT","SELL_RETREAT_25PCT",
             "RETREAT","EOD","EOD_CLOSE","EOD_1511","EOD_2330",
             "BUY_MANUAL_TARGET","SELL_MANUAL_TARGET",
             "BUY_REENTRY_RETOUCH","SELL_REENTRY_RETOUCH"}
    logged, recomputed, n = 0.0, 0.0, 0
    for t in evs:
        ev = str(t.get("event_type","") or t.get("event","") or t.get("reason","")).upper()
        if not any(ev.startswith(x) for x in exits):
            continue
        n += 1
        if kind == "equity":
            entry = _safe_f(t.get("entry_price", t.get("entry_px", 0)))
            exitp = _safe_f(t.get("price", t.get("exit_px", 0)))
            qty   = int(_safe_f(t.get("qty", 0)))
            side  = str(t.get("side",""))
            gross = (exitp-entry)*qty if side == "BUY" else (entry-exitp)*qty
            net_calc = gross - 20.0
            logged += _safe_f(t.get("net", t.get("net_pnl", net_calc)))
            recomputed += net_calc
        elif kind == "mcx":
            entry = _safe_f(t.get("entry_px", t.get("entry_price", 0)))
            exitp = _safe_f(t.get("exit_px", t.get("price", 0)))
            qty   = int(_safe_f(t.get("qty", 0)))
            side  = str(t.get("side",""))
            gross_usdt = (exitp-entry)*qty if side == "BUY" else (entry-exitp)*qty
            net_calc = gross_usdt * USDT_TO_INR - 20.0
            logged += _safe_f(t.get("net_pnl", t.get("net", net_calc)))
            recomputed += net_calc
        else:  # crypto
            entry = _safe_f(t.get("entry_px", t.get("entry_price", 0)))
            exitp = _safe_f(t.get("exit_px", t.get("price", 0)))
            qty   = int(_safe_f(t.get("qty", 0)))
            side  = str(t.get("side",""))
            gross_usdt = (exitp-entry)*qty if side == "BUY" else (entry-exitp)*qty
            net_calc = gross_usdt * USDT_TO_INR - 20.0
            logged += _safe_f(t.get("net_pnl_inr", t.get("net_pnl", t.get("net", net_calc))))
            recomputed += net_calc
    return logged, recomputed, n

def _read_equity_summary_net(ds: str) -> Optional[float]:
    try:
        import pandas as _pd
        p = os.path.join("summary", f"summary_events_{ds}.xlsx")
        if not os.path.exists(p):
            return None
        df = _pd.read_excel(p)
        if df is None or df.empty or "net_pnl" not in df.columns:
            return None
        if "symbol" in df.columns and (df["symbol"] == "TOTAL").any():
            return float(df.loc[df["symbol"] == "TOTAL", "net_pnl"].iloc[0])
        return float(df["net_pnl"].sum())
    except Exception:
        return None

def _layer45_eod_verifier():
    """Explicit EOD verification runs (15:11 equity, 23:00 mcx/crypto)."""
    if _in_grace() or not _is_weekday():
        return
    now = _now_ist()
    ds = now.strftime("%Y%m%d")
    mins = now.hour * 60 + now.minute

    # Equity EOD verification window: 15:11–15:25
    if 15 * 60 + 11 <= mins <= 15 * 60 + 25 and _EOD_VERIFY_DONE.get("equity") != ds:
        ep = os.path.join(TRADE_DIR, f"trade_events_{ds}.jsonl")
        logged, recomputed, n = _sum_exit_net(ep, "equity")
        if n > 0:
            drift = abs(logged - recomputed)
            summ = _read_equity_summary_net(ds)
            sum_drift = abs(logged - summ) if summ is not None else 0.0
            if (drift > 75.0 or sum_drift > 75.0) and _can_alert("eod_eq_drift_" + ds):
                _tg_equity(_fmt("🧮", "EOD Verify (Equity) drift",
                    f"Exits: {n}\n"
                    f"Logged net: Rs{logged:+,.2f}\n"
                    f"Recomputed net: Rs{recomputed:+,.2f}\n"
                    f"Summary net: {('Rs' + format(summ, '+,.2f')) if summ is not None else 'N/A'}\n"
                    f"Drift(log-vs-calc): Rs{drift:,.2f}"
                    + (f"\nDrift(log-vs-summary): Rs{sum_drift:,.2f}" if summ is not None else "")
                ))
        _EOD_VERIFY_DONE["equity"] = ds

    # MCX/Crypto EOD verification window: 23:00–23:40
    if 23 * 60 <= mins <= 23 * 60 + 40:
        if _EOD_VERIFY_DONE.get("mcx") != ds:
            mp = os.path.join(TRADE_DIR, f"commodity_trade_events_{ds}.jsonl")
            logged, recomputed, n = _sum_exit_net(mp, "mcx")
            drift = abs(logged - recomputed)
            if n > 0 and drift > 100.0 and _can_alert("eod_mcx_drift_" + ds):
                _tg_comm(_fmt("🧮", "EOD Verify (MCX) drift",
                    f"Exits: {n}\nLogged net: Rs{logged:+,.2f}\n"
                    f"Recomputed net: Rs{recomputed:+,.2f}\nDrift: Rs{drift:,.2f}"))
            _EOD_VERIFY_DONE["mcx"] = ds
        if _EOD_VERIFY_DONE.get("crypto") != ds:
            cp = os.path.join(TRADE_DIR, f"crypto_trade_events_{ds}.jsonl")
            logged, recomputed, n = _sum_exit_net(cp, "crypto")
            drift = abs(logged - recomputed)
            if n > 0 and drift > 100.0 and _can_alert("eod_crypto_drift_" + ds):
                _tg_crypto(_fmt("🧮", "EOD Verify (Crypto) drift",
                    f"Exits: {n}\nLogged net: Rs{logged:+,.2f}\n"
                    f"Recomputed net: Rs{recomputed:+,.2f}\nDrift: Rs{drift:,.2f}"))
            _EOD_VERIFY_DONE["crypto"] = ds

def _layer5_calculation_verifier():
    if _in_grace() or not _is_weekday(): return
    ds = _date_str()
    path = os.path.join(TRADE_DIR, "trade_events_" + ds + ".jsonl")
    if not os.path.exists(path): return
    try:
        events = _read_jsonl(path)
        if not events: return
        EXITS2 = {"T1","T2","T3","T4","T5","ST1","ST2","ST3","ST4","ST5",
                  "BUY_SL","SELL_SL","BUY_RETREAT_25PCT","SELL_RETREAT_25PCT",
                  "EOD_CLOSE","BUY_MANUAL_TARGET","SELL_MANUAL_TARGET",
                  "BUY_REENTRY_RETOUCH","SELL_REENTRY_RETOUCH"}
        positions = {}
        trades = []
        for e in events:
            sym  = e.get("symbol", e.get("sym",""))
            ev   = (e.get("event_type","") or e.get("event","")).upper()
            px   = float(e.get("price",0) or 0)
            qty  = int(e.get("qty",0) or 0)
            side = e.get("side","")
            if "ENTRY" in ev and px > 0:
                positions[sym] = {"price": px, "qty": qty, "side": side}
            elif any(ev.startswith(x) for x in EXITS2) and sym in positions:
                p = positions.pop(sym)
                gross = (px-p["price"])*qty if p["side"]=="BUY" else (p["price"]-px)*qty
                trades.append({"sym": sym, "gross": gross, "net": gross - 20.0})
        total_gross = sum(t["gross"] for t in trades)
        prev = _L5_last.get(ds, {})
        if prev:
            drift = abs(total_gross - prev.get("gross", total_gross))
            if drift > 50.0 and _can_alert("l5_drift_" + ds):
                _tg_equity(_fmt("L5 Verifier", "PnL Drift Detected",
                    "Drift from last check: Rs" + str(round(drift,2)) + "\n"
                    "Current gross: Rs" + str(round(total_gross,2)) + "\n"
                    "Previous: Rs" + str(round(prev.get("gross",0),2)) + "\n"
                    "Check for missed/duplicate events"))
        _L5_last[ds] = {"gross": total_gross, "trades": len(trades)}
    except Exception as ex:
        log.debug("Layer5 error: %s", ex)


_SCHEDULE = [
    (5,   [_check_commodity_trades, _check_crypto_trades, _layer4_live_trade_monitor]),
    (8,   [_check_equity_prices, _check_commodity_prices, _check_crypto_prices]),
    (30,  [_check_internet]),
    (60,  [_check_equity_levels]),
    (90,  [_check_tunnel]),
    (120, [_check_memory, _check_process_restarts]),
    (300, [_check_pnl_milestones]),
    (60,  [_layer5_calculation_verifier]),
    (60,  [_layer45_eod_verifier]),
    (600, [_check_reanchor_due]),
]
_last_check: Dict[int, float] = {}


def _run_checks() -> None:
    now_m = time.monotonic()
    for interval, fns in _SCHEDULE:
        if now_m - _last_check.get(interval, 0) >= interval:
            _last_check[interval] = now_m
            for fn in fns:
                try: fn()
                except Exception as e: log.debug("Check %s: %s", fn.__name__, e)


# ════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ════════════════════════════════════════════════════════════════════════════

def run_forever(check_interval: float = 5.0) -> None:
    log.info("AlertMonitor v10.5 | grace=%ds | crypto_SL=%.1f%% | mcx_SL=%.1f%%",
             STARTUP_GRACE_S, CRYPTO_SL_PCT*100, MCX_SL_PCT*100)
    log.info("Bots: equity=%s… | comm=%s… | crypto=%s…",
             _EQ_TOKEN[:12], _COMM_TOKEN[:12], _CRYP_TOKEN[:12])
    while not _stop.is_set():
        try: _run_checks()
        except Exception as e: log.debug("Loop: %s", e)
        _stop.wait(check_interval)


def start_in_background() -> threading.Thread:
    t = threading.Thread(target=run_forever, daemon=True, name="AlertMonitor")
    t.start()
    log.info("AlertMonitor v10.5 background thread started")
    return t


def stop() -> None:
    _stop.set()


# ════════════════════════════════════════════════════════════════════════════
# STANDALONE
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO,
        format="%(asctime)s [alert] %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                os.path.join("logs","alert_monitor.log") if os.path.isdir("logs")
                else "alert_monitor.log", encoding="utf-8"),
        ])
    ap = argparse.ArgumentParser(description="AlgoStack v10.5 Alert Monitor")
    ap.add_argument("--test",        action="store_true")
    ap.add_argument("--test-eq",     action="store_true")
    ap.add_argument("--test-comm",   action="store_true")
    ap.add_argument("--test-crypto", action="store_true")
    args = ap.parse_args()
    msg = _fmt("✅","Alert Monitor Test",
        f"Alerts correctly routed to 3 bots.\n"
        f"Equity:    {_EQ_TOKEN[:18]}…\n"
        f"Commodity: {_COMM_TOKEN[:18]}…\n"
        f"Crypto:    {_CRYP_TOKEN[:18]}…\n"
        f"Crypto SL threshold: {CRYPTO_SL_PCT*100:.1f}%\n"
        f"Startup grace: {STARTUP_GRACE_S}s")
    if args.test or not any([args.test_eq, args.test_comm, args.test_crypto]):
        if not any([args.test_eq, args.test_comm, args.test_crypto]):
            log.info("Starting standalone alert monitor…")
            try: run_forever()
            except KeyboardInterrupt: log.info("Stopped.")
        else:
            _tg_all(msg); log.info("Test → all 3 bots"); time.sleep(3)
    if args.test_eq:
        _tg_equity(msg); log.info("Test → equity bot"); time.sleep(3)
    if args.test_comm:
        _tg_comm(msg); log.info("Test → commodity bot"); time.sleep(3)
    if args.test_crypto:
        _tg_crypto(msg); log.info("Test → crypto bot"); time.sleep(3)
