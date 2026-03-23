#!/usr/bin/env python3
# Author: Ridhaant Ajoy Thackur
# AlgoStack v10.2 — health_check.py
# Quick diagnostic: checks all systems are working before market open
"""Run: python health_check.py"""
from __future__ import annotations
import json, os, sys, time
from datetime import datetime

try: import pytz; IST = pytz.timezone("Asia/Kolkata")
except ImportError: IST = None

def _col(c, t): return f"\033[{c}m{t}\033[0m"
OK  = lambda t: _col("32", f"  ✓  {t}")
WRN = lambda t: _col("33", f"  ⚠  {t}")
ERR = lambda t: _col("31", f"  ✗  {t}")
HDR = lambda t: _col("36", f"\n{'─'*50}\n  {t}\n{'─'*50}")

issues: list = []

def chk(label, fn):
    try:
        ok, msg = fn()
        print(OK(f"{label}: {msg}") if ok else WRN(f"{label}: {msg}"))
        if not ok: issues.append(f"{label}: {msg}")
    except Exception as e:
        print(ERR(f"{label}: {e}")); issues.append(f"{label}: {e}")

print(_col("1;36", "\n  AlgoStack v10.2 Health Check\n  Author: Ridhaant Ajoy Thackur\n"))

# ── Python Version ──────────────────────────────────────────────────────────
print(HDR("Python & Dependencies"))
chk("Python", lambda: (sys.version_info >= (3,10), sys.version.split()[0]))
for pkg, min_ver in [("numpy","1.26"),("pandas","2.1"),("dash","2.17"),("zmq","26"),
                      ("yfinance","0.2"),("pytz","2024"),("openpyxl","3.1"),
                      ("websocket","1.7"),("pyngrok","7.0")]:
    def _ck(p=pkg, mv=min_ver):
        try:
            m = __import__(p.replace("-","_"))
            v = getattr(m,"__version__",getattr(m,"version","?"))
            return True, f"{v} (min {mv})"
        except ImportError: return False, "NOT INSTALLED"
    chk(pkg, _ck)

# GPU acceleration
print(HDR("GPU / Acceleration"))
def _cupy():
    try:
        import cupy as cp; cp.cuda.Device(0).use()
        t=cp.zeros(100); t+=1; assert float(t.sum())==100
        props=cp.cuda.runtime.getDeviceProperties(0)
        nm = props["name"].decode() if isinstance(props["name"],bytes) else str(props["name"])
        return True, f"CuPy OK — {nm}"
    except: return False, "CuPy not available (run: pip install cupy-cuda12x)"
chk("CuPy/GPU", _cupy)

def _numba():
    try: import numba; return True, f"Numba {numba.__version__} (JIT fallback ready)"
    except: return False, "Numba not installed (run: pip install numba)"
chk("Numba JIT", _numba)

# ── Config ──────────────────────────────────────────────────────────────────
print(HDR("Configuration"))
chk(".env file", lambda: (os.path.exists(".env"), ".env found" if os.path.exists(".env") else ".env MISSING — copy from .env.template"))
def _cfg():
    try:
        from config import cfg
        return True, f"X={cfg.CURRENT_X_MULTIPLIER} capital=₹{cfg.CAPITAL_PER_TRADE:,.0f} crypto={'ON' if cfg.ENABLE_CRYPTO else 'OFF'}"
    except Exception as e: return False, str(e)
chk("config.py", _cfg)

def _gemini():
    try:
        from config import cfg
        key = cfg.GEMINI_API_KEY
        return (bool(key), f"Key set ({'*'*8+key[-4:] if key else 'MISSING'})")
    except: return False, "cannot read"
chk("Gemini API Key", _gemini)

# ── Price Data ──────────────────────────────────────────────────────────────
print(HDR("Price Data (live_prices.json)"))
def _prices():
    p = os.path.join("levels","live_prices.json")
    if not os.path.exists(p): return False, "File not found (start autohealer first)"
    age = round(time.time()-os.path.getmtime(p), 0)
    try:
        d = json.load(open(p))
        eq = len(d.get("prices",d.get("equity_prices",{})))
        cm = len(d.get("commodity_prices",{}))
        cr = len(d.get("crypto_prices",{}))
        return age<60, f"Age={age:.0f}s  equity={eq}  commodity={cm}  crypto={cr}"
    except: return False, f"Parse error (age={age}s)"
chk("live_prices.json", _prices)

def _crypto_levels():
    p = os.path.join("levels","crypto_initial_levels_latest.json")
    if not os.path.exists(p): return False, "Not found (CryptoEngine not started)"
    try:
        d = json.load(open(p))
        syms = list(d.get("levels",{}).keys())
        return bool(syms), f"Symbols: {', '.join(syms)}" if syms else "Empty"
    except: return False, "Parse error"
chk("Crypto levels", _crypto_levels)

# ── ZMQ ─────────────────────────────────────────────────────────────────────
print(HDR("ZMQ Price Bus"))
def _zmq():
    try:
        import zmq; ctx=zmq.Context.instance()
        s=ctx.socket(zmq.SUB); s.setsockopt(zmq.RCVTIMEO,500)
        s.setsockopt(zmq.SUBSCRIBE,b""); s.connect("tcp://127.0.0.1:28081")
        try:
            s.recv_multipart()
            return True, "ZMQ PUB active — receiving prices"
        except zmq.Again:
            return False, "ZMQ port 28081 open but no messages (Algofinal not publishing yet)"
        finally: s.close()
    except Exception as e: return False, str(e)
chk("ZMQ :28081", _zmq)

# ── Dashboard ───────────────────────────────────────────────────────────────
print(HDR("Dashboard"))
import urllib.request
def _dash(port, name):
    def _f():
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}", timeout=1)
            return True, f"::{port} is UP"
        except: return False, f"::{port} not responding (not started yet)"
    return name, _f
for name, fn in [_dash(8055,"UnifiedDash"),_dash(8050,"Algofinal"),_dash(8063,"XOptimizer")]:
    chk(name, fn)

# ── Summary ─────────────────────────────────────────────────────────────────
print(f"\n{'═'*50}")
if issues:
    print(_col("33", f"\n  ⚠  {len(issues)} issue(s) found:\n"))
    for i in issues: print(f"     • {i}")
    print()
else:
    print(_col("32", "\n  ✓  All checks passed — AlgoStack ready!\n"))
print("  Run: python autohealer.py\n")
