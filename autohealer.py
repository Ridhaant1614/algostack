"""
autohealer_v2.py — AlgoStack Process Watchdog + WiFi Auto-Login  v2.0
======================================================================
Monitors all AlgoStack processes. Auto-restarts crashed ones.
Continuously keeps college WiFi session alive.

Usage:
    python autohealer_v2.py           # start everything + monitor + WiFi
    python autohealer_v2.py --monitor # only watch (processes already running)
    python autohealer_v2.py --wifi    # only run WiFi keepalive (no process mgmt)
    python autohealer_v2.py --errors [NAME]  # show recent logs
    python autohealer_v2.py --login   # force a single portal login and exit
    python autohealer_v2.py --profile render-full  # hosted full stack mode

New in v2:
  ✅ WifiKeepalive integrated — checks internet every 90s
  ✅ Proactive re-login every 2h (session = 4h, buffer = 2h)
  ✅ Auto-login when internet drops
  ✅ Rich terminal shows WiFi status in live table
  ✅ EXIT_PFX fix for BUY_MANUAL_TARGET / SELL_MANUAL_TARGET
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime
from typing import Deque, Dict, List, Optional, Tuple

import pytz

# Import wifi module from same directory
try:
    from wifi_keepalive import WifiKeepalive, is_internet_up, portal_login
    WIFI_OK = True
except ImportError:
    WIFI_OK = False

log = logging.getLogger("autohealer")

# Suppress InsecureRequestWarning from urllib3 (portal login uses self-signed cert)
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass
import warnings as _w; _w.filterwarnings("ignore", message="Unverified HTTPS")
IST         = pytz.timezone("Asia/Kolkata")
LOGS_DIR    = "logs"
TG_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "7587307352:AAG6RaiF4gO5I_ZFZ_4b8Gj7dnsu4GtPWFw")
TG_CHATS    = [c for c in [os.getenv("TELEGRAM_CHAT_ID","1376513391"), "793674804"] if c]
MAX_RESTARTS_PER_HOUR = 5
RESTART_COOLDOWN_S    = 30
LOG_TAIL_LINES        = 120
DISABLE_AFFINITY      = os.getenv("DISABLE_AFFINITY", "0").strip() in ("1", "true", "True")


# ══════════════════════════════════════════════════════════════════════════════
#  PROCESS DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════


# Market calendar -- skip restarts on weekends/holidays
try:
    from market_calendar import MarketCalendar as _MC
    _MC_OK = True
except ImportError:
    _MC_OK = False
    _MC = None

def _is_trading_day() -> bool:
    """Return False on weekends and NSE holidays."""
    if not _MC_OK:
        return True  # fail open -- always trade if calendar unavailable
    import pytz
    from datetime import datetime
    return _MC.is_trading_day(datetime.now(pytz.timezone("Asia/Kolkata")))

PROCESSES: List[dict] = [
    {
        "name":        "Algofinal",
        "cmd":         [sys.executable, "Algofinal.py"],
        # Commodity handling is performed by commodity_engine.py (+ commodity scanners).
        # Running commodity logic inside Algofinal duplicates trade events and can misroute
        # MCX alerts into the equity bot channel.
        "env":         {"ENABLE_TUNNEL": "0", "ENABLE_COMMODITIES": "0", "ENABLE_COMMODITY_TUNNEL": "0"},
        "port":        8050,
        "start_delay": 0,
        "auto_restart": True,
        "critical":    True,
        "health_url":  "http://127.0.0.1:8050",
        "description": "Equity bot + ZMQ PUB",
    },
    {
        "name":        "UnifiedDash",
        "cmd":         [sys.executable, "-X", "utf8", "-u", "dash_launcher.py"],
        "env":         {
            "DISABLE_CLOUDFLARE": "1",
            "DISABLE_PYNGROK": "1",
            "TUNNEL_STABLE_MODE": "1",
            "DISABLE_PUBLIC_TUNNEL": "1",
            "PUBLIC_BASE_URL": "https://algostack.onrender.com",
        },
        "port":        8055,
        "start_delay": 12,
        "auto_restart": True,
        "critical":    True,
        "health_url":  "http://127.0.0.1:8055",
        "description": "Master dashboard :8055 + tunnel",
    },
    {
        "name":        "Scanner1",
        "cmd":         [sys.executable, "scanner1.py"],
        "env":         {},
        "port":        None,
        "start_delay": 8,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "Narrow X sweep",
    },
    {
        "name":        "Scanner2",
        "cmd":         [sys.executable, "scanner2.py"],
        "env":         {},
        "port":        None,
        "start_delay": 5,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "Medium X sweep",
    },
    {
        "name":        "Scanner3",
        "cmd":         [sys.executable, "scanner3.py"],
        "env":         {},
        "port":        None,
        "start_delay": 5,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "Wide X sweep",
    },
    {
        "name":        "XOptimizer",
        "cmd":         [sys.executable, "x.py"],
        "env":         {"DISABLE_XOPT_TUNNEL": "1"},
        "port":        8063,
        "start_delay": 5,
        "auto_restart": True,
        "critical":    False,
        "health_url":  "http://127.0.0.1:8063",
        "description": "Cross-scanner X optimizer",
    },
    {
        "name":        "BestXTrader",
        "cmd":         [sys.executable, "best_x_trader.py"],
        "env":         {},
        "port":        None,
        "start_delay": 10,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "Best-X paper trading engine",
    },
    # ── COMMODITY ENGINE + SCANNERS (v9.0) ─────────────────────────────────
    {
        "name":        "CommodityEngine",
        "cmd":         [sys.executable, "commodity_engine.py"],
        "env":         {},
        "port":        None,
        "start_delay": 5,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "MCX commodity engine (ZMQ pub 'commodity')",
        "weekday_only": True,   # MCX Mon–Fri only
    },
    {
        "name":        "CommScanner1",
        "cmd":         [sys.executable, "commodity_scanner1.py"],
        "env":         {"FORCE_JSON_IPC": "1"},
        "port":        None,
        "start_delay": 8,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "MCX narrow sweep (5K variations)",
        "weekday_only": True,
    },
    {
        "name":        "CommScanner2",
        "cmd":         [sys.executable, "commodity_scanner2.py"],
        "env":         {"FORCE_JSON_IPC": "1"},
        "port":        None,
        "start_delay": 5,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "MCX dual-band sweep (32.5K variations)",
        "weekday_only": True,
    },
    {
        "name":        "CommScanner3",
        "cmd":         [sys.executable, "commodity_scanner3.py"],
        "env":         {"FORCE_JSON_IPC": "1"},
        "port":        None,
        "start_delay": 5,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "MCX wide-dual sweep + cross-fusion (60K variations)",
        "weekday_only": True,
    },
    # ── CRYPTO ENGINE + SCANNERS (v9.0) ────────────────────────────────────
    # Crypto runs 24/7 including weekends — weekday_only=False
    {
        "name":        "CryptoEngine",
        "cmd":         [sys.executable, "crypto_engine.py"],
        "env":         {},
        "port":        None,
        "start_delay": 5,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "Binance WS crypto engine (ZMQ pub 'crypto')",
        "weekday_only": False,  # 24/7 — never stopped by market calendar
    },
    {
        "name":        "CryptoScanner1",
        "cmd":         [sys.executable, "crypto_scanner1.py"],
        "env":         {"FORCE_JSON_IPC": "1"},
        "port":        None,
        "start_delay": 8,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "Crypto narrow sweep (5K/6h)",
        "weekday_only": False,
    },
    {
        "name":        "CryptoScanner2",
        "cmd":         [sys.executable, "crypto_scanner2.py"],
        "env":         {"FORCE_JSON_IPC": "1"},
        "port":        None,
        "start_delay": 5,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "Crypto dual-band sweep (65K/6h)",
        "weekday_only": False,
    },
    {
        "name":        "CryptoScanner3",
        "cmd":         [sys.executable, "crypto_scanner3.py"],
        "env":         {"FORCE_JSON_IPC": "1"},
        "port":        None,
        "start_delay": 5,
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "Crypto wide-dual sweep + cross-fusion (175K/6h)",
        "weekday_only": False,
    },
    # v10.4: Alert Monitor — standalone watchdog process
    {
        "name":        "AlertMonitor",
        "cmd":         [sys.executable, "alert_monitor.py"],
        "env":         {},
        "port":        None,
        "start_delay": 15,   # start after main processes are up
        "auto_restart": True,
        "critical":    False,
        "health_url":  None,
        "description": "v10.4 comprehensive alert + health monitor",
        "weekday_only": False,  # 24/7
    },
]

# Render/low-resource mode: run only the minimum stack needed to keep
# live prices + optimizer/best-x panels updating in hosted environments.
LITE_PROCESS_NAMES = {
    "Algofinal",
    "UnifiedDash",
    "Scanner1",
    "XOptimizer",
    "BestXTrader",
    "CommodityEngine",
    "CommScanner1",
    "CryptoEngine",
    "CryptoScanner1",
    "AlertMonitor",
}


def _is_render_host() -> bool:
    return (
        os.getenv("RENDER", "").strip().lower() == "true"
        or bool(os.getenv("RENDER_EXTERNAL_HOSTNAME"))
    )


def _resolve_profile(cli_profile: Optional[str], lite_flag: bool) -> str:
    # Backward compatibility:
    # - --lite or AUTOHEALER_LITE=1 => render-lite
    if lite_flag or os.getenv("AUTOHEALER_LITE", "0").strip().lower() in ("1", "true"):
        return "render-lite"
    if cli_profile:
        return cli_profile
    env_profile = os.getenv("AUTOHEALER_PROFILE", "").strip().lower()
    if env_profile:
        return env_profile
    # Default: run complete stack unless explicit lite was requested.
    # On Render this becomes render-full automatically.
    return "render-full" if _is_render_host() else "full"


def _select_processes(profile: str = "full") -> List[dict]:
    profile = (profile or "full").strip().lower()
    if profile in ("full", "render-full"):
        return PROCESSES
    if profile in ("lite", "render-lite"):
        return [cfg for cfg in PROCESSES if cfg.get("name") in LITE_PROCESS_NAMES]
    # Fail-safe: unknown profile falls back to full.
    return PROCESSES


# ══════════════════════════════════════════════════════════════════════════════
#  MANAGED PROCESS
# ══════════════════════════════════════════════════════════════════════════════

class ManagedProcess:
    def __init__(self, cfg: dict) -> None:
        self.cfg         = cfg
        self.name        = cfg["name"]
        self.proc:       Optional[subprocess.Popen] = None
        self.started_at  = 0.0
        self.restart_times: Deque[float] = deque(maxlen=MAX_RESTARTS_PER_HOUR)
        self.total_restarts = 0
        self.status      = "stopped"
        self.last_exit_code: Optional[int] = None
        self.log_lines:  Deque[str] = deque(maxlen=LOG_TAIL_LINES)
        self._lock       = threading.Lock()

        os.makedirs(LOGS_DIR, exist_ok=True)
        log_path = os.path.join(LOGS_DIR, f"{self.name.lower()}.log")
        self._log_fh = open(log_path, "a", encoding="utf-8", buffering=1)
        self._log_fh.write(
            f"\n{'='*60}\n"
            f"Watchdog (re)start: {datetime.now(IST).isoformat()}\n"
            f"{'='*60}\n"
        )

    def start(self) -> None:
        env = {**os.environ, **self.cfg.get("env", {})}
        # v10.9: Force UTF-8 for all child processes (fixes Python 3.13 Windows crash)
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONUTF8'] = '1'
        try:
            self.proc = subprocess.Popen(
                self.cfg["cmd"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, env=env, cwd=os.getcwd(),
                encoding="utf-8", errors="replace",  # v10.9: safe on Win Python 3.13
            )
            self.started_at     = time.monotonic()
            self.status         = "starting"
            self.last_exit_code = None
            log.info("[%s] Started (PID %d)", self.name, self.proc.pid)
            # Pin child process to designated CPU core(s)
            self._pin_affinity()
            threading.Thread(target=self._drain, daemon=True,
                             name=f"{self.name}-drain").start()
        except Exception as e:
            log.error("[%s] Start failed: %s", self.name, e)
            self.status = "crashed"

    def _pin_affinity(self) -> None:
        """Set CPU affinity for the child process after launch."""
        if DISABLE_AFFINITY:
            return
        if self.proc is None:
            return
        try:
            from process_affinity import AFFINITY_MAP
            import ctypes, sys as _sys
            mask = AFFINITY_MAP.get(self.name)
            if mask is None:
                return
            pid = self.proc.pid
            if _sys.platform == "win32":
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x0200 | 0x0400, False, pid)
                if handle:
                    ok = kernel32.SetProcessAffinityMask(handle, mask)
                    kernel32.CloseHandle(handle)
                    if ok:
                        cores = [i for i in range(16) if mask & (1 << i)]
                        log.info("[%s] Pinned to CPU cores %s", self.name, cores)
            elif _sys.platform.startswith("linux"):
                cores = set(i for i in range(16) if mask & (1 << i))
                import os as _os
                _os.sched_setaffinity(pid, cores)
                log.info("[%s] Pinned to CPU cores %s", self.name, sorted(cores))
        except Exception as exc:
            log.debug("[%s] CPU affinity not set: %s", self.name, exc)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            try: self.proc.terminate(); self.proc.wait(timeout=5)
            except Exception:
                try: self.proc.kill()
                except Exception: pass
        self.status = "stopped"

    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def check_health(self) -> bool:
        url = self.cfg.get("health_url")
        if not url:
            return self.is_alive()
        try:
            urllib.request.urlopen(url, timeout=1.0)
            return True
        except Exception:
            return False

    def uptime_s(self) -> float:
        return (time.monotonic() - self.started_at) if self.started_at else 0.0

    def restarts_last_hour(self) -> int:
        now = time.monotonic()
        return sum(1 for t in self.restart_times if now - t < 3600)

    def get_last_lines(self, n: int = 3) -> List[str]:
        with self._lock:
            return list(self.log_lines)[-n:]

    def _drain(self) -> None:
        startup_done = False
        for line in iter(self.proc.stdout.readline, ""):
            line = line.rstrip()
            if not line:
                continue
            ts      = datetime.now(IST).strftime("%H:%M:%S")
            stamped = f"[{ts}] {line}"
            with self._lock:
                self.log_lines.append(stamped)
                if not startup_done and time.monotonic() - self.started_at > 8:
                    self.status = "running"
                    startup_done = True
            try:
                self._log_fh.write(stamped + "\n")
            except Exception:
                pass
        rc = self.proc.wait()
        with self._lock:
            self.last_exit_code = rc
            # For 24/7 processes (weekday_only=False): exit code=0 means startup failure
            # (e.g. waiting for prices) — treat as needing restart, not "stopped"
            _weekday_only = self.cfg.get("weekday_only", True)
            if rc != 0:
                self.status = "crashed"
            elif _weekday_only:
                self.status = "stopped"  # clean exit on non-trading day = expected
            else:
                self.status = "crashed"  # 24/7 process exiting cleanly = startup failure
            stamped = f"[{datetime.now(IST).strftime('%H:%M:%S')}] EXIT code={rc}"
            self.log_lines.append(stamped)
            try: self._log_fh.write(stamped + "\n")
            except Exception: pass
        log.warning("[%s] Exited code=%s", self.name, rc)


# ══════════════════════════════════════════════════════════════════════════════
#  WATCHDOG
# ══════════════════════════════════════════════════════════════════════════════

class Watchdog:
    def __init__(self, managed: List[ManagedProcess]) -> None:
        self.managed = managed
        self._stop   = threading.Event()

    def start_all(self) -> None:
        # v8.0: announce market status at startup
        is_td = _is_trading_day()
        if not is_td:
            import pytz as _pytz
            _now = datetime.now(_pytz.timezone("Asia/Kolkata"))
            log.warning("=" * 60)
            log.warning("  MARKET CLOSED — %s", _now.strftime("%A %d %b %Y"))
            log.warning("  Processes will start in dashboard-only mode.")
            log.warning("  Algofinal exits code=0 on weekends — this is NORMAL.")
            log.warning("=" * 60)
        log.info("Starting all processes in sequence…")
        for mp in self.managed:
            d = mp.cfg.get("start_delay", 0)
            if d > 0:
                log.info("[%s] Waiting %ds…", mp.name, d)
                time.sleep(d)
            mp.start()
            time.sleep(0.5)
        log.info("All processes launched.")

    def stop_all(self) -> None:
        self._stop.set()
        for mp in self.managed:
            mp.stop()

    def run(self) -> None:
        _status_ts = 0.0
        while not self._stop.is_set():
            for mp in self.managed:
                if not mp.cfg.get("auto_restart", True):
                    continue
                try:
                    self._check_one(mp)
                except Exception as e:
                    log.error("[%s] Watchdog check error: %s", mp.name, e)
            # v10.4: write process_status.json every 10s for AlertMonitor
            now_m = time.monotonic()
            if now_m - _status_ts > 10:
                _status_ts = now_m
                try:
                    status_data = {}
                    for mp in self.managed:
                        status_data[mp.name] = {
                            "status":          mp.status,
                            "pid":             mp.proc.pid if mp.proc else None,
                            "total_restarts":  mp.total_restarts,
                            "uptime_s":        round(mp.uptime_s(), 1),
                            "last_exit_code":  mp.last_exit_code,
                        }
                    _path = os.path.join(LOGS_DIR, "process_status.json")
                    _tmp  = _path + ".tmp"
                    with open(_tmp, "w", encoding="utf-8") as _f:
                        import json as _json
                        _json.dump(status_data, _f, separators=(",", ":"))
                    os.replace(_tmp, _path)
                except Exception:
                    pass
            self._stop.wait(3)

    def _check_one(self, mp: ManagedProcess) -> None:
        if mp.status == "stopped":
            return
        if mp.uptime_s() < 15:
            if mp.is_alive():
                mp.status = "starting"
            return

        if mp.is_alive():
            mp.status = "running"
            return

        # Process died
        if mp.status in ("running", "starting"):
            rc = mp.last_exit_code
            log.warning("[%s] Died (exit=%s)", mp.name, rc)

            # v8.0 Fix: Algofinal exits code=0 on weekends (market-closed normal exit)
            # This is NOT a crash — do not alert or restart on weekends
            is_trading = _is_trading_day()
            if mp.name == "Algofinal" and rc == 0 and not is_trading:
                if mp.status != "market_closed":
                    mp.status = "market_closed"
                    log.info("[Algofinal] Exited code=0 on non-trading day — Dashboard-only mode.")
                    log.info("[Algofinal] Will not restart until next trading day.")
                return

            if mp.cfg.get("critical") and rc != 0:
                _tg_async(f"⚠️ {mp.name} crashed (exit={rc}). Restarting…")

            # Write crash log for non-zero exits
            if rc is not None and rc != 0:
                try:
                    from log_manager import LogManager
                    crash_path = LogManager.write_crash_log(
                        mp.name, list(mp.log_lines),
                        exc=Exception(f"Process exited with code {rc}")
                    )
                    log.info("[%s] Crash log written: %s", mp.name, crash_path)
                except Exception as _le:
                    log.debug("log_manager unavailable: %s", _le)

        if mp.restarts_last_hour() >= MAX_RESTARTS_PER_HOUR:
            if mp.status != "too_many_restarts":
                log.error("[%s] Too many restarts (%d/hr)", mp.name, mp.restarts_last_hour())
                _tg_async(f"🔴 {mp.name} restart-looping. Check logs/{mp.name.lower()}.log")
                mp.status = "too_many_restarts"
            return

        if mp.restart_times and (time.monotonic() - mp.restart_times[-1]) < RESTART_COOLDOWN_S:
            return

        # For crypto scanners: wait 20s extra before first restart to let crypto_engine
        # write its anchor JSON file first
        _is_crypto_scanner = mp.name.startswith("CryptoScanner")
        if _is_crypto_scanner and mp.total_restarts == 0:
            _engine_up = any(m.name == "CryptoEngine" and m.is_alive()
                            for m in self.managed)
            if not _engine_up:
                log.info("[%s] Waiting for CryptoEngine to start first...", mp.name)
                return
            # Extra 20s cooldown on first restart so engine can write anchor files
            if (time.monotonic() - mp.started_at) < 20:
                return

        log.info("[%s] Restarting (#%d)...", mp.name, mp.total_restarts + 1)
        # Skip restarts on weekends/holidays for weekday-only processes
        weekday_only = mp.cfg.get("weekday_only", mp.name not in ("CryptoEngine", "CryptoScanner1", "CryptoScanner2", "CryptoScanner3"))
        if weekday_only and not _is_trading_day():
            if mp.status != "market_closed":
                mp.status = "market_closed"
                log.info("[%s] Not restarting -- market closed (weekend/holiday)", mp.name)
            return
        mp.restart_times.append(time.monotonic())
        mp.total_restarts += 1
        time.sleep(2)
        mp.start()
        if mp.cfg.get("critical"):
            _tg_async(f"[restart] {mp.name} restarted (#{mp.total_restarts})")


# ══════════════════════════════════════════════════════════════════════════════
#  TERMINAL DISPLAY
# ══════════════════════════════════════════════════════════════════════════════

def _live_display(
    managed: List[ManagedProcess],
    stop_evt: threading.Event,
    wifi: Optional[WifiKeepalive] = None,
) -> None:
    try:
        from rich.console import Console
        from rich.live    import Live
        from rich.table   import Table
        from rich.text    import Text
        from rich         import box
    except ImportError:
        log.warning("pip install rich for live display")
        _simple_display(managed, stop_evt, wifi)
        return

    console = Console()

    _STATUS = {
        "running":           ("green",  "● Running"),
        "starting":          ("yellow", "⏳ Starting"),
        "crashed":           ("red",    "✗ Crashed"),
        "stopped":           ("dim",    "○ Stopped"),
        "too_many_restarts": ("red",    "🔴 Restart loop"),
        "market_closed":     ("yellow", "🌙 Mkt Closed"),
        "weekend":           ("blue",   "📅 Weekend"),
    }

    def _mk_table() -> Table:
        now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
        t   = Table(
            title=f"AlgoStack v10.5 Watchdog — {now}",
            box=box.ROUNDED, border_style="dim",
            show_header=True, header_style="bold cyan",
        )
        t.add_column("Process",   style="bold white", width=14)
        t.add_column("Status",    width=17)
        t.add_column("PID",       width=7,  justify="right")
        t.add_column("Uptime",    width=9,  justify="right")
        t.add_column("Restarts",  width=8,  justify="right")
        t.add_column("Last log",  width=45)

        for mp in managed:
            st, lbl = _STATUS.get(mp.status, ("dim", mp.status))
            pid = str(mp.proc.pid) if mp.proc else "—"
            up  = _fmt_uptime(mp.uptime_s()) if mp.is_alive() else "—"
            ll  = (mp.get_last_lines(1) or ["—"])[-1][-50:]
            t.add_row(mp.name, Text(lbl, style=st), pid, up,
                      str(mp.total_restarts), Text(ll, style="dim"))

        # WiFi row
        if wifi:
            ws  = wifi.status
            net = ws["internet_up"]
            st, lbl = ("green","● Internet UP") if net else ("red","✗ Internet DOWN")
            nxt  = int(ws.get("next_login_in_s", 0))
            dl   = ws.get("download_mbps", 0)
            ul   = ws.get("upload_mbps",   0)
            ping = ws.get("ping_ms",        0)
            spd  = f"↓{dl:.1f}M ↑{ul:.1f}M {ping:.0f}ms" if dl else "speedtest…"
            ll   = ws.get("last_login_msg","")[-35:]
            t.add_row(
                "WiFi", Text(lbl, style=st), "—",
                "—", f"{ws['login_count']} logins",
                Text(f"{spd}  {ll}", style="dim"),
            )

        return t

    with Live(_mk_table(), console=console, refresh_per_second=2) as live:
        while not stop_evt.is_set():
            live.update(_mk_table())
            time.sleep(0.5)


def _simple_display(
    managed: List[ManagedProcess],
    stop_evt: threading.Event,
    wifi=None,
) -> None:
    while not stop_evt.is_set():
        os.system("cls" if os.name == "nt" else "clear")
        print(f"\nAlgoStack Watchdog — {datetime.now(IST).strftime('%H:%M:%S IST')}")
        print("=" * 60)
        for mp in managed:
            pid = str(mp.proc.pid) if mp.proc else "—"
            print(f"  {mp.name:<16} {mp.status:<18} PID:{pid}  restarts:{mp.total_restarts}")
        if wifi:
            ws = wifi.status
            print(f"  {'WiFi':<16} {'UP' if ws['internet_up'] else 'DOWN':<18} "
                  f"logins:{ws['login_count']}")
        print("\n[Ctrl+C to stop]\n")
        time.sleep(3)


def _fmt_uptime(s: float) -> str:
    if s < 60:    return f"{int(s)}s"
    if s < 3600:  return f"{int(s//60)}m{int(s%60):02d}s"
    return f"{int(s//3600)}h{int((s%3600)//60):02d}m"


# ══════════════════════════════════════════════════════════════════════════════
#  ERROR LOG VIEWER
# ══════════════════════════════════════════════════════════════════════════════

def view_errors(name: Optional[str] = None, last_n: int = 60) -> None:
    names = [name] if name else [cfg["name"] for cfg in PROCESSES] + ["wifi"]
    for n in names:
        path = os.path.join(LOGS_DIR, f"{n.lower()}.log")
        if not os.path.exists(path):
            print(f"\n[{n}] — no log yet at {path}")
            continue
        print(f"\n{'='*62}\n  {n} — last {last_n} lines\n{'='*62}")
        try:
            lines = open(path, encoding="utf-8").readlines()
            print("".join(lines[-last_n:]), end="")
        except Exception as e:
            print(f"Error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════

def _tg_async(text: str) -> None:
    def _go():
        for cid in TG_CHATS:
            try:
                data = urllib.parse.urlencode({"chat_id":cid,"text":text}).encode()
                urllib.request.urlopen(
                    f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                    data=data, timeout=10,
                )
            except Exception: pass
    threading.Thread(target=_go, daemon=True).start()


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [watchdog] %(levelname)s %(message)s",
    )
    # Per-module log files
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "watchdog.log") if os.path.isdir(LOGS_DIR)
                              else "watchdog.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(fh)

    p = argparse.ArgumentParser(description="AlgoStack watchdog + WiFi keepalive")
    p.add_argument("--monitor",  action="store_true", help="Watch only (don't start)")
    p.add_argument("--wifi",     action="store_true", help="WiFi keepalive only")
    p.add_argument("--login",    action="store_true", help="Force portal login and exit")
    p.add_argument("--lite",     action="store_true", help="Run reduced process set for low-resource hosts")
    p.add_argument("--profile",  choices=["full", "render-full", "lite", "render-lite"],
                   help="Process profile selection")
    p.add_argument("--errors",   metavar="NAME", nargs="?", const="ALL",
                   help="Show recent logs (NAME or all)")
    args = p.parse_args()

    os.makedirs(LOGS_DIR, exist_ok=True)

    # ── Just show errors ──────────────────────────────────────────────────────
    if args.errors:
        view_errors(None if args.errors == "ALL" else args.errors)
        return

    # ── Force single login ────────────────────────────────────────────────────
    if args.login:
        if not WIFI_OK:
            print("wifi_keepalive.py not found. Place it in the same directory.")
            return
        print(f"Attempting portal login to {__import__('wifi_keepalive').PORTAL_URL}…")
        ok, msg = portal_login()
        print(f"{'✅ SUCCESS' if ok else '❌ FAILED'}: {msg}")
        return

    # ── WiFi only mode ────────────────────────────────────────────────────────
    if args.wifi:
        if not WIFI_OK:
            print("wifi_keepalive.py not found.")
            return
        import wifi_keepalive as _wm
        _wm.main()
        return

    # ── Full mode ─────────────────────────────────────────────────────────────
    profile = _resolve_profile(args.profile, args.lite)
    selected_processes = _select_processes(profile=profile)
    managed  = [ManagedProcess(cfg) for cfg in selected_processes]
    watchdog = Watchdog(managed)
    stop_evt = threading.Event()

    # Start WiFi keepalive
    wifi = None
    if WIFI_OK:
        wifi = WifiKeepalive(tg_token=TG_TOKEN, tg_chats=TG_CHATS)
        wifi.start()
        # Expose to wifi_keepalive module so unified_dash can read speed data
        try:
            import wifi_keepalive as _wkm
            _wkm._GLOBAL_KEEPALIVE = wifi
        except Exception:
            pass
        log.info("WiFi keepalive active (check=%ds, re-login=%dh)",
                 __import__("wifi_keepalive").CHECK_INTERVAL,
                 __import__("wifi_keepalive").LOGIN_INTERVAL // 3600)
    else:
        log.warning("wifi_keepalive.py not found — internet monitoring disabled")

    # Start processes
    if not args.monitor:
        mode_lbl = profile.upper()
        print(
            f"\n  AlgoStack v10.5 Watchdog — Starting processes ({mode_lbl})\n"
            f"Author: Ridhaant Ajoy Thackur\n"
            f"  {len(selected_processes)} managed processes\n"
        )
        watchdog.start_all()
        time.sleep(2)

    _tg_async(
        f"🟢 AlgoStack v10.5 Watchdog started "
        f"({'monitoring' if args.monitor else 'full'}) "
        f"({datetime.now(IST).strftime('%H:%M IST')})\n"
        f"Watching {len(managed)} processes + WiFi keepalive"
    )

    # Start watchdog loop in background
    threading.Thread(target=watchdog.run, daemon=True, name="Watchdog").start()

    # Live display (blocks until Ctrl+C)
    try:
        _live_display(managed, stop_evt, wifi)
    except KeyboardInterrupt:
        pass
    finally:
        print("\nShutting down…")
        stop_evt.set()
        watchdog.stop_all()
        if wifi:
            wifi.stop()
        _tg_async(f"⏹ AlgoStack Watchdog stopped ({datetime.now(IST).strftime('%H:%M IST')})")
        print("Done.")


if __name__ == "__main__":
    main()
