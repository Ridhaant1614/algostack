# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v9.0 | Author: Ridhaant Ajoy Thackur
# wifi_keepalive.py — Internet monitor + speedtest + portal auto-login
# ═══════════════════════════════════════════════════════════════════════
"""
wifi_keepalive.py  v9.0
========================
Every 90 seconds:
  1. Fast TCP connectivity check (8.8.8.8:53) — < 100ms
  2. Speedtest.net speed measurement (download + upload + ping)
  3. If internet is DOWN → auto-login to captive portal
  4. Proactive re-login every 2h (session lasts 4h, 2h buffer)

Portal : https://172.22.2.6/connect/PortalMain
Login  : 22ucs164 / h78032ps

Speed data is stored in self.speed_history (last 50 readings) and
exposed via self.status dict for the autohealer Rich table and
the /sys dashboard page.
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import ssl
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pytz

log = logging.getLogger("wifi_keepalive")

# Suppress InsecureRequestWarning — portal uses self-signed cert on private IP
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass
import warnings as _warnings
_warnings.filterwarnings("ignore", message="Unverified HTTPS")
IST = pytz.timezone("Asia/Kolkata")

# ── Configuration ─────────────────────────────────────────────────────────────
PORTAL_URL      = os.getenv("WIFI_PORTAL_URL",    "https://172.22.2.6/connect/PortalMain")
USERNAME        = os.getenv("WIFI_USERNAME",       "22ucs164")
PASSWORD        = os.getenv("WIFI_PASSWORD",       "h78032ps")
CHECK_INTERVAL  = int(os.getenv("WIFI_CHECK_INTERVAL",  "90"))    # seconds
LOGIN_INTERVAL  = int(os.getenv("WIFI_LOGIN_INTERVAL",  "7200"))  # 2 hours
SPEEDTEST_EVERY = int(os.getenv("WIFI_SPEEDTEST_EVERY", "90"))    # seconds (same as check)

# TCP connectivity targets — any reachable = connected
_TCP_TARGETS = [("8.8.8.8", 53), ("1.1.1.1", 53), ("8.8.4.4", 53)]
_HTTP_PROBE  = "http://connectivitycheck.gstatic.com/generate_204"

# Self-signed cert context for portal
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode    = ssl.CERT_NONE


# ════════════════════════════════════════════════════════════════════════════
#  CONNECTIVITY CHECK
# ════════════════════════════════════════════════════════════════════════════

def is_internet_up(timeout_s: float = 2.0) -> bool:
    """Fast TCP probe — returns True if any DNS target is reachable."""
    for host, port in _TCP_TARGETS:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout_s)
            s.connect((host, port))
            s.close()
            return True
        except OSError:
            continue
    # HTTP fallback
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(_HTTP_PROBE, headers={"User-Agent": "AlgoStack/9.0"}),
            timeout=timeout_s,
        )
        return r.status == 204
    except Exception:
        return False


def _rtt_ms(host: str = "8.8.8.8", port: int = 53, timeout: float = 2.0) -> Optional[float]:
    """Measure TCP connection latency in milliseconds."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        t0 = time.perf_counter()
        s.connect((host, port))
        s.close()
        return round((time.perf_counter() - t0) * 1000, 2)
    except Exception:
        return None


# ════════════════════════════════════════════════════════════════════════════
#  SPEEDTEST  (speedtest-cli library + lightweight fallback)
# ════════════════════════════════════════════════════════════════════════════

class _SpeedResult:
    __slots__ = ("ts", "ping_ms", "download_mbps", "upload_mbps", "server", "ok", "error")
    def __init__(self):
        self.ts            = datetime.now(IST).strftime("%H:%M:%S")
        self.ping_ms       = 0.0
        self.download_mbps = 0.0
        self.upload_mbps   = 0.0
        self.server        = ""
        self.ok            = False
        self.error         = ""

    def to_dict(self) -> dict:
        return {
            "ts": self.ts, "ping_ms": self.ping_ms,
            "download_mbps": self.download_mbps,
            "upload_mbps":   self.upload_mbps,
            "server":        self.server,
            "ok":            self.ok,
            "error":         self.error,
        }


def _run_speedtest_cli() -> _SpeedResult:
    """Run speedtest using the speedtest-cli library (tests against speedtest.net)."""
    r = _SpeedResult()
    try:
        import speedtest as _st
        st = _st.Speedtest(secure=True)
        st.get_best_server()
        st.download(threads=4)
        st.upload(threads=2)
        res = st.results.dict()
        r.ping_ms       = round(res.get("ping", 0), 2)
        r.download_mbps = round(res.get("download", 0) / 1_000_000, 2)
        r.upload_mbps   = round(res.get("upload",   0) / 1_000_000, 2)
        r.server        = res.get("server", {}).get("host", "speedtest.net")
        r.ok            = True
        log.info("Speedtest: ping=%.1fms  ↓%.2f Mbps  ↑%.2f Mbps  [%s]",
                 r.ping_ms, r.download_mbps, r.upload_mbps, r.server)
    except ImportError:
        r.error = "speedtest-cli not installed"
        log.debug("speedtest-cli not installed — using fallback")
        r = _run_speedtest_fallback()
    except Exception as exc:
        r.error = str(exc)
        log.warning("Speedtest error: %s — using fallback", exc)
        r = _run_speedtest_fallback()
    return r


def _run_speedtest_fallback() -> _SpeedResult:
    """
    Lightweight fallback: download a 5MB file from a CDN and measure throughput.
    Uses Cloudflare's public speed test endpoint (no speedtest.net account needed).
    """
    r = _SpeedResult()
    try:
        # Measure RTT (ping)
        rtt = _rtt_ms("8.8.8.8") or _rtt_ms("1.1.1.1")
        r.ping_ms = rtt or 0.0

        # Download test: 5MB from Cloudflare speed.cloudflare.com
        test_url = "https://speed.cloudflare.com/__down?bytes=5000000"
        t0 = time.perf_counter()
        req = urllib.request.Request(test_url, headers={"User-Agent": "AlgoStack/9.0"})
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            data = resp.read()
        elapsed = time.perf_counter() - t0
        bytes_recv = len(data)
        r.download_mbps = round(bytes_recv * 8 / elapsed / 1_000_000, 2)
        r.upload_mbps   = 0.0   # skip upload in fallback (save time)
        r.server        = "speed.cloudflare.com (fallback)"
        r.ok            = True
        log.info("Speedtest (fallback): ping=%.1fms  ↓%.2f Mbps  [CDN]",
                 r.ping_ms, r.download_mbps)
    except Exception as exc:
        r.error = str(exc)
        # Absolute fallback: just measure ping
        rtt = _rtt_ms()
        if rtt is not None:
            r.ping_ms = rtt
            r.ok = True
            r.server = "ping-only"
        log.debug("Speedtest fallback error: %s", exc)
    return r


# ════════════════════════════════════════════════════════════════════════════
#  PORTAL LOGIN
# ════════════════════════════════════════════════════════════════════════════

def _login_requests() -> Tuple[bool, str]:
    """Login using requests library — handles cookies, redirects, hidden fields."""
    try:
        import requests
        import warnings
        warnings.filterwarnings("ignore", message="Unverified HTTPS")

        sess = requests.Session()
        sess.verify = False

        # GET portal to grab cookies + hidden fields
        try:
            resp = sess.get(PORTAL_URL, timeout=8, allow_redirects=True)
            html = resp.text
            eff_url = resp.url
        except Exception:
            html = ""; eff_url = PORTAL_URL

        # Extract hidden inputs
        hidden: Dict[str, str] = {}
        for m in re.finditer(
            r'<input[^>]+type=["\']hidden["\'][^>]*name=["\']([^"\']+)["\'][^>]*value=["\']([^"\']*)["\']',
            html, re.IGNORECASE,
        ):
            hidden[m.group(1)] = m.group(2)
        for m in re.finditer(
            r'<input[^>]+name=["\']([^"\']+)["\'][^>]*value=["\']([^"\']*)["\'][^>]*type=["\']hidden["\']',
            html, re.IGNORECASE,
        ):
            hidden[m.group(1)] = m.group(2)

        # Try multiple payload formats used by common captive portals
        payloads = [
            # Cisco ISE / Aruba ClearPass
            {**hidden, "username": USERNAME, "password": PASSWORD,
             "buttonClicked": "4", "err_flag": "0", "err_msg": "",
             "info_flag": "0", "info_msg": "", "redirect_url": ""},
            # Generic
            {**hidden, "username": USERNAME, "password": PASSWORD,
             "action": "login", "submit": "Login"},
            # FortiGate
            {**hidden, "username": USERNAME, "credential": PASSWORD,
             "magic": hidden.get("magic", ""), "4Tredir": "http://www.google.com/"},
            # Minimal fallback
            {"username": USERNAME, "password": PASSWORD},
        ]

        for i, payload in enumerate(payloads):
            try:
                r = sess.post(eff_url, data=payload, timeout=12,
                              allow_redirects=True, verify=False)
                loc      = r.url or ""
                external = not any(loc.startswith(f"http{'s' if s else ''}://172.") for s in [True, False])
                bad_kw   = any(k in r.text.lower() for k in
                               ("invalid", "incorrect", "failed", "error", "login", "username"))
                if external or (r.status_code == 200 and not bad_kw):
                    msg = f"Login OK (variant {i+1}, status={r.status_code})"
                    return True, msg
            except Exception as e:
                log.debug("Login payload %d error: %s", i+1, e)
                continue

        if is_internet_up():
            return True, "Internet up (login probably OK)"
        return False, "All login payloads exhausted"

    except ImportError:
        return _login_urllib()
    except Exception as e:
        return False, f"requests exception: {e}"


def _login_urllib() -> Tuple[bool, str]:
    """Stdlib-only fallback login."""
    try:
        payload = urllib.parse.urlencode({
            "username": USERNAME, "password": PASSWORD,
            "buttonClicked": "4", "err_flag": "0", "redirect_url": "",
        }).encode()
        req = urllib.request.Request(
            PORTAL_URL, data=payload, method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded",
                     "User-Agent": "AlgoStack/9.0"},
        )
        urllib.request.urlopen(req, context=_SSL_CTX, timeout=10)
        if is_internet_up():
            return True, "urllib login OK"
        return False, "urllib POST sent but internet still down"
    except Exception as e:
        return False, f"urllib login: {e}"


def portal_login() -> Tuple[bool, str]:
    """Attempt login; tries requests then urllib fallback."""
    ok, msg = _login_requests()
    if ok:
        return ok, msg
    log.warning("Primary login failed (%s) — urllib fallback", msg)
    return _login_urllib()


# ════════════════════════════════════════════════════════════════════════════
#  WIFI KEEPALIVE CLASS
# ════════════════════════════════════════════════════════════════════════════

class WifiKeepalive:
    """
    Background daemon:
      • Check internet every 90s via TCP probe
      • Run speedtest.net test every 90s (non-blocking thread)
      • Auto-login to captive portal when down
      • Proactive re-login every 2h
      • Telegram alerts on state changes
      • Exposes self.status dict and self.speed_history list
    """

    MAX_SPEED_HISTORY = 50

    def __init__(
        self,
        *,
        tg_token: str = "",
        tg_chats: list = None,
        on_status_change=None,
    ) -> None:
        self._tg_token  = tg_token
        self._tg_chats  = tg_chats or []
        self._on_change = on_status_change
        self._stop      = threading.Event()
        self._thread    = threading.Thread(
            target=self._run, daemon=True, name="WifiKeepalive",
        )
        self._speed_lock    = threading.Lock()
        self._speed_running = False

        # Public state
        self.last_login_t       = 0.0
        self.last_status        = "unknown"
        self.login_count        = 0
        self.consecutive_fails  = 0
        self.last_login_msg     = ""
        self.last_check_t       = 0.0
        self.last_speedtest_t   = 0.0
        self.speed_history: List[dict] = []   # last MAX_SPEED_HISTORY results
        self.latest_speed: Optional[dict] = None

    def start(self) -> None:
        self._thread.start()
        log.info("WifiKeepalive started  check=%ds  re-login=%dh  speedtest=%ds",
                 CHECK_INTERVAL, LOGIN_INTERVAL // 3600, SPEEDTEST_EVERY)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=6)

    def force_login(self) -> Tuple[bool, str]:
        return self._do_login("manual")

    def run_speedtest_now(self) -> Optional[dict]:
        """Trigger a speedtest in the calling thread. Returns result dict."""
        r = _run_speedtest_cli()
        self._record_speed(r)
        return r.to_dict()

    @property
    def status(self) -> dict:
        spd = self.latest_speed or {}
        return {
            "internet_up":       self.last_status == "up",
            "last_status":       self.last_status,
            "login_count":       self.login_count,
            "last_login_msg":    self.last_login_msg,
            "last_login_ago_s":  round(time.monotonic() - self.last_login_t, 0)
                                  if self.last_login_t else None,
            "next_login_in_s":   max(0, LOGIN_INTERVAL - (time.monotonic() - self.last_login_t))
                                  if self.last_login_t else 0,
            # Speedtest fields
            "ping_ms":           spd.get("ping_ms", 0),
            "download_mbps":     spd.get("download_mbps", 0),
            "upload_mbps":       spd.get("upload_mbps", 0),
            "speed_server":      spd.get("server", ""),
            "last_speedtest_ts": spd.get("ts", ""),
            "speedtest_age_s":   round(time.monotonic() - self.last_speedtest_t, 0)
                                  if self.last_speedtest_t else 9999,
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    def _run(self) -> None:
        log.info("WiFi initial login on startup…")
        self._do_login("startup")
        # Immediate speedtest after startup (non-blocking)
        self._launch_speedtest()
        self._stop.wait(3)

        while not self._stop.is_set():
            now_t = time.monotonic()
            self.last_check_t = now_t

            # ── Connectivity check ───────────────────────────────────────────
            up = is_internet_up()

            if up:
                if self.last_status != "up":
                    log.info("Internet UP ✓")
                    if self.last_status == "down":
                        self._tg(f"✅ AlgoStack: Internet restored "
                                 f"({datetime.now(IST).strftime('%H:%M IST')})")
                self._set_status("up")
                self.consecutive_fails = 0

                # Proactive re-login every LOGIN_INTERVAL
                if (now_t - self.last_login_t) >= LOGIN_INTERVAL:
                    log.info("Proactive re-login (session renewal)")
                    self._do_login("scheduled")

            else:
                self.consecutive_fails += 1
                log.warning("Internet DOWN (fail #%d) — attempting login…",
                            self.consecutive_fails)
                if self.consecutive_fails == 1:
                    self._tg(f"⚠️ AlgoStack: Internet DOWN — portal login "
                             f"({datetime.now(IST).strftime('%H:%M IST')})")
                self._set_status("down")

                ok, msg = self._do_login("reconnect")
                if ok:
                    self.consecutive_fails = 0
                    self._set_status("up")
                    self._tg(f"✅ AlgoStack: Re-login OK — internet restored "
                             f"({datetime.now(IST).strftime('%H:%M IST')})")
                else:
                    backoff = min(10 * self.consecutive_fails, 60)
                    log.error("Re-login failed: %s — retry in %ds", msg, backoff)
                    self._stop.wait(backoff)
                    continue

            # ── Speedtest (every SPEEDTEST_EVERY seconds, non-blocking) ─────
            if (now_t - self.last_speedtest_t) >= SPEEDTEST_EVERY and up:
                self._launch_speedtest()

            self._stop.wait(CHECK_INTERVAL)

    def _launch_speedtest(self) -> None:
        """Start speedtest in a daemon thread so it never blocks the check loop."""
        with self._speed_lock:
            if self._speed_running:
                return   # previous test still running, skip
            self._speed_running = True

        def _worker():
            try:
                r = _run_speedtest_cli()
                self._record_speed(r)
                # Alert if bandwidth very low (< 1 Mbps download)
                if r.ok and r.download_mbps < 1.0:
                    self._tg(f"⚠️ AlgoStack: Low bandwidth — "
                             f"↓{r.download_mbps:.2f} Mbps  ping={r.ping_ms:.0f}ms")
            finally:
                with self._speed_lock:
                    self._speed_running = False

        threading.Thread(target=_worker, daemon=True, name="Speedtest").start()

    def _record_speed(self, r: _SpeedResult) -> None:
        d = r.to_dict()
        self.latest_speed    = d
        self.last_speedtest_t = time.monotonic()
        with self._speed_lock:
            self.speed_history.append(d)
            if len(self.speed_history) > self.MAX_SPEED_HISTORY:
                self.speed_history.pop(0)
        if r.ok:
            log.info("Speed  ping=%.1fms  ↓%.2f Mbps  ↑%.2f Mbps  [%s]",
                     r.ping_ms, r.download_mbps, r.upload_mbps, r.server)
        else:
            log.warning("Speedtest failed: %s", r.error)

    def _do_login(self, reason: str) -> Tuple[bool, str]:
        self._set_status("logging_in")
        ts = datetime.now(IST).strftime("%H:%M:%S IST")
        log.info("Portal login (%s) at %s  [%s / %s]", reason, ts, USERNAME, PORTAL_URL)
        ok, msg = portal_login()
        self.last_login_msg = f"[{ts}] {msg}"
        if ok:
            self.login_count  += 1
            self.last_login_t  = time.monotonic()
            self._set_status("up")
            log.info("Login SUCCESS (#%d, %s): %s", self.login_count, reason, msg)
        else:
            self._set_status("down")
            log.error("Login FAILED (%s): %s", reason, msg)
        return ok, msg

    def _set_status(self, s: str) -> None:
        self.last_status = s
        if self._on_change:
            try:
                self._on_change(s)
            except Exception:
                pass

    def _tg(self, text: str) -> None:
        if not (self._tg_token and self._tg_chats):
            return
        def _go():
            for cid in self._tg_chats:
                try:
                    data = urllib.parse.urlencode({"chat_id": cid, "text": text}).encode()
                    urllib.request.urlopen(
                        f"https://api.telegram.org/bot{self._tg_token}/sendMessage",
                        data=data, timeout=10,
                    )
                except Exception:
                    pass
        threading.Thread(target=_go, daemon=True).start()


# ════════════════════════════════════════════════════════════════════════════
#  STANDALONE
# ════════════════════════════════════════════════════════════════════════════

# Module-level global — autohealer sets this so dashboard can read speed data
_GLOBAL_KEEPALIVE: Optional["WifiKeepalive"] = None


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [wifi] %(levelname)s %(message)s",
    )
    import warnings
    warnings.filterwarnings("ignore")

    print(f"\n  WiFi Keepalive v9.0")
    print(f"  Portal:   {PORTAL_URL}")
    print(f"  Username: {USERNAME}")
    print(f"  Check:    every {CHECK_INTERVAL}s")
    print(f"  Speedtest: every {SPEEDTEST_EVERY}s (speedtest.net)\n")

    TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7587307352:AAG6RaiF4gO5I_ZFZ_4b8Gj7dnsu4GtPWFw")
    TG_CHATS = [c for c in [os.getenv("TELEGRAM_CHAT_ID","1376513391"), "793674804"] if c]

    wk = WifiKeepalive(tg_token=TG_TOKEN, tg_chats=TG_CHATS)
    wk.start()

    try:
        while True:
            s   = wk.status
            spd = f"↓{s['download_mbps']:.1f}Mbps  ↑{s['upload_mbps']:.1f}Mbps  {s['ping_ms']:.0f}ms" \
                  if s.get("download_mbps") else "speedtest pending..."
            print(
                f"\r[{datetime.now(IST).strftime('%H:%M:%S')}] "
                f"Internet: {'UP ✓' if s['internet_up'] else 'DOWN ✗'}  "
                f"Speed: {spd}  "
                f"Logins: {s['login_count']}  "
                f"Next re-login: {int(s['next_login_in_s'])}s    ",
                end="", flush=True,
            )
            time.sleep(5)
    except KeyboardInterrupt:
        wk.stop()
        print("\nStopped.")


if __name__ == "__main__":
    main()
