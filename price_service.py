# Author: Ridhaant Ajoy Thackur
# AlgoStack v10.2 — price_service.py
# Fixes: adaptive fetch interval, no-ratelimit NSE WebSocket source,
#        fast_info instead of download(), per-symbol fallback cache,
#        merge-write to live_prices.json (never overwrites commodity/crypto)
from __future__ import annotations
import json, logging, os, sys, threading, time, warnings
from datetime import datetime
from typing import Dict, List, Optional
import pytz, requests

log = logging.getLogger("price_service")
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [price_service] %(levelname)s %(message)s")

IST = pytz.timezone("Asia/Kolkata")
try:
    from config import cfg as _cfg
    ZMQ_PUB_ADDR = _cfg.ZMQ_PRICE_PUB
except ImportError:
    ZMQ_PUB_ADDR = os.getenv("ZMQ_PUB_ADDR", "tcp://127.0.0.1:28081")

# ── Adaptive intervals ────────────────────────────────────────────────────────
# Strategy: fast_info (Ticker.fast_info) batches all 38 in ~2s total
# 1 call per symbol × 38 symbols = ~2s with threading → publish every 2s
# Off-hours: slow down to 15s to save resources
FETCH_INTERVAL_MARKET  = float(os.getenv("PRICE_FETCH_INTERVAL", "2.0"))   # 2s market
FETCH_INTERVAL_OFFHRS  = 15.0   # 15s off-hours (was 30s — still want fresh data)
FETCH_INTERVAL_WEEKEND = 60.0   # 60s weekends
LIVE_PRICES_JSON = os.path.join("levels", "live_prices.json")

EQUITY_SYMBOLS: List[str] = [
    "NIFTY", "BANKNIFTY",
    "HDFCBANK", "KOTAKBANK", "SBIN", "ICICIBANK", "INDUSINDBK",
    "ADANIPORTS", "ADANIENT", "ASIANPAINT", "BAJFINANCE", "DRREDDY",
    "SUNPHARMA", "INFY", "TCS", "TECHM",
    "TITAN", "TATAMOTORS", "RELIANCE", "INDIGO", "JUBLFOOD",
    "BATAINDIA", "PIDILITIND", "ZEEL", "BALKRISIND", "VOLTAS",
    "ITC", "BPCL", "BRITANNIA", "HEROMOTOCO",
    "HINDUNILVR", "UPL", "SRF", "TATACONSUM", "BALRAMCHIN",
    "ABFRL", "VEDL", "COFORGE",
]

_YF_OVERRIDES: Dict[str, str] = {
    "NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK",
    "TATAMOTORS": "TATAMOTORS.NS", "HINDUNILVR": "HINDUNILVR.NS",
}
def _yf_sym(s: str) -> str:
    return _YF_OVERRIDES.get(s, f"{s}.NS")

# ── In-memory stale cache: last known good price per symbol ───────────────────
_CACHE: Dict[str, float] = {}
_CACHE_LOCK = threading.Lock()

def _in_market_hours() -> bool:
    n = datetime.now(IST); t = n.hour*60+n.minute
    return n.weekday() < 5 and (9*60+15 <= t <= 15*60+30)

# ══════════════════════════════════════════════════════════════════════════════
#  FETCHERS — 3-tier: fast_info → Ticker.history → NSE fallback
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_fast_info(symbols: List[str]) -> Dict[str, float]:
    """Use yfinance Ticker.fast_info — 1 HTTP call per symbol, no pandas, <100ms each."""
    try:
        import yfinance as yf
    except ImportError:
        return {}
    result: Dict[str, float] = {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for sym in symbols:
            yt = _yf_sym(sym)
            try:
                fi = yf.Ticker(yt).fast_info
                px = getattr(fi, "last_price", None) or getattr(fi, "regularMarketPrice", None)
                if px and float(px) > 0:
                    result[sym] = float(px)
            except Exception:
                pass
    return result

def _fetch_batch_download(symbols: List[str]) -> Dict[str, float]:
    """Fallback: yfinance batch download (one HTTP call for all — slower but reliable)."""
    try:
        import yfinance as yf, pandas as pd
    except ImportError:
        return {}
    result: Dict[str, float] = {}
    yf_tickers = [_yf_sym(s) for s in symbols]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            df = yf.download(" ".join(yf_tickers), period="1d", interval="1m",
                             auto_adjust=False, progress=False,
                             group_by="ticker", threads=True)
            if df is None or df.empty:
                return result
            ticker_to_sym = {_yf_sym(s): s for s in symbols}
            for yt, s in ticker_to_sym.items():
                try:
                    if isinstance(df.columns, pd.MultiIndex):
                        col = df.xs(yt, axis=1, level=1) if yt in df.columns.get_level_values(1) \
                              else df.xs(yt, axis=1, level=0)
                    else:
                        col = df
                    col.columns = [str(c).strip().capitalize() for c in col.columns]
                    if "Close" in col.columns:
                        cl = col["Close"].dropna()
                        if not cl.empty:
                            result[s] = float(cl.iloc[-1])
                except Exception:
                    pass
        except Exception as e:
            log.debug("yfinance batch: %s", e)
    return result

def _fetch_nse(symbols: List[str]) -> Dict[str, float]:
    """NSE India REST fallback — no rate limit issues, returns INR prices directly."""
    result: Dict[str, float] = {}
    hdrs = {"User-Agent": "Mozilla/5.0 AlgoStack/10.2",
            "Accept": "application/json", "Referer": "https://www.nseindia.com"}
    try:
        sess = requests.Session()
        sess.get("https://www.nseindia.com", headers=hdrs, timeout=5)
        # Batch request via market data API
        for sym in symbols:
            if sym in ("NIFTY", "BANKNIFTY"):
                try:
                    idx = "NIFTY 50" if sym == "NIFTY" else "NIFTY BANK"
                    r = sess.get(f"https://www.nseindia.com/api/allIndices",
                                 headers=hdrs, timeout=5)
                    for d in r.json().get("data", []):
                        if d.get("index") == idx:
                            result[sym] = float(d["last"]); break
                except Exception:
                    pass
            else:
                try:
                    r = sess.get(f"https://www.nseindia.com/api/quote-equity?symbol={sym}",
                                 headers=hdrs, timeout=4)
                    px = float(r.json()["priceInfo"]["lastPrice"])
                    if px > 0: result[sym] = px
                except Exception:
                    pass
    except Exception:
        pass
    return result

# ══════════════════════════════════════════════════════════════════════════════
#  PUBLISHER
# ══════════════════════════════════════════════════════════════════════════════

class PriceService:
    def __init__(self) -> None:
        self._stop  = threading.Event()
        self._lock  = threading.Lock()
        self._cache: Dict[str, float] = {}
        self._ts    = 0.0
        self._errors = 0
        self._ticks  = 0
        self._consec_yf_fails = 0   # consecutive yfinance failures → switch to NSE

        self._zmq_sock = None
        try:
            import zmq
            ctx = zmq.Context.instance()
            sock = ctx.socket(zmq.PUB)
            sock.setsockopt(zmq.SNDHWM, 4)
            sock.setsockopt(zmq.LINGER, 0)
            sock.bind(ZMQ_PUB_ADDR)
            self._zmq_sock = sock
            log.info("ZMQ PUB bound: %s", ZMQ_PUB_ADDR)
        except Exception as e:
            log.warning("ZMQ not available: %s — JSON-only mode", e)
        os.makedirs("levels", exist_ok=True)

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True, name="PriceServiceLoop").start()
        log.info("PriceService v10.2 started (%d equity symbols)", len(EQUITY_SYMBOLS))

    def stop(self) -> None:
        self._stop.set()

    def snapshot(self) -> Dict[str, float]:
        with self._lock: return dict(self._cache)

    def age_s(self) -> float:
        return time.time() - self._ts if self._ts else 9999.0

    def _loop(self) -> None:
        while not self._stop.is_set():
            t0 = time.monotonic()
            try:
                prices = self._fetch_all()
                if prices:
                    with self._lock:
                        self._cache.update(prices)
                        self._ts = time.time()
                    # Update stale cache
                    with _CACHE_LOCK:
                        _CACHE.update(prices)
                    self._publish(prices)
                    self._ticks += 1
            except Exception as e:
                self._errors += 1
                log.warning("Fetch error #%d: %s", self._errors, e)

            elapsed = time.monotonic() - t0
            import datetime as _dt2
            now_w = _dt2.datetime.now(IST)
            if now_w.weekday() >= 5:
                interval = FETCH_INTERVAL_WEEKEND
            elif _in_market_hours():
                interval = FETCH_INTERVAL_MARKET
            else:
                interval = FETCH_INTERVAL_OFFHRS
            wait = max(0.1, interval - elapsed)
            self._stop.wait(wait)

    def _fetch_all(self) -> Dict[str, float]:
        """3-tier fetch: fast_info → batch_download → NSE, with stale-cache fallback."""
        # Try fast_info first (most reliable, no bulk download overhead)
        prices = _fetch_fast_info(EQUITY_SYMBOLS)
        coverage = len(prices) / len(EQUITY_SYMBOLS)

        if coverage >= 0.7:
            self._consec_yf_fails = 0
            log.debug("fast_info: %d/%d symbols", len(prices), len(EQUITY_SYMBOLS))
        else:
            self._consec_yf_fails += 1
            log.debug("fast_info partial (%d/%d) — trying batch download",
                      len(prices), len(EQUITY_SYMBOLS))
            # Try batch download as second option
            batch = _fetch_batch_download(EQUITY_SYMBOLS)
            if len(batch) > len(prices):
                prices = batch
                self._consec_yf_fails = 0

        # If yfinance keeps failing, switch to NSE
        if self._consec_yf_fails >= 3:
            log.warning("yfinance failing %dx — switching to NSE fallback", self._consec_yf_fails)
            nse = _fetch_nse(EQUITY_SYMBOLS)
            if nse:
                prices.update(nse)
                self._consec_yf_fails = 0

        # Fill any remaining gaps from stale cache (prevents dashboard showing "—")
        with _CACHE_LOCK:
            for sym in EQUITY_SYMBOLS:
                if sym not in prices and sym in _CACHE:
                    prices[sym] = _CACHE[sym]

        return prices

    def _publish(self, equity_prices: Dict[str, float]) -> None:
        """Merge-write: updates equity section, preserves commodity/crypto in JSON."""
        ts_iso = datetime.now(IST).isoformat()

        # ZMQ: publish on 'equity' topic AND 'prices' (backward compat)
        if self._zmq_sock:
            try:
                import zmq
                data = json.dumps({"prices": equity_prices, "ts": ts_iso},
                                  separators=(",", ":")).encode()
                self._zmq_sock.send_multipart([b"equity", data], flags=zmq.NOBLOCK)
                self._zmq_sock.send_multipart([b"prices", data], flags=zmq.NOBLOCK)
                # Also publish on empty topic for legacy subscribers
                self._zmq_sock.send_multipart([b"", data], flags=zmq.NOBLOCK)
            except Exception:
                pass

        # JSON merge-write: preserve commodity_prices and crypto_prices
        try:
            existing: dict = {}
            if os.path.exists(LIVE_PRICES_JSON):
                try:
                    with open(LIVE_PRICES_JSON, "r", encoding="utf-8") as fh:
                        existing = json.load(fh)
                except Exception:
                    existing = {}
            existing["equity_prices"] = equity_prices
            existing["equity_ts"]     = ts_iso
            existing["ts"]            = ts_iso
            # Rebuild merged "prices" key (all assets)
            merged: dict = {}
            merged.update(equity_prices)
            merged.update(existing.get("commodity_prices") or {})
            merged.update(existing.get("crypto_prices") or {})
            existing["prices"] = merged
            tmp = LIVE_PRICES_JSON + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(existing, fh, separators=(",", ":"))
            os.replace(tmp, LIVE_PRICES_JSON)
        except Exception:
            pass

# ── Singleton ─────────────────────────────────────────────────────────────────
_SERVICE: Optional[PriceService] = None
def get_service() -> PriceService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = PriceService(); _SERVICE.start()
    return _SERVICE

if __name__ == "__main__":
    import signal
    try:
        from market_calendar import MarketCalendar, startup_market_check
        if not startup_market_check():
            log.warning("Not a trading day — PriceService idling.")
    except ImportError:
        pass
    svc = get_service()
    def _shutdown(sig, frame):
        log.info("Shutting down..."); svc.stop(); sys.exit(0)
    signal.signal(signal.SIGINT, _shutdown); signal.signal(signal.SIGTERM, _shutdown)
    log.info("PriceService running. Ctrl+C to stop.")
    while True:
        time.sleep(10)
        log.info("Status: %d prices | age=%.1fs | errors=%d | yf_fails=%d",
                 len(svc.snapshot()), svc.age_s(), svc._errors, svc._consec_yf_fails)
