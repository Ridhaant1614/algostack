# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v9.0 | Author: Ridhaant Ajoy Thackur
# ipc_bus.py — Multi-topic ZMQ PUB/SUB price bus for equity/commodity/crypto
# ═══════════════════════════════════════════════════════════════════════
"""
ipc_bus.py — ZeroMQ PUB/SUB multi-topic price bus  v9.0
========================================================
Topics (all on tcp://127.0.0.1:28081):
  "equity"    — NSE prices from Algofinal (38 symbols)
  "commodity" — MCX prices from commodity_engine (5 symbols)
  "crypto"    — Binance prices from crypto_engine (5 coins)
  "prices"    — ALL prices combined (backward compat for equity scanners)

live_prices.json:
  {
    "ts": "...",
    "prices": {...ALL...},          ← backward compat
    "equity_prices": {...},
    "commodity_prices": {...},
    "crypto_prices": {...},
    "price_sources": {...}
  }
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Callable, Dict, Optional

log = logging.getLogger("ipc_bus")

ZMQ_PUB_ADDR   = os.getenv("ZMQ_PUB_ADDR", "tcp://127.0.0.1:28081")
LIVE_JSON_PATH = os.path.join("levels", "live_prices.json")

try:
    import zmq
    ZMQ_OK = True
except ImportError:
    zmq = None  # type: ignore
    ZMQ_OK = False
    log.warning("pyzmq not installed — JSON-only IPC mode")


# ════════════════════════════════════════════════════════════════════════════
# PUBLISHER  (runs inside each engine)
# ════════════════════════════════════════════════════════════════════════════

class PricePublisher:
    """
    Publishes prices on named topic AND always on "prices" (backward compat).
    Also writes sectioned live_prices.json atomically.
    """

    def __init__(self) -> None:
        self._ctx:    Optional["zmq.Context"] = None
        self._socket: Optional["zmq.Socket"]  = None
        self._lock = threading.Lock()
        self._last_write_t = 0.0
        os.makedirs("levels", exist_ok=True)

        if ZMQ_OK:
            try:
                self._ctx    = zmq.Context.instance()
                self._socket = self._ctx.socket(zmq.PUB)
                self._socket.setsockopt(zmq.SNDHWM, 2)        # drop all but 2 pending
                self._socket.setsockopt(zmq.LINGER, 0)
                self._socket.setsockopt(zmq.SNDTIMEO, 5)       # 5ms send timeout
                # TCP tuning for minimum latency
                try:
                    self._socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
                    self._socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 60)
                except Exception:
                    pass
                self._socket.bind(ZMQ_PUB_ADDR)
                log.info("ZMQ PUB bound → %s", ZMQ_PUB_ADDR)
                self._start_heartbeat()
            except Exception as exc:
                log.warning("ZMQ PUB bind failed (%s) — JSON-only mode", exc)
                self._socket = None

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def _start_heartbeat(self) -> None:
        self._hb_stop = threading.Event()
        self._hb_thread = threading.Thread(target=self._hb_loop, daemon=True,
                                           name="ZMQ-HB")
        self._hb_thread.start()

    def _hb_loop(self) -> None:
        while not self._hb_stop.is_set():
            self._hb_stop.wait(5.0)
            if self._hb_stop.is_set():
                break
            if self._socket:
                try:
                    payload = json.dumps({"heartbeat": True, "ts": time.time()},
                                        separators=(",", ":"))
                    with self._lock:
                        self._socket.send_multipart([b"hb", payload.encode()],
                                                    flags=zmq.NOBLOCK)
                except Exception:
                    pass

    # ── Public API ────────────────────────────────────────────────────────────

    def publish(
        self,
        prices: Dict[str, float],
        ts,                          # datetime
        topic: str = "prices",
        min_interval_s: float = 0.0,
        *,
        equity: Optional[Dict[str, float]]    = None,
        commodity: Optional[Dict[str, float]] = None,
        crypto: Optional[Dict[str, float]]    = None,
    ) -> None:
        """
        Publish prices on given topic AND always on "prices" for backward compat.
        topic: "equity" | "commodity" | "crypto" | "prices"

        If equity/commodity/crypto dicts are supplied, writes the full sectioned
        live_prices.json. Otherwise writes the simple single-topic format.
        """
        now_t = time.monotonic()
        if min_interval_s and (now_t - self._last_write_t) < min_interval_s:
            return

        ts_str  = ts.strftime("%Y-%m-%d %H:%M:%S")
        payload = {"ts": ts_str, "prices": prices}
        data    = json.dumps(payload, separators=(",", ":"))

        # ── ZMQ publish ───────────────────────────────────────────────────────
        if self._socket is not None:
            try:
                with self._lock:
                    # Publish on specific topic
                    self._socket.send_multipart(
                        [topic.encode(), data.encode()], flags=zmq.NOBLOCK)
                    # Also publish on "prices" for backward compat
                    if topic != "prices":
                        self._socket.send_multipart(
                            [b"prices", data.encode()], flags=zmq.NOBLOCK)
            except Exception:
                pass

        # ── JSON file (atomic merge — never overwrites other asset classes) ────
        if equity is not None or commodity is not None or crypto is not None:
            self._write_sectioned_json(ts_str, equity or {}, commodity or {},
                                       crypto or {})
        else:
            # Per-topic merge: update only the relevant section
            try:
                existing: dict = {}
                if os.path.exists(LIVE_JSON_PATH):
                    try:
                        with open(LIVE_JSON_PATH, "r", encoding="utf-8") as fh:
                            existing = json.load(fh)
                    except Exception:
                        existing = {}
                if topic == "commodity":
                    existing["commodity_prices"] = prices
                    existing["commodity_ts"]     = ts_str
                elif topic == "crypto":
                    existing["crypto_prices"] = prices
                    existing["crypto_ts"]     = ts_str
                else:
                    existing["prices"]        = prices
                    existing["equity_prices"] = prices
                    existing["equity_ts"]     = ts_str
                existing["ts"] = ts_str
                # Rebuild merged "prices" key
                merged: dict = {}
                merged.update(existing.get("equity_prices") or existing.get("prices") or {})
                merged.update(existing.get("commodity_prices") or {})
                merged.update(existing.get("crypto_prices") or {})
                # Keep "prices" key = equity only for backward compat (dashboard equity count)
                # Full merged available via equity_prices + commodity_prices + crypto_prices
                if topic not in ("commodity", "crypto"):
                    existing["prices"] = merged
                tmp = LIVE_JSON_PATH + ".tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(existing, fh, separators=(",", ":"))
                os.replace(tmp, LIVE_JSON_PATH)
            except Exception:
                pass

        self._last_write_t = now_t

    def _write_sectioned_json(
        self,
        ts_str: str,
        equity: Dict[str, float],
        commodity: Dict[str, float],
        crypto: Dict[str, float],
    ) -> None:
        """Write merged live_prices.json with all asset-class sections."""
        merged  = {**equity, **commodity, **crypto}
        payload = {
            "ts":               ts_str,
            "prices":           merged,       # ALL prices (backward compat)
            "equity_prices":    equity,
            "commodity_prices": commodity,
            "crypto_prices":    crypto,
            "author":           "Ridhaant Ajoy Thackur",
        }
        try:
            tmp = LIVE_JSON_PATH + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, separators=(",", ":"))
            os.replace(tmp, LIVE_JSON_PATH)
        except Exception:
            pass

    def close(self) -> None:
        try:
            if hasattr(self, "_hb_stop"):
                self._hb_stop.set()
        except Exception:
            pass
        try:
            if self._socket:
                self._socket.close()
            if self._ctx:
                self._ctx.term()
        except Exception:
            pass


# ════════════════════════════════════════════════════════════════════════════
# SUBSCRIBER  (runs inside each scanner)
# ════════════════════════════════════════════════════════════════════════════

class PriceSubscriber:
    """
    Background thread: ZMQ SUB → PriceStore.
    Subscribes to a named topic (default "prices" for backward compat).
    Falls back to JSON file polling when ZMQ is unavailable.
    """

    def __init__(
        self,
        store: "PriceStore",
        csv_path: str,
        topic: bytes = b"prices",
        *,
        on_tick: Optional[Callable[[Dict[str, float]], None]] = None,
    ) -> None:
        self._store    = store
        self._csv_path = csv_path
        self._topic    = topic
        self._on_tick  = on_tick
        self._stop     = threading.Event()
        self._last_ts  = ""

        self._thread = threading.Thread(
            target=self._run, daemon=True,
            name=f"PriceSub-{topic.decode()}-{os.getpid()}"
        )

    def start(self) -> None:
        os.makedirs(os.path.dirname(self._csv_path) or ".", exist_ok=True)
        self._thread.start()
        mode = "ZMQ" if ZMQ_OK else "JSON-file"
        log.info("PriceSubscriber[%s] started (mode=%s)", self._topic.decode(), mode)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=6)

    def _run(self) -> None:
        import csv
        with open(self._csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["ts", "symbol", "price"])
            # Commodity/Crypto engines often fail ZMQ bind when equity publisher owns the port.
            # Default those subscribers to JSON IPC unless explicitly overridden.
            force_json = (os.getenv("FORCE_JSON_IPC", "0") == "1") or (
                self._topic in (b"commodity", b"crypto") and os.getenv("FORCE_ZMQ_IPC", "0") != "1"
            )
            if ZMQ_OK and not force_json:
                self._run_zmq(writer, fh)
            else:
                self._run_json(writer, fh)

    def _run_zmq(self, writer, fh) -> None:
        fallback_s = float(os.getenv("IPC_ZMQ_FALLBACK_S", "12.0") or 12.0)
        ctx  = zmq.Context.instance()
        sock = ctx.socket(zmq.SUB)
        sock.setsockopt(zmq.RCVHWM, 2)          # keep only 2 msgs in queue
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVTIMEO, 20)      # 20 ms — fast loop exit
        try:
            sock.setsockopt(zmq.CONFLATE, 1)   # keep only LATEST message (< 1ms latency)
        except AttributeError:
            pass
        try:
            sock.setsockopt(zmq.TCP_KEEPALIVE, 1)
        except Exception:
            pass
        sock.connect(ZMQ_PUB_ADDR)
        sock.setsockopt(zmq.SUBSCRIBE, self._topic)
        sock.setsockopt(zmq.SUBSCRIBE, b"hb")
        log.info("ZMQ SUB[%s] connected → %s", self._topic.decode(), ZMQ_PUB_ADDR)

        last_data = time.monotonic()
        while not self._stop.is_set():
            try:
                _topic, raw = sock.recv_multipart()
                payload = json.loads(raw)
                if payload.get("heartbeat"):
                    # Heartbeats confirm socket liveness, not topic data liveness.
                    if fallback_s > 0 and (time.monotonic() - last_data) > fallback_s:
                        try:
                            log.warning(
                                "ZMQ SUB[%s] heartbeat-only for %.1fs — switching to JSON IPC",
                                self._topic.decode(), time.monotonic() - last_data,
                            )
                        except Exception:
                            pass
                        try:
                            sock.close()
                        except Exception:
                            pass
                        self._run_json(writer, fh)
                        return
                    continue
                # Extra safety: ignore unexpected topics if any leaked through.
                if _topic != self._topic:
                    continue
                ts     = payload.get("ts", "")
                prices = payload.get("prices", {})
                if not prices:
                    continue
                last_data = time.monotonic()
                self._store.update(prices)
                if self._on_tick:
                    self._on_tick(prices)
                for sym, px in prices.items():
                    writer.writerow([ts, sym.upper(), px])
                fh.flush()
            except zmq.Again:
                # Auto-fallback: if ZMQ is up but no data arrives, switch to JSON polling.
                # This handles cases where publisher failed to bind and system is in JSON-only IPC mode.
                if fallback_s > 0 and (time.monotonic() - last_data) > fallback_s:
                    try:
                        log.warning(
                            "ZMQ SUB[%s] idle for %.1fs — switching to JSON IPC (set FORCE_JSON_IPC=1 to force)",
                            self._topic.decode(), time.monotonic() - last_data,
                        )
                    except Exception:
                        pass
                    try:
                        sock.close()
                    except Exception:
                        pass
                    self._run_json(writer, fh)
                    return
                continue
            except zmq.ZMQError:
                if self._stop.is_set():
                    break
                time.sleep(0.1)
            except Exception:
                time.sleep(0.1)
        sock.close()

    def _run_json(self, writer, fh) -> None:
        while not self._stop.is_set():
            try:
                if not os.path.exists(LIVE_JSON_PATH):
                    self._stop.wait(0.5)
                    continue
                with open(LIVE_JSON_PATH, "r", encoding="utf-8") as pf:
                    payload = json.load(pf)
                ts     = payload.get("ts", "")
                if self._topic == b"commodity":
                    prices = payload.get("commodity_prices", {}) or {}
                elif self._topic == b"crypto":
                    prices = payload.get("crypto_prices", {}) or {}
                elif self._topic == b"equity":
                    prices = payload.get("equity_prices", payload.get("prices", {})) or {}
                else:
                    prices = payload.get("prices", {}) or {}
                if ts == self._last_ts or not prices:
                    self._stop.wait(0.02)
                    continue
                self._last_ts = ts
                self._store.update(prices)
                if self._on_tick:
                    self._on_tick(prices)
                for sym, px in prices.items():
                    writer.writerow([ts, sym.upper(), px])
                fh.flush()
            except json.JSONDecodeError:
                self._stop.wait(0.05)
            except Exception:
                self._stop.wait(0.25)


# ════════════════════════════════════════════════════════════════════════════
# PRICE STORE  (thread-safe, shared between subscriber and sweep engine)
# ════════════════════════════════════════════════════════════════════════════

class PriceStore:
    """Thread-safe dict of {SYMBOL: latest_float_price}."""
    __slots__ = ("_lock", "_prices", "_ticks")

    def __init__(self) -> None:
        self._lock   = threading.Lock()
        self._prices: Dict[str, float] = {}
        self._ticks  = 0

    def set(self, symbol: str, price: float) -> None:
        with self._lock:
            self._prices[symbol.upper()] = float(price)

    def update(self, prices: Dict[str, float]) -> None:
        with self._lock:
            for k, v in prices.items():
                self._prices[k.upper()] = float(v)
            self._ticks += 1

    def get(self, symbol: str, default=None):
        with self._lock:
            return self._prices.get(symbol.upper(), default)

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._prices)

    @property
    def ticks(self) -> int:
        with self._lock:
            return self._ticks
