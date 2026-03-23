# ═══════════════════════════════════════════════════════════════════════
# © 2026 Ridhaant Ajoy Thackur. All rights reserved.
# AlgoStack™ is proprietary software. Unauthorised copying or distribution is prohibited.
# AlgoStack v9.0 | Author: Ridhaant Ajoy Thackur
# tg_async.py — Unified async Telegram router for equity/commodity/crypto
# ═══════════════════════════════════════════════════════════════════════
"""
tg_async.py — Non-blocking Telegram alert router  v9.0
=======================================================
THREE bots, one module:
  Equity:    cfg.TG_TOKEN         → only equity alerts + startup URL
  Commodity: cfg.TG_COMMODITY_TOKEN → only MCX alerts + startup URL
  Crypto:    cfg.TG_CRYPTO_TOKEN  → only crypto alerts + startup URL

RULE: Each bot sends ONLY two things:
  1. Dashboard URL once at startup
  2. Trade alerts for its asset class only
  NOTHING ELSE. No errors, no heartbeats, no status updates.

Usage:
    from tg_async import send_alert, send_document_alert, send_startup_url

    send_alert("BUY GOLD @ 90543", asset_class="commodity")
    send_document_alert("/path/file.xlsx", "MCX levels", asset_class="commodity")
    send_startup_url("https://xyz.ngrok-free.dev")  # sends to ALL 3 bots
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from typing import Dict, List, Optional

log = logging.getLogger("tg_async")

_MAX_QUEUE  = int(os.getenv("TG_MAX_QUEUE",   "500"))
_MAX_RETRY  = 3
_RETRY_WAIT = 1.5     # faster first retry
_SEND_TIMEOUT = 12    # connection timeout in seconds


# ════════════════════════════════════════════════════════════════════════════
# CORE ASYNC SENDER CLASS
# ════════════════════════════════════════════════════════════════════════════

class AsyncTelegramSender:
    """Thread-safe non-blocking Telegram sender (single worker per instance)."""

    def __init__(self) -> None:
        self._q: "queue.Queue[Optional[dict]]" = queue.Queue(maxsize=_MAX_QUEUE)
        self._worker = threading.Thread(target=self._run, daemon=True,
                                        name="TelegramWorker")
        self._worker.start()

    def send_text(self, text: str, *, token: str, chat_ids: List[str],
                  parse_mode: str = "HTML") -> None:
        self._enqueue({"type": "text", "text": text, "token": token,
                       "chat_ids": chat_ids, "parse_mode": parse_mode})

    def send_document(self, path: str, *, token: str, chat_ids: List[str],
                      caption: Optional[str] = None) -> None:
        self._enqueue({"type": "document", "path": path, "token": token,
                       "chat_ids": chat_ids, "caption": caption})

    def shutdown(self, wait: bool = True, timeout: float = 10.0) -> None:
        self._q.put(None)
        if wait:
            self._worker.join(timeout=timeout)

    def _enqueue(self, item: dict) -> None:
        try:
            self._q.put_nowait(item)
        except queue.Full:
            log.debug("TG queue full — dropping: %.80s",
                      item.get("text", item.get("caption", "?")))

    def _run(self) -> None:
        import requests
        while True:
            item = self._q.get()
            if item is None:
                break
            try:
                if item["type"] == "text":
                    self._do_text(requests, item)
                elif item["type"] == "document":
                    self._do_document(requests, item)
            except Exception as exc:
                log.debug("TG worker error: %s", exc)
            finally:
                self._q.task_done()

    def _do_text(self, requests, item: dict) -> None:
        url = f"https://api.telegram.org/bot{item['token']}/sendMessage"
        for cid in item["chat_ids"]:
            self._post(requests, url, data={"chat_id": cid, "text": item["text"],
                                            "parse_mode": item.get("parse_mode", "HTML")})

    def _do_document(self, requests, item: dict) -> None:
        url  = f"https://api.telegram.org/bot{item['token']}/sendDocument"
        path = item["path"]
        if not os.path.exists(path):
            return
        try:
            with open(path, "rb") as fh:
                file_bytes = fh.read()
        except Exception:
            return
        fname = os.path.basename(path)
        for cid in item["chat_ids"]:
            data = {"chat_id": cid}
            if item.get("caption"):
                data["caption"] = item["caption"]
            self._post(requests, url, data=data,
                       files={"document": (fname, file_bytes)})

    def _post(self, requests, url: str, *, data: Dict,
              files=None, retries: int = _MAX_RETRY) -> None:
        for attempt in range(1, retries + 1):
            try:
                kw: dict = {"data": data, "timeout": _SEND_TIMEOUT}
                if files:
                    kw["files"] = files
                r = requests.post(url, **kw)
                if r.status_code == 429:
                    wait = float(r.json().get("parameters", {}).get("retry_after", 5))
                    log.debug("TG rate-limited — sleeping %.1fs", wait)
                    time.sleep(min(wait, 30))
                    continue
                if r.status_code >= 500:  # server error — retry
                    if attempt < retries:
                        time.sleep(_RETRY_WAIT * attempt)
                        continue
                return   # success or non-retryable client error
            except Exception as exc:
                if attempt < retries:
                    time.sleep(_RETRY_WAIT * attempt)   # exponential-ish backoff
                else:
                    log.debug("TG send failed (%d attempts): %s", retries, exc)


# ════════════════════════════════════════════════════════════════════════════
# GLOBAL SINGLETON
# ════════════════════════════════════════════════════════════════════════════

_global_sender: Optional[AsyncTelegramSender] = None


def _get_sender() -> AsyncTelegramSender:
    global _global_sender
    if _global_sender is None:
        _global_sender = AsyncTelegramSender()
    return _global_sender


def _resolve_bot(asset_class: str):
    """Return (token, chat_ids) for the given asset class."""
    from config import cfg
    if asset_class == "commodity":
        return cfg.TG_COMMODITY_TOKEN, cfg.TG_COMMODITY_CHATS
    if asset_class == "crypto":
        return cfg.TG_CRYPTO_TOKEN, cfg.TG_CRYPTO_CHATS
    return cfg.TG_TOKEN, cfg.TG_CHAT_IDS


# ════════════════════════════════════════════════════════════════════════════
# PUBLIC API  (used everywhere in AlgoStack v9.0)
# ════════════════════════════════════════════════════════════════════════════

def send_alert(text: str, asset_class: str = "equity") -> None:
    """
    Route a trade alert to the correct Telegram bot.
    asset_class: "equity" | "commodity" | "crypto"

    IMPORTANT: Only sends if the bot token is configured.
    Never sends system messages — only trade alerts + startup URL.
    """
    token, chats = _resolve_bot(asset_class)
    if not token or not chats:
        return
    _get_sender().send_text(text, token=token, chat_ids=chats)


def send_document_alert(file_path: str, caption: str,
                        asset_class: str = "equity") -> None:
    """
    Send an Excel/document file to the correct Telegram bot (non-blocking).
    asset_class: "equity" | "commodity" | "crypto"
    """
    token, chats = _resolve_bot(asset_class)
    if not token or not chats:
        return
    if not os.path.exists(file_path):
        log.debug("send_document_alert: file not found: %s", file_path)
        return
    _get_sender().send_document(file_path, token=token, chat_ids=chats,
                                caption=caption)


def get_dashboard_url() -> str:
    """Read the ONE shared dashboard URL written by Algofinal's ngrok tunnel."""
    try:
        with open(os.path.join("levels", "dashboard_url.json"), encoding="utf-8") as f:
            return json.load(f).get("public_url", "http://localhost:8055")
    except Exception:
        return "http://localhost:8055"


_STARTUP_SENT = False  # module-level dedup: send URL only once per process

def send_startup_url(public_url: str) -> None:
    """
    Send the ONE unified dashboard URL to ALL three Telegram bots at startup.
    Idempotent — only sends once per Python process lifetime.
    v10.7: updated message format, correct version string.
    """
    global _STARTUP_SENT
    if _STARTUP_SENT:
        log.debug("send_startup_url: already sent, skipping (url=%s)", public_url)
        return
    _STARTUP_SENT = True
    msg = (
        f"🟢 AlgoStack v10.7 LIVE\n"
        f"{public_url}\n\n"
        f"📊 Equity (NSE 09:30–15:11 IST)\n"
        f"🥇 Commodity (MCX 09:00–23:30 IST)\n"
        f"₿ Crypto (Binance 24/7, 6h re-anchor)\n"
        f"📈 History | 🎯 Performance | 🤖 AI Agent\n\n"
        f"Target: 0.30%/day | 240,000 variations/day\n"
        f"© 2026 Ridhaant Ajoy Thackur"
    )
    for ac in ("equity", "commodity", "crypto"):
        send_alert(msg, asset_class=ac)
    log.info("Startup URL sent to all 3 bots: %s", public_url)


# ── Backward-compatibility wrappers (used by Algofinal v8 call sites) ─────────

def get_sender() -> AsyncTelegramSender:
    return _get_sender()


def send_text_async(text: str, *, token: str, chat_ids: List[str],
                    parse_mode: str = "HTML") -> None:
    _get_sender().send_text(text, token=token, chat_ids=chat_ids,
                            parse_mode=parse_mode)


def send_document_async(path: str, *, token: str, chat_ids: List[str],
                        caption: Optional[str] = None) -> None:
    _get_sender().send_document(path, token=token, chat_ids=chat_ids,
                                caption=caption)
