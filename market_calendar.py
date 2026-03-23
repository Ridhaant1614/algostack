# ═══════════════════════════════════════════════════════════════════════
# AlgoStack v8.0 | Author: Ridhaant Ajoy Thackur
# market_calendar.py — NSE/MCX Market Calendar + Holiday Guard
# ═══════════════════════════════════════════════════════════════════════
"""
market_calendar.py -- NSE/MCX Market Calendar
==============================================
Single source of truth for trading day / session logic.
Used by EVERY module in AlgoStack.

Usage:
    from market_calendar import MarketCalendar, startup_market_check
    if not MarketCalendar.is_trading_day(datetime.now(IST)):
        sys.exit(0)
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional, Set

import pytz
import requests

log = logging.getLogger("market_calendar")
IST = pytz.timezone("Asia/Kolkata")


class MarketCalendar:
    """
    NSE + MCX holiday-aware trading calendar.
    All methods are class methods -- no instantiation needed.
    """

    # ── NSE official equity trading holidays ─────────────────────────────────
    NSE_HOLIDAYS_2025: Set[date] = {
        date(2025, 1, 26),   # Republic Day
        date(2025, 2, 26),   # Mahashivratri
        date(2025, 3, 14),   # Holi
        date(2025, 4, 14),   # Dr. Ambedkar Jayanti
        date(2025, 4, 18),   # Good Friday
        date(2025, 5, 1),    # Maharashtra Day
        date(2025, 8, 15),   # Independence Day
        date(2025, 10, 2),   # Gandhi Jayanti
        date(2025, 10, 24),  # Diwali Laxmi Puja
        date(2025, 11, 5),   # Diwali Balipratipada
        date(2025, 11, 15),  # Gurunanak Jayanti
        date(2025, 12, 25),  # Christmas
    }

    NSE_HOLIDAYS_2026: Set[date] = {
        date(2026, 1, 26),   # Republic Day
        date(2026, 3, 19),   # Holi
        date(2026, 4, 3),    # Good Friday
        date(2026, 4, 14),   # Dr. Ambedkar Jayanti
        date(2026, 5, 1),    # Maharashtra Day
        date(2026, 8, 15),   # Independence Day
        date(2026, 10, 2),   # Gandhi Jayanti
        date(2026, 11, 14),  # Diwali
        date(2026, 12, 25),  # Christmas
    }

    # Fetched live from NSE API -- cached per process run
    _online_holidays: Set[date] = set()
    _online_fetched: bool = False
    _online_fetch_ts: float = 0.0

    # ── Core checks ──────────────────────────────────────────────────────────

    @classmethod
    def _all_holidays(cls) -> Set[date]:
        """Return union of hardcoded + live-fetched holidays."""
        if not cls._online_fetched or time.monotonic() - cls._online_fetch_ts > 86400:
            cls._try_fetch_online()
        return cls.NSE_HOLIDAYS_2025 | cls.NSE_HOLIDAYS_2026 | cls._online_holidays

    @classmethod
    def is_trading_day(cls, dt) -> bool:
        """
        True if dt is a valid NSE equity trading day
        (Mon-Fri, not an NSE holiday).
        dt can be datetime or date.
        """
        d = dt.date() if isinstance(dt, datetime) else dt
        if d.weekday() >= 5:          # Saturday=5, Sunday=6
            return False
        return d not in cls._all_holidays()

    @classmethod
    def is_trading_date_str(cls, ds: str) -> bool:
        """
        True if the date string YYYYMMDD is a valid NSE trading day.
        Handles weekends and NSE holidays.
        Returns False for any date that is Sat/Sun or an NSE holiday.
        Returns True on parse error (fail-open).

        Usage:
            from market_calendar import MarketCalendar
            if not MarketCalendar.is_trading_date_str("20260322"):  # Sunday
                skip_this_file()
        """
        try:
            d = date(int(ds[:4]), int(ds[4:6]), int(ds[6:8]))
            return cls.is_trading_day(d)
        except (ValueError, IndexError):
            log.debug("is_trading_date_str: invalid date string %r, returning True (fail-open)", ds)
            return True  # fail-open: don't accidentally skip data on parse error

    @classmethod
    def is_equity_session(cls, dt: datetime) -> bool:
        """True during NSE equity trading hours: 09:15 - 15:30 IST on a trading day."""
        if not cls.is_trading_day(dt):
            return False
        t = dt.hour * 60 + dt.minute
        return 9 * 60 + 15 <= t <= 15 * 60 + 30

    @classmethod
    def is_premarket(cls, dt: datetime) -> bool:
        """True during 09:15 - 09:29 IST (premarket -- level calc only, no entries)."""
        if not cls.is_trading_day(dt):
            return False
        t = dt.hour * 60 + dt.minute
        return 9 * 60 + 15 <= t < 9 * 60 + 30

    @classmethod
    def is_commodity_session(cls, dt: datetime) -> bool:
        """True during MCX commodity trading hours: 09:30 - 23:00 IST on a trading day."""
        if not cls.is_trading_day(dt):
            return False
        t = dt.hour * 60 + dt.minute
        return 9 * 60 + 30 <= t <= 23 * 60

    @classmethod
    def is_entry_allowed(cls, dt: datetime, is_commodity: bool = False) -> bool:
        """
        True when NEW entries are permitted:
          - Equity:    09:35 - 15:11 IST (post-blackout, pre-EOD)
          - Commodity: 09:35 - 23:00 IST
        """
        if not cls.is_trading_day(dt):
            return False
        t = dt.hour * 60 + dt.minute
        if is_commodity:
            return 9 * 60 + 35 <= t <= 23 * 60
        return 9 * 60 + 35 <= t <= 15 * 60 + 11

    @classmethod
    def next_trading_day(cls, dt) -> date:
        """Return the next calendar date that is a valid trading day."""
        d = (dt.date() if isinstance(dt, datetime) else dt) + timedelta(days=1)
        while not cls.is_trading_day(d):
            d += timedelta(days=1)
        return d

    @classmethod
    def prev_trading_day(cls, dt) -> date:
        """Return the most recent trading day before dt."""
        d = (dt.date() if isinstance(dt, datetime) else dt) - timedelta(days=1)
        while not cls.is_trading_day(d):
            d -= timedelta(days=1)
        return d

    @classmethod
    def seconds_to_next_open(cls, dt: datetime) -> int:
        """
        Seconds until 09:00 IST on the next trading day.
        Returns 0 if market is currently open.
        """
        if cls.is_equity_session(dt):
            return 0
        d = dt.date()
        if not cls.is_trading_day(d) or dt.hour >= 15 or (dt.hour == 15 and dt.minute >= 30):
            d = cls.next_trading_day(dt)
        next_open = datetime(d.year, d.month, d.day, 9, 0, 0, tzinfo=IST)
        diff = int((next_open - dt).total_seconds())
        return max(0, diff)

    @classmethod
    def status_string(cls, dt: datetime) -> str:
        """Human-readable market status for the dashboard."""
        if not cls.is_trading_day(dt):
            nd = cls.next_trading_day(dt)
            return f"MARKET CLOSED -- {dt.strftime('%A %d %b')} is not a trading day. Next: {nd.strftime('%a %d %b %Y')}"
        t = dt.hour * 60 + dt.minute
        if t < 9 * 60 + 15:
            return f"Pre-open -- Market opens at 09:15 IST"
        if cls.is_premarket(dt):
            return f"Pre-market -- Level calculation phase (entries open at 09:35)"
        if 9 * 60 + 30 <= t < 9 * 60 + 35:
            return "09:30 Blackout -- Waiting for price anchor (entries open at 09:35)"
        if cls.is_entry_allowed(dt):
            return f"LIVE -- Entries allowed until 15:11 IST"
        if t <= 15 * 60 + 30:
            return "EOD -- Squaring off positions (no new entries)"
        return f"After-hours -- Next session: {cls.next_trading_day(dt).strftime('%a %d %b')}"

    # ── Online refresh ────────────────────────────────────────────────────────

    @classmethod
    def _try_fetch_online(cls) -> None:
        """Try to fetch NSE holiday list from API. Silent on failure."""
        cls._online_fetched = True
        cls._online_fetch_ts = time.monotonic()
        try:
            r = requests.get(
                "https://www.nseindia.com/api/holiday-master?type=trading",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=5,
            )
            if r.status_code == 200:
                data = r.json()
                holidays: Set[date] = set()
                for item in data.get("CM", []):
                    try:
                        d = datetime.strptime(item["tradingDate"], "%d-%b-%Y").date()
                        holidays.add(d)
                    except Exception:
                        pass
                cls._online_holidays = holidays
                log.debug("Fetched %d NSE holidays online", len(holidays))
        except Exception as exc:
            log.debug("Online holiday fetch failed: %s", exc)


def startup_market_check(send_tg_fn=None, skip_on_weekend: bool = True) -> bool:
    """
    Call at the top of any script's main() to gate execution on trading days.

    Args:
        send_tg_fn: optional callable(msg) to send a Telegram notification
        skip_on_weekend: if False, allows non-trading day (useful for testing)

    Returns:
        True  -- today is a trading day, proceed
        False -- today is a holiday/weekend, caller should set DASHBOARD_ONLY mode
    """
    now = datetime.now(IST)
    if not skip_on_weekend:
        return True

    if not MarketCalendar.is_trading_day(now):
        nd = MarketCalendar.next_trading_day(now)
        msg = (
            f"AlgoStack: NOT a trading day "
            f"({now.strftime('%A %d %b %Y')}). "
            f"Next trading day: {nd.strftime('%A %d %b %Y')}. "
            f"Running in Dashboard-Only mode."
        )
        log.warning(msg)
        if send_tg_fn:
            try:
                send_tg_fn(msg)
            except Exception:
                pass
        return False

    log.info("Trading day confirmed: %s", now.strftime("%A %d %b %Y"))
    return True
