# Author: Ridhaant Ajoy Thackur
"""
news_dashboard.py -- India Market News Intelligence Dashboard (AlgoStack v3.0)
==============================================================================
Plotly Dash application on port 8070.
Integrates into unified_dash_v3.py as the "Intel" tab.

Features:
  - Live Nifty/Sensex/INR-USD/Crude banner (30s refresh)
  - Short/long term market predictions
  - RSS feeds: ET Markets, Moneycontrol, NSE, BSE, RBI, state news
  - International RSS: Reuters, Fed, CNBC
  - Reddit: r/IndiaInvestments, r/DalalStreetBets sentiment
  - Sector sentiment heatmap
  - Economic calendar (RBI MPC, FOMC dates)
  - FII/DII flow chart

Run standalone:  python news_dashboard.py
Or add to autohealer PROCESSES list (port 8070).
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests

log = logging.getLogger("news_dashboard")
IST = pytz.timezone("Asia/Kolkata")

try:
    from config import cfg as _cfg
    NEWS_PORT   = _cfg.NEWS_DASH_PORT
    REDDIT_ID   = _cfg.REDDIT_CLIENT_ID
    REDDIT_SEC  = _cfg.REDDIT_CLIENT_SECRET
    NEWS_API_KEY = _cfg.NEWS_API_KEY
except ImportError:
    NEWS_PORT   = int(os.getenv("NEWS_DASH_PORT", "8070"))
    REDDIT_ID   = os.getenv("REDDIT_CLIENT_ID", "")
    REDDIT_SEC  = os.getenv("REDDIT_CLIENT_SECRET", "")
    NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

try:
    from sentiment_analyzer import analyzer as _sa
except ImportError:
    _sa = None

try:
    from trend_predictor import TrendPredictor
except ImportError:
    TrendPredictor = None


# ══════════════════════════════════════════════════════════════════════════════
#  RSS FEED SOURCES
# ══════════════════════════════════════════════════════════════════════════════

INDIA_RSS = {
    "ET Markets":        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "Moneycontrol":      "https://www.moneycontrol.com/rss/latestnews.xml",
    "Business Standard": "https://www.business-standard.com/rss/markets-106.rss",
    "LiveMint":          "https://lifestyle.livemint.com/news/rss/markets.xml",
    "Financial Express": "https://www.financialexpress.com/market/feed/",
    "RBI Press":         "https://www.rbi.org.in/scripts/RSS.aspx?Id=0&Category=0",
    "PIB Finance":       "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3",
    "Maharashtra ET":    "https://economictimes.indiatimes.com/maharashtra/rssfeeds/15286414.cms",
}

INTL_RSS = {
    "Reuters Business":  "https://feeds.reuters.com/reuters/businessNews",
    "CNBC World":        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "US Fed":            "https://www.federalreserve.gov/feeds/press_all.xml",
}

# Economic calendar (static -- update quarterly)
ECONOMIC_CALENDAR = [
    {"date": "2026-04-09", "event": "RBI MPC Meeting",       "impact": "HIGH",   "type": "India"},
    {"date": "2026-04-30", "event": "US FOMC Meeting",       "impact": "HIGH",   "type": "Global"},
    {"date": "2026-05-12", "event": "India CPI Release",     "impact": "MEDIUM", "type": "India"},
    {"date": "2026-06-04", "event": "RBI MPC Meeting",       "impact": "HIGH",   "type": "India"},
    {"date": "2026-06-17", "event": "US FOMC Meeting",       "impact": "HIGH",   "type": "Global"},
    {"date": "2026-04-15", "event": "India IIP Data",        "impact": "MEDIUM", "type": "India"},
    {"date": "2026-05-28", "event": "India Q4 GDP Advance",  "impact": "HIGH",   "type": "India"},
]

_NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 AlgoStack/3.0",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com",
}


# ══════════════════════════════════════════════════════════════════════════════
#  NEWS AGGREGATOR
# ══════════════════════════════════════════════════════════════════════════════

class NewsAggregator:
    """
    Fetches news from RSS feeds, Reddit, and NSE/FII APIs.
    Stores results in thread-safe cache.
    Background thread refreshes on schedule.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: Dict[str, Any] = {
            "india_news":   [],
            "intl_news":    [],
            "reddit_posts": [],
            "fii_data":     {},
            "market_banner": {},
            "sentiment":    {"score": 0.0, "label": "NEUTRAL", "n_samples": 0},
            "prediction":   {},
            "last_refresh": 0.0,
        }
        self._stop = threading.Event()

    def start(self) -> None:
        t = threading.Thread(target=self._loop, daemon=True, name="NewsAggregator")
        t.start()
        log.info("NewsAggregator started")

    def stop(self) -> None:
        self._stop.set()

    def get(self, key: str, default=None):
        with self._lock:
            return self._cache.get(key, default)

    def _loop(self) -> None:
        last_news   = 0.0
        last_reddit = 0.0
        last_fii    = 0.0
        last_banner = 0.0
        last_pred   = 0.0
        while not self._stop.is_set():
            now = time.monotonic()
            try:
                if now - last_banner > 30:
                    self._refresh_banner()
                    last_banner = now
                if now - last_news > 300:
                    self._refresh_news()
                    last_news = now
                if now - last_reddit > 600:
                    self._refresh_reddit()
                    last_reddit = now
                if now - last_fii > 1800:
                    self._refresh_fii()
                    last_fii = now
                if now - last_pred > 900:
                    self._refresh_prediction()
                    last_pred = now
                with self._lock:
                    self._cache["last_refresh"] = time.time()
            except Exception as e:
                log.warning("Aggregator error: %s", e)
            self._stop.wait(15)

    # ── Banner ────────────────────────────────────────────────────────────────
    def _refresh_banner(self) -> None:
        try:
            import yfinance as yf
            import warnings
            symbols = {"^NSEI":"Nifty50", "^BSESN":"Sensex",
                       "INR=X":"USD/INR", "CL=F":"Crude Oil"}
            banner = {}
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                for ticker, name in symbols.items():
                    try:
                        t = yf.Ticker(ticker)
                        px = t.fast_info.get("lastPrice")
                        prev = t.fast_info.get("previousClose")
                        if px and prev:
                            chg = (float(px) - float(prev)) / float(prev) * 100
                            banner[name] = {"price": round(float(px), 2), "chg": round(chg, 2)}
                    except Exception:
                        pass
            with self._lock:
                self._cache["market_banner"] = banner
        except Exception as e:
            log.debug("Banner refresh error: %s", e)

    # ── RSS news ──────────────────────────────────────────────────────────────
    def _refresh_news(self) -> None:
        india, intl = [], []
        india = self._fetch_rss(INDIA_RSS, max_per_source=5)
        intl  = self._fetch_rss(INTL_RSS,  max_per_source=4)
        # Score sentiment
        if _sa:
            all_items = india + intl
            for item in all_items:
                s = _sa.analyze(item.get("title","") + " " + item.get("summary",""))
                item["sentiment"] = s
            agg = _sa.aggregate([i["sentiment"] for i in all_items if "sentiment" in i])
        else:
            agg = {"score": 0.0, "label": "NEUTRAL", "n_samples": 0}
        with self._lock:
            self._cache["india_news"]  = india[:30]
            self._cache["intl_news"]   = intl[:20]
            self._cache["sentiment"]   = agg

    def _fetch_rss(self, sources: Dict[str, str], max_per_source: int = 5) -> List[dict]:
        try:
            import feedparser
        except ImportError:
            log.warning("feedparser not installed. pip install feedparser")
            return []
        items = []
        for source, url in sources.items():
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:max_per_source]:
                    pub = ""
                    if hasattr(entry, "published"):
                        pub = str(entry.published)[:16]
                    items.append({
                        "source":   source,
                        "title":    getattr(entry, "title", ""),
                        "summary":  getattr(entry, "summary", "")[:200],
                        "link":     getattr(entry, "link", ""),
                        "published": pub,
                    })
            except Exception:
                pass
        # Sort by published (newest first, best effort)
        return sorted(items, key=lambda x: x.get("published",""), reverse=True)

    # ── Reddit ────────────────────────────────────────────────────────────────
    def _refresh_reddit(self) -> None:
        if not REDDIT_ID or not REDDIT_SEC:
            # Public API fallback (JSON endpoint, no auth needed for public posts)
            posts = self._fetch_reddit_public(["IndiaInvestments", "DalalStreetBets"])
        else:
            posts = self._fetch_reddit_oauth(["IndiaInvestments", "DalalStreetBets",
                                              "IndianStreetBets"])
        if _sa:
            for p in posts:
                p["sentiment"] = _sa.analyze(p.get("title",""))
        with self._lock:
            self._cache["reddit_posts"] = posts[:25]

    def _fetch_reddit_public(self, subreddits: List[str]) -> List[dict]:
        posts = []
        hdrs  = {"User-Agent": "AlgoStack/3.0"}
        for sub in subreddits:
            try:
                r = requests.get(
                    f"https://www.reddit.com/r/{sub}/hot.json?limit=10",
                    headers=hdrs, timeout=8,
                )
                for child in r.json().get("data",{}).get("children",[]):
                    d = child.get("data", {})
                    posts.append({
                        "subreddit": sub,
                        "title":     d.get("title",""),
                        "score":     d.get("score", 0),
                        "comments":  d.get("num_comments", 0),
                        "url":       "https://reddit.com" + d.get("permalink",""),
                    })
            except Exception:
                pass
        return sorted(posts, key=lambda x: -x.get("score",0))

    def _fetch_reddit_oauth(self, subreddits: List[str]) -> List[dict]:
        try:
            import praw
            reddit = praw.Reddit(
                client_id=REDDIT_ID,
                client_secret=REDDIT_SEC,
                user_agent=os.getenv("REDDIT_USER_AGENT", "AlgoStack/3.0"),
            )
            posts = []
            for sub in subreddits:
                try:
                    for p in reddit.subreddit(sub).hot(limit=8):
                        posts.append({
                            "subreddit": sub,
                            "title":     p.title,
                            "score":     p.score,
                            "comments":  p.num_comments,
                            "url":       f"https://reddit.com{p.permalink}",
                        })
                except Exception:
                    pass
            return sorted(posts, key=lambda x: -x.get("score",0))
        except ImportError:
            return self._fetch_reddit_public(subreddits)

    # ── FII/DII ────────────────────────────────────────────────────────────────
    def _refresh_fii(self) -> None:
        try:
            sess = requests.Session()
            sess.get("https://www.nseindia.com", headers=_NSE_HEADERS, timeout=5)
            r = sess.get(
                "https://www.nseindia.com/api/fiidiiTradeReact",
                headers=_NSE_HEADERS, timeout=8,
            )
            data = r.json()
            fii = {}
            for i, row in enumerate(data[:5]):   # last 5 trading days
                fii[i] = {
                    "date":     row.get("date",""),
                    "fii_buy":  float(row.get("fiiBuy", 0)),
                    "fii_sell": float(row.get("fiiSell", 0)),
                    "fii_net":  float(row.get("fiiNetVal", 0)),
                    "dii_buy":  float(row.get("diiBuy", 0)),
                    "dii_sell": float(row.get("diiSell", 0)),
                    "dii_net":  float(row.get("diiNetVal", 0)),
                }
            with self._lock:
                self._cache["fii_data"] = fii
        except Exception as e:
            log.debug("FII data error: %s", e)

    # ── Prediction ────────────────────────────────────────────────────────────
    def _refresh_prediction(self) -> None:
        if TrendPredictor is None:
            return
        try:
            with self._lock:
                sent_score = self._cache["sentiment"].get("score", 0.0)
            pred = TrendPredictor.predict(sentiment_score=sent_score)
            with self._lock:
                self._cache["prediction"] = pred
        except Exception as e:
            log.debug("Prediction error: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
#  DASH APP
# ══════════════════════════════════════════════════════════════════════════════

def build_news_app(aggregator: NewsAggregator):
    """Build and return a Dash app for the news intelligence dashboard."""
    import dash
    from dash import dcc, html, Input, Output, callback

    app = dash.Dash(
        __name__,
        title="AlgoStack Market Intelligence",
        requests_pathname_prefix="/intel/",
        suppress_callback_exceptions=True,
    )
    server = app.server

    # ── Colors ────────────────────────────────────────────────────────────────
    BG    = "#0d1117"; CARD  = "#161b22"; BORDER= "#30363d"
    TEXT  = "#e6edf3"; DIM   = "#8b949e"; ACCENT= "#58a6ff"
    GREEN = "#3fb950"; RED   = "#f85149"; AMBER = "#d29922"
    YELLOW= "#e3b341"; FONT  = "Inter, system-ui, sans-serif"

    _IMPACT_CLR = {"HIGH": RED, "MEDIUM": AMBER, "LOW": DIM}
    _SENT_CLR   = lambda s: GREEN if s > 0.1 else RED if s < -0.1 else DIM

    def _badge(text: str, color: str) -> html.Span:
        return html.Span(text, style={
            "background": color + "22", "color": color,
            "border": f"1px solid {color}44",
            "borderRadius": "4px", "padding": "2px 7px",
            "fontSize": "11px", "fontWeight": "600", "marginRight": "4px",
        })

    def _card(*children, title: str = "", style: dict = None) -> html.Div:
        inner = []
        if title:
            inner.append(html.Div(title, style={"color": ACCENT, "fontSize": "11px",
                                                "fontWeight": "700", "letterSpacing": "0.08em",
                                                "marginBottom": "12px", "textTransform": "uppercase"}))
        inner.extend(children)
        return html.Div(inner, style={
            "background": CARD, "border": f"1px solid {BORDER}",
            "borderRadius": "8px", "padding": "16px",
            "marginBottom": "12px", **(style or {})
        })

    # ── Layout ────────────────────────────────────────────────────────────────
    app.layout = html.Div([
        dcc.Interval(id="t30",  interval=30_000,  n_intervals=0),
        dcc.Interval(id="t300", interval=300_000, n_intervals=0),
        dcc.Interval(id="t900", interval=900_000, n_intervals=0),

        # Section 1: Market banner
        html.Div(id="intel-banner", style={"marginBottom": "16px"}),

        # Section 2: Predictions
        html.Div(id="intel-prediction", style={"marginBottom": "16px"}),

        # Section 3: News feed (3 columns)
        html.Div([
            html.Div(id="intel-intl",   style={"flex": "1", "minWidth": "280px"}),
            html.Div(id="intel-india",  style={"flex": "1", "minWidth": "280px"}),
            html.Div(id="intel-reddit", style={"flex": "1", "minWidth": "280px"}),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "16px", "flexWrap": "wrap"}),

        # Section 4: Sector heatmap + FII chart
        html.Div([
            html.Div(id="intel-sectors", style={"flex": "1"}),
            html.Div(id="intel-fii",     style={"flex": "1"}),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "16px", "flexWrap": "wrap"}),

        # Section 5: Economic calendar
        html.Div(id="intel-calendar"),

    ], style={"background": BG, "color": TEXT, "fontFamily": FONT,
              "padding": "16px", "minHeight": "100vh"})

    # ── Callbacks ─────────────────────────────────────────────────────────────

    @app.callback(Output("intel-banner","children"), Input("t30","n_intervals"))
    def _banner(_):
        try:
            data = aggregator.get("market_banner", {})
            sent = aggregator.get("sentiment", {})
            pred = aggregator.get("prediction", {})
            items = []
            for name, info in data.items():
                chg   = info.get("chg", 0)
                color = GREEN if chg >= 0 else RED
                items.append(html.Div([
                    html.Div(name, style={"color": DIM, "fontSize": "11px"}),
                    html.Div(f"{info.get('price','--'):,}", style={"fontSize": "18px","fontWeight":"700","color":TEXT}),
                    html.Div(f"{'+' if chg>=0 else ''}{chg:.2f}%", style={"color":color,"fontSize":"12px"}),
                ], style={"textAlign":"center","padding":"8px 16px","borderRight":f"1px solid {BORDER}"}))

            # Sentiment gauge
            sc  = float(sent.get("score", 0))
            sc_color = _SENT_CLR(sc)
            sc_label = sent.get("label", "NEUTRAL")
            items.append(html.Div([
                html.Div("SENTIMENT", style={"color":DIM,"fontSize":"11px"}),
                html.Div(f"{sc:+.2f}", style={"fontSize":"18px","fontWeight":"700","color":sc_color}),
                html.Div(sc_label, style={"color":sc_color,"fontSize":"12px"}),
            ], style={"textAlign":"center","padding":"8px 16px","borderRight":f"1px solid {BORDER}"}))

            # Prediction pill
            st = pred.get("short_term", {})
            lt = pred.get("long_term", {})
            if st and lt:
                items.append(html.Div([
                    html.Div("PREDICTION", style={"color":DIM,"fontSize":"11px"}),
                    html.Div(f"ST: {st.get('label','--')}", style={"fontSize":"12px","color": GREEN if st.get('score',0)>0 else RED, "marginTop":"4px"}),
                    html.Div(f"LT: {lt.get('label','--')}", style={"fontSize":"12px","color": GREEN if lt.get('score',0)>0 else RED}),
                ], style={"textAlign":"center","padding":"8px 16px"}))

            return html.Div(items, style={
                "display":"flex","flexWrap":"wrap","alignItems":"center",
                "background":CARD,"border":f"1px solid {BORDER}",
                "borderRadius":"8px","marginBottom":"12px",
            })
        except Exception as e:
            return html.Div(f"Banner error: {e}", style={"color":RED})

    @app.callback(Output("intel-prediction","children"), Input("t900","n_intervals"))
    def _prediction(_):
        try:
            pred = aggregator.get("prediction", {})
            if not pred:
                return _card(html.Div("Prediction engine warming up... (refreshes every 15 min)",
                                      style={"color":DIM}), title="Market Predictions")
            def _pred_panel(data: dict) -> html.Div:
                score = data.get("score", 0)
                sc    = GREEN if score > 0.1 else RED if score < -0.1 else DIM
                rng   = data.get("nifty_range", {})
                return html.Div([
                    html.Div([
                        html.Span(f"Score: {score:+.3f}  ", style={"color":sc,"fontWeight":"700"}),
                        html.Span(data.get("label",""), style={"color":sc,"fontWeight":"700","fontSize":"14px"}),
                    ]),
                    html.Div(f"Horizon: {data.get('horizon','')}", style={"color":DIM,"fontSize":"12px","marginTop":"4px"}),
                    html.Div(f"Nifty Range: {rng.get('support',0):,} - {rng.get('resistance',0):,}",
                             style={"color":YELLOW,"fontSize":"13px","marginTop":"6px"}),
                    html.Div("Key Catalysts:", style={"color":DIM,"fontSize":"11px","marginTop":"8px"}),
                    *[html.Div(f"+ {c}", style={"color":GREEN,"fontSize":"12px"})
                      for c in data.get("key_catalysts",[])[:3]],
                    html.Div("Key Risks:", style={"color":DIM,"fontSize":"11px","marginTop":"6px"}),
                    *[html.Div(f"- {r}", style={"color":RED,"fontSize":"12px"})
                      for r in data.get("key_risks",[])[:3]],
                ], style={"flex":"1","background":BG,"borderRadius":"6px","padding":"12px","margin":"4px"})

            return _card(
                html.Div([
                    _pred_panel(pred.get("short_term", {})),
                    _pred_panel(pred.get("long_term", {})),
                ], style={"display":"flex","gap":"8px","flexWrap":"wrap"}),
                html.Div(f"Generated: {pred.get('generated_at','')[:19]}",
                         style={"color":DIM,"fontSize":"11px","marginTop":"8px"}),
                title="Market Predictions (AI)",
            )
        except Exception as e:
            return html.Div(f"Prediction error: {e}", style={"color":RED})

    @app.callback(Output("intel-intl","children"), Input("t300","n_intervals"))
    def _intl_news(_):
        try:
            items = aggregator.get("intl_news", [])[:12]
            rows  = []
            for it in items:
                s = it.get("sentiment", {})
                sc = s.get("score", 0)
                imp = s.get("impact_level", "LOW")
                rows.append(html.Div([
                    html.Div([
                        _badge(imp, _IMPACT_CLR.get(imp, DIM)),
                        _badge(it.get("source",""), ACCENT),
                        *[_badge(sec, AMBER) for sec in s.get("affected_sectors",[])[:2]],
                    ], style={"marginBottom":"4px"}),
                    html.A(it.get("title",""), href=it.get("link","#"), target="_blank",
                           style={"color":TEXT,"fontSize":"13px","fontWeight":"600",
                                  "textDecoration":"none","lineHeight":"1.4"}),
                    html.Div([
                        html.Span(f"{sc:+.2f}  ", style={"color":_SENT_CLR(sc),"fontSize":"11px"}),
                        html.Span(it.get("published","")[:10], style={"color":DIM,"fontSize":"11px"}),
                    ], style={"marginTop":"4px"}),
                ], style={"padding":"10px 0","borderBottom":f"1px solid {BORDER}"}))
            return _card(*rows, title="International News")
        except Exception as e:
            return _card(html.Div(f"Error: {e}", style={"color":RED}), title="International News")

    @app.callback(Output("intel-india","children"), Input("t300","n_intervals"))
    def _india_news(_):
        try:
            items = aggregator.get("india_news", [])[:12]
            rows  = []
            for it in items:
                s  = it.get("sentiment", {})
                sc = s.get("score", 0)
                rows.append(html.Div([
                    html.Div([
                        _badge(it.get("source",""), ACCENT),
                        *[_badge(sec, AMBER) for sec in s.get("affected_sectors",[])[:2]],
                    ], style={"marginBottom":"4px"}),
                    html.A(it.get("title",""), href=it.get("link","#"), target="_blank",
                           style={"color":TEXT,"fontSize":"13px","fontWeight":"600",
                                  "textDecoration":"none","lineHeight":"1.4"}),
                    html.Div([
                        html.Span(f"{sc:+.2f}  ", style={"color":_SENT_CLR(sc),"fontSize":"11px"}),
                        html.Span(it.get("published","")[:10], style={"color":DIM,"fontSize":"11px"}),
                    ], style={"marginTop":"4px"}),
                ], style={"padding":"10px 0","borderBottom":f"1px solid {BORDER}"}))
            return _card(*rows, title="India Market News")
        except Exception as e:
            return _card(html.Div(f"Error: {e}", style={"color":RED}), title="India Market News")

    @app.callback(Output("intel-reddit","children"), Input("t300","n_intervals"))
    def _reddit(_):
        try:
            posts = aggregator.get("reddit_posts", [])[:10]
            sent  = aggregator.get("sentiment", {})
            sc    = float(sent.get("score", 0))
            rows  = [
                html.Div([
                    html.Span("Retail Sentiment Index: ", style={"color":DIM,"fontSize":"12px"}),
                    html.Span(f"{sc:+.2f}  {sent.get('label','')}",
                              style={"color":_SENT_CLR(sc),"fontWeight":"700","fontSize":"13px"}),
                    html.Span(f"  ({sent.get('n_samples',0)} articles)",
                              style={"color":DIM,"fontSize":"11px"}),
                ], style={"marginBottom":"12px"}),
            ]
            for p in posts:
                ps = p.get("sentiment", {})
                psc = ps.get("score", 0)
                rows.append(html.Div([
                    html.Div([
                        _badge(p.get("subreddit",""), "#6e40c9"),
                        html.Span(f"  {p.get('score',0):,} pts  {p.get('comments',0)} comments",
                                  style={"color":DIM,"fontSize":"11px"}),
                    ]),
                    html.A(p.get("title",""), href=p.get("url","#"), target="_blank",
                           style={"color":TEXT,"fontSize":"12px","textDecoration":"none","lineHeight":"1.4"}),
                    html.Div(f"Sentiment: {psc:+.2f}",
                             style={"color":_SENT_CLR(psc),"fontSize":"11px","marginTop":"2px"}),
                ], style={"padding":"8px 0","borderBottom":f"1px solid {BORDER}"}))
            return _card(*rows, title="Social Media Pulse (Reddit)")
        except Exception as e:
            return _card(html.Div(f"Error: {e}", style={"color":RED}), title="Social Media Pulse")

    @app.callback(Output("intel-sectors","children"), Input("t300","n_intervals"))
    def _sector_heatmap(_):
        try:
            if _sa is None:
                return _card(html.Div("Sector heatmap requires vaderSentiment",
                                      style={"color":DIM}), title="Sector Heatmap")
            india  = aggregator.get("india_news", [])
            intl   = aggregator.get("intl_news",  [])
            all_it = india + intl
            from sentiment_analyzer import IndianMarketSentimentAnalyzer
            sectors = list(IndianMarketSentimentAnalyzer.SECTOR_KEYWORDS.keys())
            scores: Dict[str, List[float]] = {s: [] for s in sectors}
            for it in all_it:
                sent = it.get("sentiment", {})
                sc   = sent.get("score", 0)
                for sec in sent.get("affected_sectors", []):
                    if sec in scores:
                        scores[sec].append(sc)
            cells = []
            for sec in sectors:
                avg = sum(scores[sec]) / len(scores[sec]) if scores[sec] else 0
                n   = len(scores[sec])
                color = GREEN if avg > 0.1 else RED if avg < -0.1 else DIM
                bg    = (GREEN if avg > 0.1 else RED if avg < -0.1 else BORDER) + "33"
                cells.append(html.Div([
                    html.Div(sec, style={"fontWeight":"600","fontSize":"12px","color":TEXT}),
                    html.Div(f"{avg:+.2f}", style={"color":color,"fontWeight":"700","fontSize":"18px"}),
                    html.Div(f"{n} articles", style={"color":DIM,"fontSize":"11px"}),
                ], style={"background":bg,"border":f"1px solid {color}44","borderRadius":"6px",
                          "padding":"10px","textAlign":"center"}))
            return _card(
                html.Div(cells, style={"display":"grid","gridTemplateColumns":"repeat(3,1fr)","gap":"8px"}),
                title="Sector News Sentiment Heatmap",
            )
        except Exception as e:
            return _card(html.Div(f"Error: {e}", style={"color":RED}), title="Sector Heatmap")

    @app.callback(Output("intel-fii","children"), Input("t300","n_intervals"))
    def _fii_panel(_):
        try:
            import plotly.graph_objects as go
            fii = aggregator.get("fii_data", {})
            if not fii:
                return _card(html.Div("Fetching FII/DII data...", style={"color":DIM}),
                             title="FII / DII Net Flows")
            dates   = [fii[i].get("date","") for i in sorted(fii)]
            fii_net = [fii[i].get("fii_net",0) for i in sorted(fii)]
            dii_net = [fii[i].get("dii_net",0) for i in sorted(fii)]
            fig = go.Figure()
            fig.add_bar(x=dates, y=fii_net, name="FII Net",
                        marker_color=[GREEN if v>=0 else RED for v in fii_net])
            fig.add_bar(x=dates, y=dii_net, name="DII Net",
                        marker_color=[AMBER if v>=0 else "#8b0000" for v in dii_net])
            fig.update_layout(
                paper_bgcolor=CARD, plot_bgcolor=BG, font_color=TEXT,
                barmode="group", height=220,
                xaxis=dict(color=DIM, gridcolor=BORDER),
                yaxis=dict(title="Rs Cr", color=DIM, gridcolor=BORDER),
                legend=dict(bgcolor=CARD, bordercolor=BORDER),
                margin=dict(l=50,r=10,t=10,b=40),
            )
            from dash import dcc as _dcc
            return _card(_dcc.Graph(figure=fig, config={"displayModeBar":False}),
                        title="FII / DII Net Flows (Rs Cr)")
        except Exception as e:
            return _card(html.Div(f"Error: {e}", style={"color":RED}), title="FII / DII")

    @app.callback(Output("intel-calendar","children"), Input("t900","n_intervals"))
    def _calendar(_):
        try:
            now = datetime.now(IST).date()
            upcoming = [
                e for e in ECONOMIC_CALENDAR
                if datetime.strptime(e["date"],"%Y-%m-%d").date() >= now
            ][:8]
            rows = []
            for ev in upcoming:
                d   = datetime.strptime(ev["date"],"%Y-%m-%d")
                imp = ev.get("impact","MEDIUM")
                rows.append(html.Div([
                    html.Div(d.strftime("%d %b"), style={"color":YELLOW,"fontWeight":"700",
                                                          "fontSize":"14px","minWidth":"60px"}),
                    html.Div([
                        _badge(imp, _IMPACT_CLR.get(imp, DIM)),
                        _badge(ev.get("type",""), ACCENT),
                    ]),
                    html.Div(ev.get("event",""), style={"color":TEXT,"fontSize":"13px"}),
                ], style={"display":"flex","gap":"12px","alignItems":"center",
                          "padding":"8px 0","borderBottom":f"1px solid {BORDER}"}))
            return _card(*rows, title="Economic Calendar (Upcoming)")
        except Exception as e:
            return _card(html.Div(f"Error: {e}", style={"color":RED}), title="Economic Calendar")

    return app


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════════

_AGGREGATOR: Optional[NewsAggregator] = None


def get_aggregator() -> NewsAggregator:
    global _AGGREGATOR
    if _AGGREGATOR is None:
        _AGGREGATOR = NewsAggregator()
        _AGGREGATOR.start()
    return _AGGREGATOR


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [news] %(levelname)s %(message)s")
    agg = get_aggregator()
    app = build_news_app(agg)
    log.info("News Intelligence Dashboard starting on port %d", NEWS_PORT)
    app.run(host="0.0.0.0", port=NEWS_PORT, debug=False)
