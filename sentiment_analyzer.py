# Author: Ridhaant Ajoy Thackur
"""
sentiment_analyzer.py -- Indian Market Sentiment Analysis (AlgoStack v3.0)
==========================================================================
VADER sentiment + Indian market-specific keyword boosters.
Returns scores in [-1.0 (very bearish) to +1.0 (very bullish)].

Used by news_dashboard.py and trend_predictor.py.
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional

log = logging.getLogger("sentiment_analyzer")

# Try VADER -- gracefully degrade if not installed
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _VADER = SentimentIntensityAnalyzer()
    _VADER_OK = True
except ImportError:
    _VADER = None
    _VADER_OK = False
    log.warning("vaderSentiment not installed. pip install vaderSentiment. Using keyword-only mode.")


class IndianMarketSentimentAnalyzer:
    """
    VADER + Indian market-specific keyword boosters for NSE/BSE news.
    Scores: +1.0 = very bullish, -1.0 = very bearish, 0 = neutral.
    """

    # Indian market positive (bullish) keywords
    BULLISH_KEYWORDS = {
        "buyback", "dividend", "bonus share", "results beat", "strong quarter",
        "all-time high", "record profit", "fii buying", "dii buying",
        "rate cut", "stimulus", "capex boost", "pli scheme", "infrastructure push",
        "order win", "new contract", "expansion", "acquisition", "merger approved",
        "rbi support", "government boost", "tax cut", "ease regulations",
        "monsoon normal", "gst collection high", "exports rise", "rupee stable",
        "upgrade", "outperform", "buy rating", "target price raised",
        "strong guidance", "beat estimates", "revenue growth", "margin expansion",
        "debt reduction", "credit upgrade", "overweight", "strong buy",
    }

    # Indian market negative (bearish) keywords
    BEARISH_KEYWORDS = {
        "profit warning", "downgrade", "fii selling", "margin pressure",
        "rate hike", "inflation high", "npa rise", "bank stress", "bad loans",
        "slowdown", "recession fears", "global selloff", "circuit breaker",
        "sebi probe", "ed raid", "cbi investigation", "promoter pledge",
        "debt default", "ncd default", "credit downgrade", "rating cut",
        "monsoon deficit", "gst miss", "trade deficit", "rupee fall",
        "sanctions", "tariff hike", "supply chain disruption",
        "miss estimates", "weak guidance", "below expectations",
        "losses widened", "revenue decline", "cash crunch", "insolvency",
        "underweight", "sell rating", "target price cut", "reduce",
        "market cap loss", "pledge invocation", "halt production",
    }

    # Sector classification keywords
    SECTOR_KEYWORDS: Dict[str, List[str]] = {
        "Banking":  ["bank", "nbfc", "rbi", "repo rate", "credit", "npa", "loan",
                     "hdfc", "sbi", "icici", "kotak", "yes bank", "banking system",
                     "deposit", "emi", "interest rate india"],
        "IT":       ["it sector", "technology", "software", "infosys", "tcs", "wipro",
                     "hcl", "tech mahindra", "dollar", "us gdp", "us recession",
                     "layoffs", "it spending", "cloud", "digital"],
        "Pharma":   ["pharma", "drug", "usfda", "fda warning", "api", "cipla",
                     "sunpharma", "drreddy", "biocon", "clinical trial",
                     "generic drug", "patent", "healthcare"],
        "Auto":     ["auto", "ev", "electric vehicle", "tatamotors", "maruti",
                     "bajaj", "hero", "oil price", "vehicle sales", "two-wheeler",
                     "tractor sales", "automobile"],
        "Energy":   ["crude oil", "bpcl", "reliance", "ongc", "opec",
                     "energy", "petroleum", "natural gas", "power", "solar",
                     "renewable", "coal india"],
        "Metals":   ["steel", "metals", "vedanta", "tata steel", "jsw",
                     "coal", "iron ore", "china demand", "aluminium", "copper",
                     "zinc", "nickel"],
        "FMCG":     ["fmcg", "consumer goods", "hul", "itc", "britannia",
                     "inflation", "rural demand", "urban consumption",
                     "dabur", "nestle", "godrej"],
        "Realty":   ["real estate", "realty", "dlf", "housing",
                     "home loan", "mortgage", "construction", "cement",
                     "property prices"],
        "Indices":  ["nifty", "sensex", "banknifty", "nifty50", "nifty500",
                     "market breadth", "advance decline", "fii flows",
                     "dii flows", "ipo"],
        "Telecom":  ["telecom", "airtel", "jio", "vi", "5g", "spectrum",
                     "arpu", "subscriber"],
        "Aviation": ["aviation", "airline", "indigo", "air india", "spicejet",
                     "jet fuel", "load factor"],
    }

    # High-impact keywords that amplify scores
    HIGH_IMPACT_WORDS = {
        "breaking", "urgent", "alert", "crash", "rally", "surge",
        "plunge", "soar", "collapse", "record", "historic",
    }

    def analyze(self, text: str) -> dict:
        """
        Analyze text and return sentiment dict.

        Returns:
            {
                "score":            float in [-1.0, 1.0],
                "label":            "BULLISH" | "BEARISH" | "NEUTRAL",
                "confidence":       float in [0, 1],
                "affected_sectors": List[str],
                "bullish_triggers": List[str],
                "bearish_triggers": List[str],
                "impact_level":     "HIGH" | "MEDIUM" | "LOW",
            }
        """
        if not text:
            return self._neutral()

        text_lower = text.lower()

        # Base score from VADER
        if _VADER_OK:
            vader_score = _VADER.polarity_scores(text)["compound"]
        else:
            vader_score = 0.0

        # Count Indian market keyword hits
        bullish_hits = [k for k in self.BULLISH_KEYWORDS if k in text_lower]
        bearish_hits = [k for k in self.BEARISH_KEYWORDS if k in text_lower]

        # Keyword boost: ±5% per keyword, capped at ±0.5
        boost = max(-0.5, min(0.5, (len(bullish_hits) - len(bearish_hits)) * 0.05))

        # High-impact amplifier: multiply by 1.2 if breaking/urgent/etc.
        amplifier = 1.2 if any(w in text_lower for w in self.HIGH_IMPACT_WORDS) else 1.0

        final_score = max(-1.0, min(1.0, (vader_score + boost) * amplifier))

        # Sector classification
        affected = [
            sector for sector, kws in self.SECTOR_KEYWORDS.items()
            if any(kw in text_lower for kw in kws)
        ]

        # Impact level
        if any(w in text_lower for w in self.HIGH_IMPACT_WORDS) or abs(final_score) > 0.5:
            impact = "HIGH"
        elif abs(final_score) > 0.2 or len(bullish_hits) + len(bearish_hits) >= 2:
            impact = "MEDIUM"
        else:
            impact = "LOW"

        return {
            "score":            round(final_score, 3),
            "label":            "BULLISH" if final_score > 0.1 else
                                "BEARISH" if final_score < -0.1 else "NEUTRAL",
            "confidence":       round(abs(final_score), 3),
            "affected_sectors": affected,
            "bullish_triggers": bullish_hits[:5],
            "bearish_triggers": bearish_hits[:5],
            "impact_level":     impact,
        }

    def analyze_batch(self, texts: List[str]) -> List[dict]:
        return [self.analyze(t) for t in texts]

    def aggregate(self, results: List[dict]) -> dict:
        """Aggregate multiple sentiment results into a composite score."""
        if not results:
            return self._neutral()
        scores = [r["score"] for r in results if "score" in r]
        if not scores:
            return self._neutral()
        avg = sum(scores) / len(scores)
        # Weight recent items more heavily (last 20%)
        if len(scores) >= 5:
            recent = scores[-max(1, len(scores) // 5):]
            avg = avg * 0.7 + (sum(recent) / len(recent)) * 0.3
        all_sectors: list = []
        for r in results:
            all_sectors.extend(r.get("affected_sectors", []))
        sector_counts: Dict[str, int] = {}
        for s in all_sectors:
            sector_counts[s] = sector_counts.get(s, 0) + 1
        top_sectors = sorted(sector_counts, key=lambda x: -sector_counts[x])[:5]
        return {
            "score":     round(max(-1.0, min(1.0, avg)), 3),
            "label":     "BULLISH" if avg > 0.1 else "BEARISH" if avg < -0.1 else "NEUTRAL",
            "n_samples": len(scores),
            "top_sectors": top_sectors,
        }

    @staticmethod
    def _neutral() -> dict:
        return {
            "score": 0.0, "label": "NEUTRAL", "confidence": 0.0,
            "affected_sectors": [], "bullish_triggers": [], "bearish_triggers": [],
            "impact_level": "LOW",
        }


# Module-level singleton
analyzer = IndianMarketSentimentAnalyzer()
