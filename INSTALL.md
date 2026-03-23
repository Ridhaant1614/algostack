# AlgoStack v10.2 — Installation & Setup Guide
**Author: Ridhaant Ajoy Thackur**

---

## System Requirements
| Component | Minimum | Your Machine |
|-----------|---------|-------------|
| CPU | 4 cores | i5-12450H (8C/12T) ✅ |
| RAM | 8 GB | 16 GB ✅ |
| GPU | Optional | GTX 1650 4GB ✅ |
| Python | 3.10+ | 3.12 recommended |
| OS | Windows 10+ / Linux | Windows 11 ✅ |

---

## Quick Start (3 steps)

### Step 1: Install Python dependencies
```batch
cd algostack_v10_2
pip install -r requirements.txt
```

### Step 2: Enable GPU acceleration (optional but recommended)
Your GTX 1650 supports CUDA 12. To enable 15-30× faster scanners:
```batch
pip install cupy-cuda12x
```
If cupy install fails, Numba JIT is used automatically (5-10× faster than NumPy).

### Step 3: Start everything
```batch
python autohealer.py
```

---

## Performance Optimisations (v10.2)

### Calculation Capacity
| Scanner | Symbols | X-values | Calcs/tick |
|---------|---------|----------|-----------|
| S1 Narrow | 38 equity | 1,000 | 38,000 |
| S2 Dual | 38 equity | 16,000 | 608,000 |
| S3 Wide | 38 equity | 32,000 | 1,216,000 |
| CommScanners | 5 MCX | 49,000 | 245,000 |
| CryptoScanners | 5 Binance | 49,000 | 245,000 |
| **TOTAL** | | **147,000** | **2,352,000/tick** |

### CPU Core Allocation (i5-12450H)
| Core | Process |
|------|---------|
| 0-1 | Algofinal (equity engine) |
| 1 | UnifiedDash (:8055) |
| 2 | Scanner1 (Narrow 1K) |
| 3 | Scanner2 (Dual 16K) |
| 4 | Scanner3 (Wide 32K) ← GPU accelerated |
| 5 | CommodityEngine + CommScanners |
| 6 | CryptoEngine + CryptoScanners |
| 7 | XOptimizer + BestXTrader |
| 8 | price_service |
| 9 | autohealer/watchdog |

### GPU Strategy (GTX 1650)
- Scanner3 uses CuPy for the 32K×38 = 1.2M calculation batch
- Each tick processed in <1ms on GPU vs ~50ms on CPU
- VRAM used: ~11 MB / 4096 MB (0.3%)

---

## Configuration (.env file)
Copy `.env.template` to `.env` and fill in:
```
GEMINI_API_KEY=AIzaSyB9fJ1geyar2gtgs-HYsVTOKEGwmNt_r08
CURRENT_X_MULTIPLIER=0.008575
CAPITAL_PER_TRADE=100000
```

---

## Dashboard Access
After starting autohealer.py:
- **Local**: http://localhost:8055
- **Network**: http://[your-ip]:8055
- **Public**: URL sent to Telegram on startup (ngrok tunnel)

---

## Telegram Alerts Format
All three asset classes (Equity/MCX/Crypto) now use the same format:
```
🚨 SYMBOL [asset] — Entry/Exit at HH:MM:SS

Previous Close / Anchor: X.XX
Current Price: X.XX  (+Y.YY%)
Deviation (X): X.XX
Quantity: N

📈 Buy Levels:
Buy Above: X.XX
Target 1: X.XX  (+Y.YY%)
Target 2: X.XX  (+Y.YY%)
Stop Loss: X.XX  (-Y.YY%)
```

---

## Troubleshooting

**"CommScanners exit code=0 on weekends"** → Normal. MCX is closed.

**"Possibly delisted; no price data found"** → Fixed in v10.2. price_service now uses fast_info with NSE fallback.

**"Rate limit exceeded" in yfinance** → Fixed. Adaptive 3s/30s interval + NSE API fallback.

**Dashboard laggy on mobile** → Fixed. Callbacks consolidated from 41 to 32, CSS mobile-optimised.

**Crypto prices missing** → Check crypto_engine.py is running. Prices now published to live_prices.json via topic-merge (never overwritten by equity prices).

**Old Telegram link received** → Fixed. URL sent exactly once by unified_dash_v3.py on startup. All other sends suppressed.
