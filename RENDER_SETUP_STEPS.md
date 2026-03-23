# AlgoStack Render Setup (Do This Exactly)

## Why the site showed 502 / blank page

Render **free** instances have about **512 MB RAM**. Running **all 16 processes** (`render-full`) uses more than that → Render kills the container (**out of memory**) → **HTTP 502**.

**Default is now `render-lite`** (fewer processes) so the site stays up. You still get: Equity + Dash + 1 equity scanner + XOptimizer + BestX + CommodityEngine + 1 comm scanner + CryptoEngine + 1 crypto scanner + AlertMonitor.

To run **every** scanner process, use a **paid Render plan with more RAM** or a **VPS** (see `DEPLOY_ALWAYS_ON.md`), then set `AUTOHEALER_PROFILE=render-full`.

---

## 1) Push latest code to GitHub

```powershell
git add .
git commit -m "Render: lite default, fast PORT bind, skip WiFi on cloud"
git push
```

## 2) Open your Render service

1. [dashboard.render.com](https://dashboard.render.com) → service **algostack** → **Settings**

## 3) Start Command

Use:

`python -X utf8 -u autohealer.py --profile render-lite`

(Or leave blank if `render.yaml` / Dockerfile sets it.)

**Do not** use only `unified_dash_v3.py` — that is UI-only (no live engines).

## 4) Environment variables

| Variable | Value (free tier) |
|----------|-------------------|
| `AUTOHEALER_PROFILE` | `render-lite` |
| `DISABLE_WIFI_KEEPALIVE` | `1` |
| `DISABLE_AFFINITY` | `1` |
| `FORCE_JSON_IPC` | `1` |
| `DISABLE_PUBLIC_TUNNEL` | `1` |
| `DISABLE_CLOUDFLARE` | `1` |
| `DISABLE_PYNGROK` | `1` |
| `TUNNEL_STABLE_MODE` | `1` |
| `PUBLIC_BASE_URL` | `https://algostack.onrender.com` |
| `PUBLIC_LINK_PASSWORD` | `Ridz@2004` |
| `TZ` | `Asia/Kolkata` |

Remove any manual **`UNIFIED_DASH_PORT`** override — the app uses Render’s **`PORT`** automatically.

Optional: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, …

## 5) Deploy

**Manual Deploy** → **Deploy latest commit** → open `https://algostack.onrender.com`

## 6) Verify in logs

You should see:

- `Starting processes (RENDER-LITE)` (or `LITE`)
- `UnifiedDash` starts **first** (fast bind to `$PORT`)
- `WiFi keepalive skipped (hosted …)`
- **No** `Ran out of memory (used over 512MB)`

If you still see OOM, remove extra env or reduce processes further.

## 7) Full 16-process stack on Render

Only if your instance has **enough RAM** (paid tier):

- Start command: `python -X utf8 -u autohealer.py --profile render-full`
- `AUTOHEALER_PROFILE=render-full`

---

## Important

- Data on `algostack.onrender.com` comes **only** from processes running **on Render**, not from your laptop.
