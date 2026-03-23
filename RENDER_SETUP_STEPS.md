# AlgoStack Render Setup (Do This Exactly)

Follow these steps in order. This will make Render run all AlgoStack sections/subsections with live data.

## 1) Push latest code to GitHub

Run in project folder:

```powershell
git add .
git commit -m "Render full profile setup"
git push
```

If there are no new changes, Git may say nothing to commit. That is fine.

## 2) Open your Render service

1. Go to [https://dashboard.render.com](https://dashboard.render.com)
2. Open service: `algostack`
3. Go to **Settings**

## 3) Set Start Command

In Render Settings, set:

`python -X utf8 -u autohealer.py --profile render-full`

If you use Blueprint (`render.yaml`), this may already be auto-set.

## 4) Add/Update Environment Variables

Go to **Environment** and set these exactly:

- `AUTOHEALER_PROFILE=render-full`
- `DISABLE_AFFINITY=1`
- `FORCE_JSON_IPC=1`
- `DISABLE_PUBLIC_TUNNEL=1`
- `DISABLE_CLOUDFLARE=1`
- `DISABLE_PYNGROK=1`
- `TUNNEL_STABLE_MODE=1`
- `PUBLIC_BASE_URL=https://algostack.onrender.com`
- `PUBLIC_LINK_PASSWORD=Ridz@2004`
- `TZ=Asia/Kolkata`

Optional Telegram vars (if you want alerts):

- `TELEGRAM_BOT_TOKEN=...`
- `TELEGRAM_CHAT_ID=...`

## 5) Deploy

1. Click **Manual Deploy** -> **Deploy latest commit**
2. Wait until deploy finishes
3. Open: [https://algostack.onrender.com](https://algostack.onrender.com)

## 6) Verify in Render logs

Open **Logs** and confirm you see:

- `Starting processes (RENDER-FULL)`
- multiple process starts (`Algofinal`, `UnifiedDash`, scanners, `XOptimizer`, `BestXTrader`, `CommodityEngine`, `CryptoEngine`, `AlertMonitor`)

If you do not see `RENDER-FULL`, the start command or env vars are wrong.

## 7) If memory issue happens on free plan

Temporarily switch to lite mode:

- Set `AUTOHEALER_PROFILE=render-lite`
- Redeploy

When stable, switch back to `render-full`.

## Important

Use one source only:

- If you open `algostack.onrender.com`, data must come from Render processes.
- Do not mix local autohealer output with Render dashboard expectations.
