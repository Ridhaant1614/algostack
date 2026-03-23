# Deploy on Render (No Credit Card Path)

This gives you a stable public URL without local tunnel dependency.

## 1) Push latest code to GitHub

Repository example: `Ridhaant1614/algostack`

## 2) Create Render service

1. Go to Render Dashboard
2. Click **New +** -> **Web Service**
3. Connect GitHub and choose `Ridhaant1614/algostack`
4. Render auto-detects Docker (`Dockerfile` present)
5. Choose **Free** plan
6. Deploy

Use this start command (free tier — avoids OOM / 502):

`python -X utf8 -u autohealer.py --profile render-lite`

For **all 16 processes**, you need more RAM (paid Render or VPS):

`python -X utf8 -u autohealer.py --profile render-full`

## 3) Required environment variables in Render

Add these in Render -> Service -> Environment:

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

Optional:

- `TELEGRAM_BOT_TOKEN=...`
- `TELEGRAM_CHAT_ID=...`

## 4) Open your Render URL

Render gives URL like:

`https://<service-name>.onrender.com`

Use that URL as your public dashboard link.

## Notes

- Free plan may sleep when idle (cold start on first request).
- This still avoids local tunnel 503/1033 failures.
- **512 MB free tier**: use `render-lite` or the instance will OOM → **502**.
- **Full stack** (`render-full`): use a larger instance or VPS.
- The app is configured to bind Render's dynamic `PORT`.
- Do not mix modes: if you open Render URL, data must be produced by Render processes (not your local laptop). Keep one active source of truth.
