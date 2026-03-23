# Render memory (why you saw HTTP 502)

| Symptom | Cause |
|--------|--------|
| `Ran out of memory (used over 512MB)` in Render **Events** | Too many Python processes for free tier |
| `HTTP 502` / blank page | Container killed after OOM |
| `No open ports detected` | Nothing listening on `$PORT` yet, or crash before **UnifiedDash** binds |

**Fix used in this repo**

- Default **`render-lite`** on Render (fits ~512 MB).
- **UnifiedDash starts first** with **0 s delay** so `$PORT` opens quickly.
- **WiFi keepalive disabled** on Render (not needed; wastes CPU/RAM).
- **No Rich TUI** in the main process on Render (lighter supervisor loop).

**If you need every scanner (16 processes)**

- Upgrade Render instance RAM, **or**
- Run full stack on a VPS (`DEPLOY_ALWAYS_ON.md` + `AUTOHEALER_PROFILE=full` or `render-full`).
