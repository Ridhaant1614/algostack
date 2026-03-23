# AlgoStack Always-On Deployment (Docker VPS)

This mode removes free tunnel dependency and serves the dashboard directly from a VPS public IP/domain.

## 1) Pick a host (free/low-cost)

- Oracle Cloud Always Free VM (recommended for always-on)
- Any Linux VPS where Docker is allowed

## 2) Server setup

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```

## 3) Upload project

Copy this project folder to the VPS, then run:

```bash
cd algostack
docker compose up -d --build
```

## 4) Open firewall

Allow inbound TCP `8055` on the VPS/security-group.

Dashboard URL:

```text
http://<VPS_PUBLIC_IP>:8055
```

## 5) Optional fixed domain

Point DNS A record to VPS IP and use reverse proxy (Nginx/Caddy) for HTTPS.

## 6) Basic operations

```bash
docker compose ps
docker compose logs -f algostack
docker compose restart algostack
docker compose down
```

## Notes

- `docker-compose.yml` persists logs/results via bind mounts.
- In this mode Cloudflare/ngrok/localtunnel are intentionally disabled.
- Set `PUBLIC_LINK_PASSWORD` through env if you want a custom dashboard password.
- Default process profile is `render-full` (all sections/subsections enabled).
