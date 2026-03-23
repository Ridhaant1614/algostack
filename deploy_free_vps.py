#!/usr/bin/env python3
"""
One-command free-tier deploy helper for AlgoStack.

This script automates the Docker VPS steps:
1) Upload project to remote host via scp
2) Install Docker + compose plugin
3) Build and start AlgoStack via docker compose
4) Open firewall port 8055 (ufw, if present)
5) Print final dashboard URL

Typical target: Oracle Cloud Always Free VM (Ubuntu).
"""

from __future__ import annotations

import argparse
import os
import pathlib
import shlex
import subprocess
import sys
from typing import Sequence


def run(cmd: Sequence[str], *, check: bool = True) -> int:
    print(">", " ".join(shlex.quote(c) for c in cmd), flush=True)
    proc = subprocess.run(cmd)
    if check and proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc.returncode


def run_ssh(host: str, user: str, key: str | None, remote_cmd: str, check: bool = True) -> int:
    cmd = ["ssh", "-o", "StrictHostKeyChecking=accept-new"]
    if key:
        cmd += ["-i", key]
    cmd += [f"{user}@{host}", remote_cmd]
    return run(cmd, check=check)


def run_scp(host: str, user: str, key: str | None, local_path: str, remote_path: str) -> int:
    cmd = ["scp", "-r", "-o", "StrictHostKeyChecking=accept-new"]
    if key:
        cmd += ["-i", key]
    cmd += [local_path, f"{user}@{host}:{remote_path}"]
    return run(cmd, check=True)


def main() -> int:
    p = argparse.ArgumentParser(description="Deploy AlgoStack to free-tier VPS")
    p.add_argument("--host", required=True, help="VPS public IP or hostname")
    p.add_argument("--user", default="ubuntu", help="SSH username (default: ubuntu)")
    p.add_argument("--key", default="", help="SSH private key path (optional)")
    p.add_argument("--remote-dir", default="~/algostack", help="Remote deploy directory")
    p.add_argument("--skip-upload", action="store_true", help="Skip scp upload step")
    args = p.parse_args()

    project_root = pathlib.Path(__file__).resolve().parent
    key_path = os.path.expanduser(args.key) if args.key else ""

    if key_path and not os.path.exists(key_path):
        print(f"ERROR: SSH key not found: {key_path}", file=sys.stderr)
        return 2

    # 1) Upload code
    if not args.skip_upload:
        print("\n[1/5] Uploading project...")
        run_scp(args.host, args.user, key_path or None, str(project_root), args.remote_dir)
    else:
        print("\n[1/5] Upload skipped (--skip-upload).")

    # 2) Install docker if missing
    print("\n[2/5] Installing Docker (if needed)...")
    install_cmd = (
        "set -e; "
        "if ! command -v docker >/dev/null 2>&1; then "
        "  sudo apt-get update && sudo apt-get install -y docker.io docker-compose-plugin; "
        "  sudo systemctl enable docker; sudo systemctl start docker; "
        "fi; "
        "sudo usermod -aG docker $USER || true"
    )
    run_ssh(args.host, args.user, key_path or None, install_cmd, check=True)

    # 3) Build + start compose
    print("\n[3/5] Building and starting AlgoStack...")
    up_cmd = (
        f"set -e; cd {shlex.quote(args.remote_dir)}; "
        "docker compose up -d --build"
    )
    run_ssh(args.host, args.user, key_path or None, up_cmd, check=True)

    # 4) Open firewall port 8055 (best-effort)
    print("\n[4/5] Opening firewall port 8055 (best-effort)...")
    fw_cmd = (
        "set -e; "
        "if command -v ufw >/dev/null 2>&1; then "
        "  sudo ufw allow 8055/tcp || true; sudo ufw reload || true; "
        "fi"
    )
    run_ssh(args.host, args.user, key_path or None, fw_cmd, check=False)

    # 5) Status + final URL
    print("\n[5/5] Fetching service status...")
    status_cmd = f"cd {shlex.quote(args.remote_dir)} && docker compose ps"
    run_ssh(args.host, args.user, key_path or None, status_cmd, check=False)

    print("\nDeploy complete.")
    print(f"Dashboard URL: http://{args.host}:8055")
    print("Dashboard password: Ridz@2004 (or PUBLIC_LINK_PASSWORD on server)")
    print("\nIf it doesn't open, check VPS security-group/firewall inbound rule for TCP 8055.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
