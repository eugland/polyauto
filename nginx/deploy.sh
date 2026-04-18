#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

sudo mkdir -p /var/www/local-gateway

if command -v rsync >/dev/null 2>&1; then
  sudo rsync -a --delete "$REPO_DIR/html/" /var/www/local-gateway/
else
  sudo rm -rf /var/www/local-gateway/*
  sudo cp -r "$REPO_DIR/html/"* /var/www/local-gateway/
fi

sudo cp "$REPO_DIR/conf/local-gateway.conf" /etc/nginx/sites-available/local-gateway.conf
sudo ln -sf /etc/nginx/sites-available/local-gateway.conf /etc/nginx/sites-enabled/local-gateway.conf
sudo rm -f /etc/nginx/sites-enabled/default

sudo nginx -t
sudo systemctl enable nginx >/dev/null 2>&1 || true
sudo systemctl reload nginx

echo "Deployed: http://$(hostname -I | awk '{print $1}'):5051/"
