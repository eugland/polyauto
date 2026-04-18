# Nginx Gateway (Linux)

Static homepage + reverse proxy routes served by native Nginx on port `5051`.

## One-command deploy

From repo root:

```bash
chmod +x nginx/deploy.sh
./nginx/deploy.sh
```

## Edit points

- `html/apps.json`: cards shown on homepage
- `conf/local-gateway.conf`: real proxy routing (`location` + `proxy_pass`)

Keep these in sync when you add/remove services.

After route changes:

```bash
sudo nginx -t && sudo systemctl reload nginx
```
