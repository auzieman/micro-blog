# IONOS public preview deployment

Status: live preview pattern

Date: 2026-07-28

## Intent

Run the micro-blog public preview on the IONOS primary host while leaving the
current production `auzietek.com` and `www.auzietek.com` Drupal site untouched.

Public preview hostnames:

```text
beta.auzietek.com
blackknight.auzietek.com
linux-users.auzietek.com
retro-users.auzietek.com
```

All four names point at the IONOS primary host and route through nginx to the
same micro-blog UI. The app selects the lane from the requested hostname.

## Preserved services

These services are intentionally retained during cleanup and preview work:

```text
Auzietek Drupal
Grafana
Prometheus
Kanboard
micro-blog beta preview
```

Retired from the IONOS nodes:

```text
DTLabs / Gogs stack, after backup
old cafe / Kyburz Drupal-era trees, moved to archive
Ollama / Open WebUI VPS experiment, removed to reclaim disk
```

`dtlab.auzietek.com` now redirects to the GitHub profile as a decommissioned
endpoint rather than proxying to a dead Gogs service.

## VPS compose shape

The checked-in `docker-compose.yml` is still useful for local development.
The IONOS preview deployment should keep only the UI exposed to the host:

```text
blog-ui  -> 127.0.0.1:18081
nginx    -> https public hostnames -> 127.0.0.1:18081
```

The following services should remain internal to the Docker network on the VPS:

```text
blog-api
postgres
redis
rabbitmq
otel-collector
```

This keeps the public surface small: HTTPS nginx in front, app UI behind it,
and stateful/runtime services private.

## Proxy behavior

The Flask UI uses `ProxyFix` so canonical URLs and redirects respect nginx
headers such as:

```text
X-Forwarded-Proto
X-Forwarded-Host
X-Forwarded-For
```

This is required so public canonical links render as `https://...` instead of
internal or proxied `http://...` URLs.

## DNS behavior

IONOS DNS writes should use the BKC DNS helper and the secret stored outside
Git. The safe command style is:

```sh
python3 ionos_dns.py --api-key-file /root/.secrets/ionos-api-key \
  ensure-record --name blackknight.auzietek.com --type A \
  --content 74.208.45.165 --ttl 300
```

Avoid hand-building the DNS API request body. The helper already handles the
IONOS record-create array payload requirement.

## Cutover boundary

Do not point `auzietek.com` or `www.auzietek.com` at the micro-blog deployment
until content review, admin security review, backup/rollback, and certificate
validation are complete.

