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
www.blackknightcontroller.com -> redirects to blackknight.auzietek.com
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

Current DNS alignment observed on 2026-07-29:

```text
auzietek.com                         A 74.208.45.165   legacy Drupal/root
www.auzietek.com                     A 74.208.45.165   legacy Drupal/www
beta.auzietek.com                    A 74.208.45.165   micro-blog preview
blackknight.auzietek.com             A 74.208.45.165   micro-blog BlackKnight lane
linux-users.auzietek.com             A 74.208.45.165   micro-blog Linux lane
retro-users.auzietek.com             A 74.208.45.165   micro-blog Retro lane
kb.auzietek.com                      A 74.208.45.165   Kanboard
mon.auzietek.com / mon1.auzietek.com A 74.208.45.165   Grafana/monitoring
dtlab.auzietek.com                   A 74.208.45.165   redirect to GitHub profile

auzietek.lab.auzietek.com            CNAME swarm1.lab.auzietek.com
blackknight.lab.auzietek.com         CNAME swarm1.lab.auzietek.com
linux-users.lab.auzietek.com         CNAME swarm1.lab.auzietek.com
retro-users.lab.auzietek.com         CNAME swarm1.lab.auzietek.com
microblog.lab.auzietek.com           CNAME swarm1.lab.auzietek.com
swarm1.lab.auzietek.com              A 192.168.1.15
```

Potential cleanup found during the DNS pass:

```text
grafnaa.lab.auzietek.com             A 192.168.1.24   likely typo/stale
```

Do not delete the typo record until the monitoring/dashboard links have been
searched. If unused, disable or remove it in a small DNS cleanup pass.

`lab.auzietek.com` is not a separate IONOS zone; lab hostnames currently live as
records inside the `auzietek.com` zone.

## Current nginx alignment

Observed on the IONOS primary host on 2026-07-29:

```text
auzietek.com
www.auzietek.com
  -> legacy Drupal/backend
  -> uses auzietek.com 2026 certificate material

beta.auzietek.com
blackknight.auzietek.com
linux-users.auzietek.com
retro-users.auzietek.com
  -> proxy_pass http://127.0.0.1:18081
  -> uses auzietek.com fullchain/private key 2026 material

dtlab.auzietek.com
  -> 301 https://github.com/auzieman/

blackknightcontroller.com
www.blackknightcontroller.com
  -> 301 https://blackknight.auzietek.com$request_uri
  -> certificate still needs a domain-specific solution

kb.auzietek.com
mon.auzietek.com / mon1.auzietek.com
prom1.auzietek.com
clu.auzietek.com / clu-api.auzietek.com
  -> existing legacy/service routes, not part of the micro-blog preview cutover
```

Verification wrinkle observed on 2026-07-29:

```text
http://www.blackknightcontroller.com/
  -> redirects correctly to https://blackknight.auzietek.com/

https://www.blackknightcontroller.com/
  -> currently lands on the Drupal/default TLS site instead of the redirect
```

Treat this as an SNI/certificate/server-block issue. Do not promote the
`blackknightcontroller.com` domain until HTTPS on both bare and `www` has a
valid certificate and reaches the redirect block.

The nginx config currently references a mix of certificate file names:

```text
auzietek.com_fullchain_2026.pem
auzietek.com_private_key_2026.cer
auzietek.com_private_key_2026.key
auzietek.com_private_key_2025.cer
auzietek.com_ssl_certificate.crt
```

Before production cutover, normalize certificate naming and verify the served
chain with external clients. Some corporate/work SSL filters may be stricter
than Chrome on a home network.

## Current legacy Drupal routes

Useful legacy routes observed during the 2026-07-29 crawl:

```text
/
/node/1                       Welcome
/thinktank                    ThinkTank
/node/8                       External tools
/node/9                       Friends and Partners
/articles                     Articles
/blogs                        Blogs
/node/3                       Hire a human not an algorithm
/node/4                       Covid-19
/node/5                       Pet projects post, Amiga's not dead its just different
/node/11                      Containers article
/index.php/node/33            BlackKnightController introduction
/node/35                      The Dawn of a New Era in Computing
/node/36                      RACS: Building a Brighter Future for All
/node/37                      RACS medical infrastructure article
/index.php/node/43            MuIRC / AmigaOS 4.1 article
/node/44                      RX-Demo observable cloud-native app article
```

Suggested transition:

```text
keep auzietek.com/www on Drupal during beta review
promote selected content into micro-blog lanes
create legacy.auzietek.com or /legacy before final cutover
redirect important old URLs to polished replacements where clear
leave less-important history in the legacy archive
```

Static Auzietek pages in micro-blog should remain singular page views. Blog
browsing belongs under `/blog`, lane domains, or article routes.

## New BlackKnightController domain

`blackknightcontroller.com` exists as a separate IONOS DNS zone. During the
first pass, the API allowed creating:

```text
www.blackknightcontroller.com A 74.208.45.165
```

The IONOS-generated root records were visible but protected from PATCH by the
DNS API:

```text
blackknightcontroller.com A    IONOS parking address
blackknightcontroller.com AAAA IONOS parking address
```

Until the root parking/website-builder lock is cleared in the IONOS UI, the
bare domain may continue to show IONOS parking content. The nginx redirect is
already prepared for both names:

```text
blackknightcontroller.com
www.blackknightcontroller.com
```

Both redirect to:

```text
https://blackknight.auzietek.com
```

`www.blackknightcontroller.com` works immediately because its A record points at
the Auzietek VPS. HTTPS for the new domain still needs a dedicated certificate;
the current `*.auzietek.com` certificate does not cover
`blackknightcontroller.com`.

## Cutover boundary

Do not point `auzietek.com` or `www.auzietek.com` at the micro-blog deployment
until content review, admin security review, backup/rollback, and certificate
validation are complete.
