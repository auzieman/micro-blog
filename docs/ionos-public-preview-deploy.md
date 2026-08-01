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

blackknightcontroller.com            pending/parking until IONOS lock clears
www.blackknightcontroller.com        A 74.208.45.165   redirect/product lane
blackknightcontrollerweb.online      pending/new domain, candidate redirect/product lane

auzietech.com                        pending/new domain, candidate Auzietek redirect
auzietech.net                        pending/new domain, candidate Auzietek redirect
auzietech.info                       pending/new domain, candidate Auzietek redirect
auzietech.online                     pending/new domain, candidate Auzietek redirect
auzietech.store                      pending/new domain, candidate Auzietek redirect or merch/lab future

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

## Public viewer telemetry

The UI emits `blog.page.views_total` for public GET requests. Static assets and
mounted content-file fetches are excluded so page-view charts show human-facing
routes rather than every CSS, image, or imported article asset.

Metric labels intentionally stay low-cardinality:

```text
host
route
page_kind
lane
country
region
status
```

Raw visitor IP addresses are not used as metric labels. The structured app log
includes only a salted `visitor.id` hash for correlation during troubleshooting.

Optional GeoIP enrichment is controlled by environment variables:

```text
VISITOR_HASH_SALT=change-me-before-deploy
GEOIP_LOOKUP_URL=
GEOIP_TIMEOUT_SECONDS=0.75
GEOIP_CACHE_SECONDS=86400
```

If `GEOIP_LOOKUP_URL` is blank, public requests still count but location labels
remain `unknown` for public IPs and `private` for RFC1918/lab traffic. If used,
the URL must include an `{ip}` placeholder and return JSON containing one of
`country_code`, `countryCode`, or `country`, plus optionally `region`,
`regionName`, or `state_prov`.

The checked-in Grafana overview dashboard now includes public-page panels for:

```text
public page-view rate by host/lane/page kind
top public routes over the last hour
viewer countries over the last day
```

IONOS monitoring alignment note:

```text
remote Prometheus stack: /svc/grafana-compose
micro-blog collector:    /svc/micro-blog/collector/otel-collector-local.yaml
metric endpoint:         otel collector Prometheus exporter on port 9464
```

On the current VPS, Prometheus is a preserved swarm service while micro-blog is
a compose deployment. A temporary scrape target can be made by attaching the
Prometheus container to `micro-blog_app_net` and scraping the collector's
private Docker IP on `:9464`. Long term, replace that with a durable shared
monitoring network, a stable internal scrape hostname, or provisioning managed
by the deployment pipeline.

The dashboard JSON is deployed with the app under:

```text
/svc/micro-blog/collector/microblog-overview-dashboard.json
```

If Grafana API auth is available, import it with overwrite enabled. If not,
mount/provision it into the existing Grafana stack rather than relying on the
compose-local Grafana profile.

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

## New domains and redirect posture

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

The newer AuzieTech spellings and `blackknightcontrollerweb.online` should be
treated as brand-protection and routing names until there is a deliberate use
case. The safe first behavior is HTTP/HTTPS redirect after DNS and certificate
coverage are ready:

```text
blackknightcontroller.com
www.blackknightcontroller.com
blackknightcontrollerweb.online
www.blackknightcontrollerweb.online
  -> https://blackknight.auzietek.com/

auzietech.com
www.auzietech.com
auzietech.net
www.auzietech.net
auzietech.info
www.auzietech.info
auzietech.online
www.auzietech.online
auzietech.store
www.auzietech.store
  -> https://beta.auzietek.com/ during preview
  -> https://www.auzietek.com/ after production cutover
```

The Flask UI now knows these hostnames as lane aliases so nginx can proxy them
without landing visitors in the wrong lane while redirect/certificate work is
being finalized.

2026-08-01 receipt: nginx on the Auzietek VPS has HTTP redirect handling ready
for the new names. `www.blackknightcontroller.com` already resolves to
`74.208.45.165` and redirects to the BlackKnight lane. The newly purchased root
domains were visible through the IONOS DNS API, but write attempts returned
`401 UNAUTHORIZED`, so their parking A/AAAA records remain pending until IONOS
finishes provisioning or the DNS API authorization catches up. Do not treat that
as an nginx or Flask failure.

## Cutover boundary

Do not point `auzietek.com` or `www.auzietek.com` at the micro-blog deployment
until content review, admin security review, backup/rollback, and certificate
validation are complete.

When the cutover is approved, keep the old Drupal site available as a legacy
archive rather than silently destroying history. Candidate shape:

```text
legacy.auzietek.com  -> old Drupal/archive host or static export
auzietek.com         -> canonical new Auzietek site
www.auzietek.com     -> canonical new Auzietek site
```
