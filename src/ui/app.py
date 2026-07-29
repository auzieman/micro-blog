import logging
import json
import os
import time
import uuid
from urllib.parse import urlencode

import requests
from flask import Flask, abort, redirect, render_template, request, send_from_directory, session, url_for, Response
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from werkzeug.middleware.proxy_fix import ProxyFix

from blog_shared import (
    BlogTelemetry,
    article_json_ld,
    article_public_metadata,
    build_rss_xml,
    build_sitemap_xml,
    configure_logging,
    event_scope,
)

configure_logging()
logger = logging.getLogger("microblog.ui")
telemetry = BlogTelemetry("blog-ui")


def coerce_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-me-for-real-deployments")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = coerce_bool(os.getenv("SESSION_COOKIE_SECURE"), False)
app.config["PERMANENT_SESSION_LIFETIME"] = int(os.getenv("ADMIN_SESSION_SECONDS", "3600"))
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_CONTENT_LENGTH_BYTES", str(2 * 1024 * 1024)))
FlaskInstrumentor().instrument_app(app)

API_BASE_URL = os.getenv("BLOG_API_BASE_URL", "http://localhost:8080")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "auzieman@gmail.com")
ADMIN_ACCESS_CODE = os.getenv("ADMIN_ACCESS_CODE", "local-admin")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_OAUTH_REDIRECT_URI = os.getenv("GOOGLE_OAUTH_REDIRECT_URI", "")
SITE_URL = os.getenv("SITE_URL", "http://localhost:8081").rstrip("/")
SITE_NAME = os.getenv("SITE_NAME", "Micro Blog")
SITE_DESCRIPTION = os.getenv("SITE_DESCRIPTION", "Single-admin micro blog with imports and observability.")
SITE_BRAND = os.getenv("SITE_BRAND", SITE_NAME)
SITE_SECTION = os.getenv("SITE_SECTION", "business")
SITE_POSITIONING = os.getenv(
    "SITE_POSITIONING",
    "Infrastructure automation, practical operations, and field-tested systems guidance.",
)
SITE_AUDIENCE = os.getenv("SITE_AUDIENCE", "engineering leaders, young engineers, and right-fit clients")
SITE_HEADLINE = os.getenv(
    "SITE_HEADLINE",
    "Infrastructure automation with the lab evidence still attached.",
)
SITE_NAV_LINKS_JSON = os.getenv("SITE_NAV_LINKS_JSON", "")
MICROSITES_JSON = os.getenv("MICROSITES_JSON", "")
HOST_LANE_MAP_JSON = os.getenv("HOST_LANE_MAP_JSON", "")
DEFAULT_OG_IMAGE = os.getenv("DEFAULT_OG_IMAGE", "")
THEME_VARIANTS = ["auzietek", "linux-pro", "retro", "aurora", "paper", "midnight"]
DEFAULT_THEME_VARIANT = os.getenv("DEFAULT_THEME_VARIANT", "auzietek")
DRUPAL_SOURCE_TYPES = {
    "blog_post": "jsonapi/node/blog_post",
    "article": "jsonapi/node/article",
}
ADMIN_PREVIEW_TTL_SECONDS = int(os.getenv("ADMIN_PREVIEW_TTL_SECONDS", "1800"))
CONTENT_IMPORT_ROOT = os.getenv("CONTENT_IMPORT_ROOT", "/content")
ADMIN_LOGIN_WINDOW_SECONDS = int(os.getenv("ADMIN_LOGIN_WINDOW_SECONDS", "900"))
ADMIN_LOGIN_MAX_ATTEMPTS = int(os.getenv("ADMIN_LOGIN_MAX_ATTEMPTS", "5"))
ENABLE_HSTS = coerce_bool(os.getenv("ENABLE_HSTS"), False)

_ADMIN_PREVIEW_CACHE: dict[str, dict] = {}
_ADMIN_LOGIN_ATTEMPTS: dict[str, list[float]] = {}

DEFAULT_SITE_NAV_LINKS = [
    {"label": "Services", "href": "/blog?tag=services"},
    {"label": "BlackKnight", "href": "https://blackknight.auzietek.com"},
    {"label": "Linux Users", "href": "https://linux-users.auzietek.com"},
    {"label": "Retro Users", "href": "https://retro-users.auzietek.com"},
    {"label": "Labs", "href": "/blog?tag=lab"},
    {"label": "RSS", "href": "/rss.xml"},
]

DEFAULT_MICROSITES = [
    {
        "name": "www.auzietek.com",
        "label": "Auzietek",
        "role": "Business front door",
        "summary": "Services, product direction, client-fit proof, and polished public articles.",
        "href": "https://beta.auzietek.com/",
    },
    {
        "name": "blackknight.auzietek.com",
        "label": "BlackKnight",
        "role": "Product and platform journal",
        "summary": "BKC demos, hardware automation, pipeline evidence, and operator-facing patterns.",
        "href": "https://blackknight.auzietek.com/",
    },
    {
        "name": "linux-users.auzietek.com",
        "label": "Linux Users",
        "role": "Teaching lane",
        "summary": "Clear walkthroughs for newer engineers without losing the evidence trail.",
        "href": "https://linux-users.auzietek.com/blog",
    },
    {
        "name": "retro-users.auzietek.com",
        "label": "Retro Users",
        "role": "Retro computing lane",
        "summary": "Amiga, classic systems, preservation notes, and modern tooling around old iron.",
        "href": "https://retro-users.auzietek.com/blog",
    },
]

LANE_CONFIG = {
    "auzietek": {
        "label": "Auzietek",
        "site_name": "Auzietek",
        "site_brand": "Auzietek",
        "site_section": "business",
        "theme": "auzietek",
        "tag": "services",
        "featured_slug": "infrastructure-automation-that-stays-repeatable",
        "headline": "Human-first engineering for cleaner systems.",
        "description": "Auzietek helps small teams and practical operators make technology more natural, predictable, and clean through repeatable automation, clear evidence, and human-led AI operations.",
        "positioning": "Simpler solutions, practical automation, and human-first AIOps.",
        "audience": "engineering leaders, founders, and right-fit clients",
        "landing": {
            "eyebrow": "What can Auzietek do for you?",
            "title": "Make complex technology feel natural again.",
            "body": (
                "Auzietek is built around a simple rule: solutions should be simpler than the "
                "problems that demanded them. We help organizations clean up infrastructure, "
                "automate repeatable work, and make operations easier to explain. The lab work "
                "behind the site is not just content; it is proof that the patterns are being "
                "built, tested, broken, repaired, and documented in the open."
            ),
            "bullets": [
                "Stabilize Linux, virtualization, container, and network operations.",
                "Automate deployments from bare metal through running services.",
                "Use AI as an engineering amplifier with evidence, guardrails, and human judgment.",
                "Convert tribal knowledge into reusable runbooks, pipelines, training material, and opportunities for more people to learn.",
            ],
            "links": [
                {"label": "BlackKnightController", "href": "/blog?lane=blackknight"},
                {"label": "ThinkTank notes", "href": "/blog?tag=thinktank&lane=auzietek"},
                {"label": "AIOps direction", "href": "/blog?tag=aiops&lane=auzietek"},
                {"label": "Garland Computers", "href": "https://www.garlandcomputers.com/"},
                {"label": "Linux Users", "href": "/blog?lane=linux"},
                {"label": "Retro Users", "href": "/blog?lane=retro"},
            ],
            "sections": [
                {
                    "title": "Infrastructure automation",
                    "body": "PXE, IPMI, SSH, Docker, OpenStack, Proxmox, ESXi, monitoring, and repeatable deployment patterns for labs, small offices, and practical platform teams.",
                },
                {
                    "title": "AIOps without the fog",
                    "body": "AI-assisted operations that keep people in control: structured context, clean tool contracts, retrieved evidence, and decisions that can be audited later.",
                },
                {
                    "title": "Operator education",
                    "body": "Articles, demos, and working examples that teach engineers how to reason through real systems instead of memorizing disconnected commands.",
                },
                {
                    "title": "Product-minded cleanup",
                    "body": "Auzietek can help turn recurring support pain into documented processes, internal tools, or product ideas that make future work smaller.",
                },
            ],
        },
    },
    "blackknight": {
        "label": "BlackKnight",
        "site_name": "BlackKnightController",
        "site_brand": "BlackKnight",
        "site_section": "product journal",
        "theme": "midnight",
        "tag": "blackknightcontroller",
        "featured_slug": "blackknightcontroller-recovery-weekend-repeatable-lab",
        "headline": "Rebuild real infrastructure from power button to running service.",
        "description": "BlackKnightController automates the work engineers normally do by hand: power control, PXE, SSH, templates, APIs, validation, and evidence capture.",
        "positioning": "A lab-proven infrastructure automation control plane.",
        "audience": "operators, platform engineers, MSPs, and infrastructure-curious clients",
        "landing": {
            "eyebrow": "What is BlackKnightController?",
            "title": "What can BlackKnightController do for you?",
            "body": (
                "BlackKnightController turns normal IT actions into reusable pipelines. It can wake "
                "hardware, boot installers, ship scripts and templates, configure services, call APIs, "
                "validate the result, and keep the evidence close enough that the next run is smarter."
            ),
            "bullets": [
                "Rebuild cattle-style lab or edge servers without treating them like pets.",
                "Provision hypervisors, swarms, OpenStack labs, service VMs, and supporting network pieces.",
                "Capture known-good fragments so hard-won fixes do not disappear into chat history.",
                "Give operators a web UI and API for repeatable infrastructure actions.",
            ],
            "links": [
                {"label": "GitHub", "href": "https://github.com/auzietek/BlackKnightController"},
                {"label": "Recent BKC posts", "href": "/blog?lane=blackknight"},
                {"label": "Lab evidence", "href": "/blog?tag=lab&lane=blackknight"},
            ],
            "videos": [
                {
                    "title": "Segment 01: IPMI and pipeline foundations",
                    "summary": "Power control, PXE intent, and the first repeatable bare-metal flow.",
                    "href": "https://www.youtube.com/@auzietek",
                },
                {
                    "title": "Segments 10-40: OpenStack, Proxmox, and move-in day",
                    "summary": "From destructive lab rebuilds to running services and validation.",
                    "href": "https://youtu.be/BnD7X-uhDuI",
                },
                {
                    "title": "Segment 50: ESXi as a controlled lab target",
                    "summary": "A pragmatic commercial-hypervisor lane using BKC-style evidence.",
                    "href": "https://www.youtube.com/@auzietek",
                },
            ],
        },
    },
    "linux": {
        "label": "Linux Users",
        "site_name": "Linux Users",
        "site_brand": "Linux Users",
        "site_section": "teaching lane",
        "theme": "linux-pro",
        "tag": "linux",
        "headline": "Linux operations taught from the evidence outward.",
        "description": "Clear Linux walkthroughs for newer engineers using Debian, Fedora, containers, networks, and real troubleshooting traces.",
        "positioning": "Practical Linux education without hand-waving.",
        "audience": "newer Linux engineers, homelab builders, and practical operations teams",
        "landing": {
            "eyebrow": "Teaching lane",
            "title": "Linux notes that turn real incidents into repeatable lessons.",
            "body": (
                "Linux Users is the clean-room teaching version of the lab notebook: PXE, "
                "Debian, Fedora, containers, monitoring, SSH, storage, and practical "
                "troubleshooting rewritten for engineers who want the pattern, not the fog."
            ),
            "bullets": [
                "Legacy Auzietek articles are mirrored into lab as drafts before publishing.",
                "Posts should explain the reusable operator pattern, not just the one-off fix.",
                "The light theme stays intentional: readable, calm, and built for learning.",
            ],
            "links": [
                {"label": "PXE install pattern", "href": "/blog?tag=pxe&lane=linux"},
                {"label": "Monitoring notes", "href": "/blog?tag=monitoring&lane=linux"},
                {"label": "Docker and swarms", "href": "/blog?tag=docker-swarm&lane=linux"},
            ],
        },
    },
    "retro": {
        "label": "Retro Users",
        "site_name": "Retro Users",
        "site_brand": "Retro Users",
        "site_section": "retro computing lane",
        "theme": "retro",
        "tag": "retro",
        "headline": "Old machines, new lessons, useful constraints.",
        "description": "Classic computing, preservation, emulation, and the engineering lessons that still matter in modern labs.",
        "positioning": "Retro systems as a practical teaching lens.",
        "audience": "retro-computing fans, preservation-minded builders, and curious engineers",
        "hero_image_url": "/content-files/assets/retro/retro-computing-workbench-header.png",
        "landing": {
            "eyebrow": "Retro computing atmosphere",
            "title": "Yesterday's machines still teach today's infrastructure habits.",
            "body": (
                "Retro Users collects the Amiga, classic workstation, terminal, and old-tools "
                "side of Auzietek. The point is not a brand collage; it is the feeling of a "
                "living workshop where constraints made engineers sharp."
            ),
            "bullets": [
                "Legacy Amiga and tooling posts are mirrored into lab as reviewable drafts.",
                "GitHub repos and YouTube clips become evidence cards beside the articles.",
                "The theme can be playful, but the writing still teaches a usable lesson.",
            ],
            "links": [
                {"label": "Amiga posts", "href": "/blog?tag=AmigaOS4.1&lane=retro"},
                {"label": "Retro lessons", "href": "/blog?tag=retro&lane=retro"},
                {"label": "Auzietek YouTube", "href": "https://www.youtube.com/@auzietek"},
            ],
            "videos": [
                {
                    "title": "Retro computing video playlist",
                    "summary": "A safer media lane for older systems, experiments, and supporting build clips.",
                    "href": "https://www.youtube.com/watch?v=Wb904ngIYY0&list=PLzZDgKo1qG2ECpD0ZHQYKTGIuNsXMgNb7",
                },
            ],
        },
    },
}

DEFAULT_HOST_LANE_MAP = {
    "microblog.lab.auzietek.com": "auzietek",
    "auzietek.lab.auzietek.com": "auzietek",
    "blackknight.lab.auzietek.com": "blackknight",
    "linux-users.lab.auzietek.com": "linux",
    "retro-users.lab.auzietek.com": "retro",
    "beta.auzietek.com": "auzietek",
    "blackknight.auzietek.com": "blackknight",
    "linux-users.auzietek.com": "linux",
    "retro-users.auzietek.com": "retro",
}

AUZIETEK_PAGES = {
    "welcome": {
        "path": "/",
        "label": "Welcome",
        "tag": "services",
        "eyebrow": "Auzietek",
        "title": "What can Auzietek do for you?",
        "body": "Auzietek helps you make technology more natural, predictable, and clean: fewer mystery systems, fewer one-off fixes, clearer automation, better observability, and engineering guidance your team can reuse.",
        "points": [
            "Assess and clean up Linux, virtualization, container, and network environments.",
            "Build repeatable deployment paths from bare metal to application services.",
            "Turn incidents and experiments into documentation, training, and better operating habits.",
        ],
    },
    "thinktank": {
        "path": "/thinktank",
        "label": "ThinkTank",
        "tag": "think-tank",
        "eyebrow": "ThinkTank",
        "title": "Ideas with a path toward useful systems.",
        "body": "The ThinkTank is where Auzietek keeps larger product and social-technology ideas: RACS, human-centered computing, BlackKnightController, AIOps, and the long arc of making complex technology feel more natural and humane.",
        "points": [
            "RACS remains a serious long-range thread: sustainable, autonomous infrastructure patterns for communities, education, medical support, and disaster resilience.",
            "Human-centered computing asks how interfaces, automation, and AI can reduce friction instead of increasing it.",
            "BlackKnightController turns the more immediate parts of that vision into working infrastructure automation.",
            "Auzix and next-era computing ideas stay in debate until they have enough practical evidence to promote.",
        ],
    },
    "principles": {
        "path": "/principles",
        "label": "Principles",
        "tag": "principles",
        "eyebrow": "Why we build",
        "title": "Solutions should be simpler than the problems that demanded them.",
        "body": "Auzietek is built around human-first engineering: technology should reduce friction, preserve dignity, and make serious systems work easier to understand. We value demonstrated skill over credentials alone, practical evidence over theatre, and flexible work that leaves room for a real life.",
        "points": [
            "Make engineering accessible, exciting, and repeatable instead of turning it into ceremony.",
            "Hire and collaborate around merit, evidence, curiosity, judgment, and what people have actually built.",
            "Use lean learning loops: build the smallest useful thing, validate it against reality, preserve what worked, and improve the next pass.",
            "Use AI to amplify human operators, not to hide decisions inside opaque systems.",
            "Protect team health with trust, clarity, accountability, and respect before process theater.",
            "Share useful work broadly: play fair with the license, and ask for help when support is what makes adoption sustainable.",
            "If the company succeeds, direct meaningful resources toward education, disaster relief, community labs, and broader access to technology.",
        ],
    },
    "articles": {
        "path": "/articles",
        "label": "Articles",
        "tag": None,
        "eyebrow": "Articles",
        "title": "Proof, walkthroughs, and practical teaching material.",
        "body": "Articles are where Auzietek turns real work into public guidance: practical walkthroughs, migration notes, troubleshooting patterns, and product thinking that help engineers and clients see how the work is actually done.",
        "points": [
            "Linux and platform articles move into the Linux Users lane.",
            "BlackKnightController updates stay linked to working demos and pipeline evidence.",
            "Retro computing articles preserve older lessons that still matter in modern systems.",
        ],
    },
    "services": {
        "path": "/services",
        "label": "Services",
        "tag": "services",
        "eyebrow": "Services",
        "title": "Services for teams that need cleaner systems, not more ceremony.",
        "body": "Auzietek is a fit for small teams, founders, MSP-style environments, and practical operators who need cleaner operations, repeatable deployments, service migration, observability, and automation an engineer can still reason about at 2 AM.",
        "points": [
            "Infrastructure assessment, cleanup, and lab-to-production pipeline design.",
            "Small-office and homelab patterns that can grow into professional operations.",
            "Documentation, training, and remediation plans that preserve context.",
        ],
    },
    "aiops": {
        "path": "/aiops",
        "label": "AIOps",
        "tag": "aiops",
        "eyebrow": "Human + AI operations",
        "title": "AI should make engineers faster without making systems opaque.",
        "body": "Auzietek's AIOps direction is grounded in human-led systems work: stable tool contracts, evidence retrieval, graph context, operational memory, and clear permissions before action. The point is better collaboration, not magic.",
        "points": [
            "Humans steer intent; AI helps gather context, draft steps, and validate outcomes.",
            "Operational memory keeps known-good fragments near the pipelines that use them.",
            "The system should make work faster while making decisions easier to audit.",
        ],
    },
    "friends": {
        "path": "/friends",
        "label": "Friends",
        "tag": "partners",
        "eyebrow": "Friends and partners",
        "title": "Good infrastructure work is stronger with good neighbors.",
        "body": "Auzietek keeps room for field-tested resources, community projects, and practical vendors that make real systems easier to buy, repair, explain, and operate. This page is intentionally curated: useful links, clear context, no link confetti.",
        "points": [
            "Resources are listed because they are useful to the work, not because every listing is a formal partnership.",
            "Hardware, open-source tools, learning material, and collaborators should each have enough context to be worth the click.",
            "As lab work becomes public guidance, this page will collect the people and projects that helped make it practical.",
        ],
        "resources": [
            {
                "name": "Garland Computers",
                "category": "Hardware and lab supplier",
                "description": "A practical local computer resource for parts, systems, repair conversations, and the kind of hands-on hardware support that keeps labs moving.",
                "why": "Referenced as a useful field resource while Auzietek builds and documents repeatable infrastructure labs.",
                "url": "https://www.garlandcomputers.com/",
                "disclosure": "Informal lab/resource reference; not presented as a formal partnership.",
            },
            {
                "name": "BlackKnightController",
                "category": "Auzietek project",
                "description": "The infrastructure automation control plane used in the lab stories: IPMI, PXE, hypervisor buildouts, Docker Swarm, OpenStack, ESXi, DNS, and evidence capture.",
                "why": "It is the working proof engine behind many of the public examples.",
                "url": "https://github.com/auzieman/BlackKnightController-main",
                "disclosure": "Auzietek-owned project.",
            },
            {
                "name": "Micro Blog",
                "category": "Auzietek project",
                "description": "A lightweight publishing system for turning lab work, imports, screenshots, and field notes into public articles without dragging a full CMS everywhere.",
                "why": "This beta site is running on it now, which makes the site itself part of the evidence trail.",
                "url": "https://github.com/auzieman/micro-blog",
                "disclosure": "Auzietek-owned project.",
            },
        ],
    },
}

LAB_HOST_BY_LANE = {
    "auzietek": "auzietek.lab.auzietek.com",
    "blackknight": "blackknight.lab.auzietek.com",
    "linux": "linux-users.lab.auzietek.com",
    "retro": "retro-users.lab.auzietek.com",
}

PUBLIC_HOST_BY_LANE = {
    "auzietek": "beta.auzietek.com",
    "blackknight": "blackknight.auzietek.com",
    "linux": "linux-users.auzietek.com",
    "retro": "retro-users.auzietek.com",
}

THEME_LANE_MAP = {lane["theme"]: key for key, lane in LANE_CONFIG.items()}

MICROSITE_LANE_BY_NAME = {
    "www.auzietek.com": "auzietek",
    "auzietek": "auzietek",
    "blackknight.auzietek.com": "blackknight",
    "blackknight": "blackknight",
    "linux-users.auzietek.com": "linux",
    "linux users": "linux",
    "linux-users": "linux",
    "retro-users.auzietek.com": "retro",
    "retro users": "retro",
    "retro-users": "retro",
}


def _load_json_list(raw_value: str, fallback: list[dict]) -> list[dict]:
    normalized = raw_value.strip()
    if (
        len(normalized) >= 2
        and normalized[0] == normalized[-1]
        and normalized[0] in {"'", '"'}
    ):
        normalized = normalized[1:-1].strip()
    if not normalized:
        return fallback
    try:
        value = json.loads(normalized)
    except json.JSONDecodeError:
        logger.warning("invalid JSON list configuration ignored")
        return fallback
    if not isinstance(value, list):
        logger.warning("JSON list configuration must be a list")
        return fallback
    clean_items = []
    for item in value:
        if isinstance(item, dict):
            clean_items.append(item)
    return clean_items or fallback


def microsite_href_for_lane(lane_key: str) -> str:
    host = PUBLIC_HOST_BY_LANE.get(lane_key)
    if not host:
        return "#"
    return f"https://{host}/" if lane_key in {"auzietek", "blackknight"} else f"https://{host}/blog"


def normalize_microsites(items: list[dict]) -> list[dict]:
    normalized = []
    for item in items:
        fixed = dict(item)
        if not str(fixed.get("href") or "").strip():
            key_source = str(fixed.get("name") or fixed.get("label") or "").strip().lower()
            lane_key = MICROSITE_LANE_BY_NAME.get(key_source)
            if lane_key:
                fixed["href"] = microsite_href_for_lane(lane_key)
        normalized.append(fixed)
    return normalized


def resolve_lane(lane_key: str | None) -> tuple[str | None, dict | None]:
    if not lane_key:
        return None, None
    normalized = lane_key.strip().lower()
    if normalized in LANE_CONFIG:
        return normalized, LANE_CONFIG[normalized]
    return None, None


def _load_json_object(raw_value: str, fallback: dict) -> dict:
    normalized = raw_value.strip()
    if (
        len(normalized) >= 2
        and normalized[0] == normalized[-1]
        and normalized[0] in {"'", '"'}
    ):
        normalized = normalized[1:-1].strip()
    if not normalized:
        return fallback
    try:
        value = json.loads(normalized)
    except json.JSONDecodeError:
        logger.warning("invalid JSON object configuration ignored")
        return fallback
    if not isinstance(value, dict):
        logger.warning("JSON object configuration must be an object")
        return fallback
    return {str(key).strip().lower(): str(val).strip().lower() for key, val in value.items() if str(key).strip() and str(val).strip()} or fallback


def request_host() -> str:
    return request.host.split(":", 1)[0].strip().lower()


def resolve_request_lane(explicit_lane: str | None) -> tuple[str | None, dict | None, bool]:
    lane_key, lane = resolve_lane(explicit_lane)
    if lane:
        return lane_key, lane, False
    host_map = _load_json_object(HOST_LANE_MAP_JSON, DEFAULT_HOST_LANE_MAP)
    host_lane = host_map.get(request_host())
    lane_key, lane = resolve_lane(host_lane)
    return lane_key, lane, bool(lane)


def effective_site_url(host_lane_selected: bool) -> str:
    if host_lane_selected:
        return request.url_root.rstrip("/")
    return SITE_URL


def article_lane_key(article: dict | None) -> str | None:
    if not article:
        return None
    theme_lane = THEME_LANE_MAP.get(str(article.get("theme_variant") or "").strip().lower())
    if theme_lane:
        return theme_lane
    article_tags = {str(tag).strip().lower() for tag in article.get("tags") or []}
    for key, lane in LANE_CONFIG.items():
        lane_tag = str(lane.get("tag", "")).strip().lower()
        if lane_tag and lane_tag in article_tags:
            return key
    return None


def lane_host_for_current_zone(lane_key: str) -> str | None:
    host = request_host()
    if host.endswith(".lab.auzietek.com"):
        return LAB_HOST_BY_LANE.get(lane_key)
    if host.endswith(".auzietek.com"):
        return PUBLIC_HOST_BY_LANE.get(lane_key)
    return None


def redirect_for_lane_mismatch(selected: dict | None, active_lane: str | None, host_lane_selected: bool):
    selected_lane = article_lane_key(selected)
    if not selected_lane or not active_lane or selected_lane == active_lane:
        return None
    query = {"lane": selected_lane}
    host = lane_host_for_current_zone(selected_lane) if host_lane_selected else None
    if host:
        return redirect(f"{request.scheme}://{host}{url_for('public_post', slug=selected['slug'])}?{urlencode(query)}#article-start", code=302)
    return redirect(f"{url_for('public_post', slug=selected['slug'])}?{urlencode(query)}#article-start", code=302)


def lane_nav_links(active_lane: str | None, active_theme: str | None, active_page: str | None = None) -> list[dict]:
    if active_lane == "auzietek":
        return [
            {"label": page["label"], "href": page["path"], "active": key == active_page}
            for key, page in AUZIETEK_PAGES.items()
        ] + [{"label": "RSS", "href": "/rss.xml", "active": False}]
    links = []
    for key, lane in LANE_CONFIG.items():
        href = f"/blog?{urlencode({'lane': key})}"
        links.append({"label": lane["label"], "href": href, "active": key == active_lane})
    links.append({"label": "All Posts", "href": f"/blog?{urlencode({'theme': active_theme})}" if active_theme else "/blog", "active": active_lane is None})
    links.append({"label": "RSS", "href": "/rss.xml", "active": False})
    return links


def api_get(path: str, **params):
    return requests.get(f"{API_BASE_URL}{path}", params=params, timeout=15)


def api_post(path: str, payload: dict):
    payload.setdefault("admin_email", ADMIN_EMAIL)
    return requests.post(f"{API_BASE_URL}{path}", json=payload, timeout=20)


def api_put(path: str, payload: dict):
    payload.setdefault("admin_email", ADMIN_EMAIL)
    return requests.put(f"{API_BASE_URL}{path}", json=payload, timeout=20)


def build_drupal_endpoint(site_url: str, source_type: str, explicit_endpoint: str) -> str:
    if explicit_endpoint:
        return explicit_endpoint
    base = site_url.rstrip("/")
    suffix = DRUPAL_SOURCE_TYPES.get(source_type, "")
    return f"{base}/{suffix}" if suffix else base


def is_admin_authenticated() -> bool:
    return session.get("admin_email") == ADMIN_EMAIL


def google_oauth_ready() -> bool:
    return bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)


def google_redirect_uri() -> str:
    return GOOGLE_OAUTH_REDIRECT_URI.strip() or f"{request.url_root.rstrip('/')}/admin/login/google/callback"


def _purge_preview_cache() -> None:
    now = time.time()
    stale_keys = [key for key, value in _ADMIN_PREVIEW_CACHE.items() if value.get("expires_at", 0) <= now]
    for key in stale_keys:
        _ADMIN_PREVIEW_CACHE.pop(key, None)


def _load_preview_state() -> dict:
    _purge_preview_cache()
    token = session.get("admin_preview_token")
    if not token:
        return {}
    return _ADMIN_PREVIEW_CACHE.get(token, {})


def _store_preview_state(payload: dict) -> None:
    _purge_preview_cache()
    token = session.get("admin_preview_token") or uuid.uuid4().hex
    session["admin_preview_token"] = token
    _ADMIN_PREVIEW_CACHE[token] = {**payload, "expires_at": time.time() + ADMIN_PREVIEW_TTL_SECONDS}


def _clear_preview_state() -> None:
    token = session.pop("admin_preview_token", None)
    if token:
        _ADMIN_PREVIEW_CACHE.pop(token, None)


def _client_identifier() -> str:
    forwarded_for = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
    return forwarded_for or request.remote_addr or "unknown"


def _is_login_rate_limited() -> bool:
    now = time.time()
    client_id = _client_identifier()
    attempts = [stamp for stamp in _ADMIN_LOGIN_ATTEMPTS.get(client_id, []) if now - stamp <= ADMIN_LOGIN_WINDOW_SECONDS]
    _ADMIN_LOGIN_ATTEMPTS[client_id] = attempts
    return len(attempts) >= ADMIN_LOGIN_MAX_ATTEMPTS


def _record_login_attempt(success: bool) -> None:
    client_id = _client_identifier()
    if success:
        _ADMIN_LOGIN_ATTEMPTS.pop(client_id, None)
        return
    now = time.time()
    attempts = [stamp for stamp in _ADMIN_LOGIN_ATTEMPTS.get(client_id, []) if now - stamp <= ADMIN_LOGIN_WINDOW_SECONDS]
    attempts.append(now)
    _ADMIN_LOGIN_ATTEMPTS[client_id] = attempts


def admin_context(message=None):
    preview_state = _load_preview_state()
    return {
        "admin_email": ADMIN_EMAIL,
        "auth_mode": "google" if google_oauth_ready() else "local-code",
        "message": message,
        "drupal_preview": preview_state.get("drupal_preview", []),
        "drupal_endpoints": preview_state.get("drupal_endpoints", []),
        "drupal_form": preview_state.get("drupal_form", {}),
        "filesystem_preview": preview_state.get("filesystem_preview", []),
        "filesystem_form": preview_state.get("filesystem_form", {}),
        "public_crawl_preview": preview_state.get("public_crawl_preview", []),
        "public_crawl_form": preview_state.get("public_crawl_form", {}),
        "bootstrap_form": preview_state.get(
            "bootstrap_form",
            {
                "content_subdir": "posts/linux",
                "status": "published",
                "theme_variant": DEFAULT_THEME_VARIANT,
                "sync_mode": "update",
                "keyword_filter": "",
                "page_limit": "",
            },
        ),
        "drupal_source_types": DRUPAL_SOURCE_TYPES,
    }


def fetch_public_payload(page: int, page_size: int, slug: str | None, tag: str | None, featured_slug: str | None = None):
    payload = {"items": [], "total": 0, "page": page, "page_size": page_size}
    posts = []
    selected = None
    redirect_slug = None
    response = api_get("/posts", page=page, page_size=page_size, tag=tag)
    response.raise_for_status()
    payload = response.json()
    posts = payload["items"]
    selected_slug = slug or featured_slug
    if selected_slug:
        selected_response = api_get(f"/posts/{selected_slug}")
        if selected_response.status_code == 404:
            return payload, posts, None, None
        selected_response.raise_for_status()
        selected = selected_response.json()
        redirect_slug = selected.get("redirect_slug") if slug else None
    elif posts:
        selected = posts[0]
    return payload, posts, selected, redirect_slug


def fetch_all_public_posts():
    response = api_get("/posts/all")
    response.raise_for_status()
    return response.json()["items"]


def fetch_admin_payload(page: int, page_size: int):
    response = api_get("/admin/posts", page=page, page_size=page_size, admin_email=ADMIN_EMAIL)
    response.raise_for_status()
    return response.json()


def fetch_admin_post(article_id: str):
    response = api_get(f"/admin/posts/{article_id}", admin_email=ADMIN_EMAIL)
    response.raise_for_status()
    return response.json()


def fetch_admin_revisions(article_id: str):
    response = api_get(f"/admin/posts/{article_id}/revisions", admin_email=ADMIN_EMAIL)
    response.raise_for_status()
    return response.json()["items"]


def build_public_context(selected, posts, payload, message=None, active_theme=None, preview_mode=False, tag=None, lane_key=None, lane=None, site_url=None, static_page=None, active_page=None, is_homepage=False, show_article_section=None):
    resolved_site_url = (site_url or SITE_URL).rstrip("/")
    site_name = lane.get("site_name", SITE_NAME) if lane else SITE_NAME
    site_description = lane.get("description", SITE_DESCRIPTION) if lane else SITE_DESCRIPTION
    site_brand = lane.get("site_brand", SITE_BRAND) if lane else SITE_BRAND
    site_section = lane.get("site_section", SITE_SECTION) if lane else SITE_SECTION
    site_positioning = lane.get("positioning", SITE_POSITIONING) if lane else SITE_POSITIONING
    site_audience = lane.get("audience", SITE_AUDIENCE) if lane else SITE_AUDIENCE
    site_headline = lane.get("headline", SITE_HEADLINE) if lane else SITE_HEADLINE
    resolved_theme = active_theme or (lane.get("theme") if lane else None) or (selected.get("theme_variant") if selected else DEFAULT_THEME_VARIANT)
    if static_page:
        metadata = {
            "title": f"{static_page['title']} | {site_name}",
            "description": static_page.get("body") or site_description,
            "canonical_url": f"{resolved_site_url}{static_page.get('path') or request.path}",
            "og_image_url": DEFAULT_OG_IMAGE or None,
            "twitter_card": "summary_large_image" if DEFAULT_OG_IMAGE else "summary",
        }
        json_ld = ""
    elif selected:
        metadata = article_public_metadata(selected, resolved_site_url, site_name, DEFAULT_OG_IMAGE or None)
        json_ld = article_json_ld(selected, resolved_site_url, site_name, DEFAULT_OG_IMAGE or None)
    else:
        metadata = {
            "title": f"{site_name} | Infrastructure automation and practical systems support",
            "description": site_description,
            "canonical_url": f"{resolved_site_url}/blog",
            "og_image_url": DEFAULT_OG_IMAGE or None,
            "twitter_card": "summary_large_image" if DEFAULT_OG_IMAGE else "summary",
        }
        json_ld = ""
    total_pages = max(1, (payload["total"] + payload["page_size"] - 1) // payload["page_size"])
    return {
        "posts": posts,
        "selected": selected,
        "total": payload["total"],
        "page": payload["page"],
        "page_size": payload["page_size"],
        "total_pages": total_pages,
        "page_sizes": [10, 20],
        "active_theme": resolved_theme,
        "theme_variants": THEME_VARIANTS,
        "active_lane": lane_key,
        "active_lane_label": lane.get("label") if lane else "All Posts",
        "message": message,
        "is_admin_authenticated": is_admin_authenticated(),
        "site_name": site_name,
        "site_description": site_description,
        "site_brand": site_brand,
        "site_section": site_section,
        "site_positioning": site_positioning,
        "site_audience": site_audience,
        "site_headline": site_headline,
        "site_nav_links": lane_nav_links(lane_key, resolved_theme, active_page) if lane else _load_json_list(SITE_NAV_LINKS_JSON, DEFAULT_SITE_NAV_LINKS),
        "microsites": normalize_microsites(_load_json_list(MICROSITES_JSON, DEFAULT_MICROSITES)),
        "lane_landing": lane.get("landing") if lane else None,
        "lane_hero_image_url": lane.get("hero_image_url") if lane else "",
        "static_page": static_page,
        "is_homepage": is_homepage,
        "show_article_section": (bool(selected or posts) or not static_page) if show_article_section is None else show_article_section,
        "static_resources": static_page.get("resources", []) if static_page else [],
        "meta_title": metadata["title"],
        "meta_description": metadata["description"],
        "canonical_url": metadata["canonical_url"],
        "meta_og_image": metadata["og_image_url"],
        "meta_twitter_card": metadata["twitter_card"],
        "json_ld": json_ld,
        "preview_mode": preview_mode,
        "tag": tag,
    }


@app.after_request
def apply_security_headers(response):
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "img-src 'self' data: https:; "
        "style-src 'self' 'unsafe-inline' https:; "
        "script-src 'self' 'unsafe-inline' https:; "
        "font-src 'self' https: data:; "
        "connect-src 'self' http: https:; "
        "frame-src 'self' https://www.youtube.com https://www.youtube-nocookie.com; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self' https://accounts.google.com"
    )
    if ENABLE_HSTS:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Cache-Control"] = "no-store" if request.path.startswith("/admin") else "public, max-age=60"
    return response


@app.get("/healthz")
def healthz():
    return "Healthy", 200


@app.get("/")
@app.get("/blog")
def public_index():
    started = time.perf_counter()
    result = "success"
    page = int(request.args.get("page", "1"))
    page_size = int(request.args.get("page_size", "10"))
    lane_key, lane, host_lane_selected = resolve_request_lane(request.args.get("lane"))
    tag = request.args.get("tag")
    if lane and not tag:
        tag = lane["tag"]
    theme = request.args.get("theme") or (lane.get("theme") if lane else None)
    message = request.args.get("message")
    with event_scope(logger, "ui.public_index", page=page, page_size=page_size, tag=tag, theme=theme, lane=lane_key) as log:
        try:
            payload, posts, selected, _redirect_slug = fetch_public_payload(page, page_size, None, tag, lane.get("featured_slug") if lane else None)
        except Exception as exc:
            result = "error"
            log.exception("UI public index failed")
            telemetry.error("blog-ui", type(exc).__name__)
            payload = {"items": [], "total": 0, "page": page, "page_size": page_size}
            posts = []
            selected = None
            message = str(exc)
        finally:
            telemetry.api("/blog", "GET", result, (time.perf_counter() - started) * 1000.0)
    static_page = AUZIETEK_PAGES.get("welcome") if lane_key == "auzietek" and request.path == "/" else None
    return render_template("public_index.html", **build_public_context(selected, posts, payload, message=message, active_theme=theme, tag=tag, lane_key=lane_key, lane=lane, site_url=effective_site_url(host_lane_selected), static_page=static_page, active_page="welcome" if static_page else None, is_homepage=bool(static_page), show_article_section=request.path == "/blog"))


@app.get("/thinktank")
@app.get("/principles")
@app.get("/articles")
@app.get("/services")
@app.get("/aiops")
@app.get("/friends")
def auzietek_page():
    started = time.perf_counter()
    result = "success"
    page_key = request.path.strip("/") or "welcome"
    static_page = AUZIETEK_PAGES.get(page_key)
    if not static_page:
        abort(404)
    lane_key, lane = resolve_lane("auzietek")
    page = int(request.args.get("page", "1"))
    page_size = int(request.args.get("page_size", "10"))
    tag = request.args.get("tag")
    if not tag and static_page.get("tag") is not None:
        tag = static_page.get("tag")
    theme = request.args.get("theme") or lane.get("theme")
    message = request.args.get("message")
    with event_scope(logger, "ui.auzietek_page", page=page, page_size=page_size, tag=tag, theme=theme, page_key=page_key) as log:
        try:
            payload, posts, selected, _redirect_slug = fetch_public_payload(page, page_size, None, tag)
        except Exception as exc:
            result = "error"
            log.exception("UI Auzietek page failed")
            telemetry.error("blog-ui", type(exc).__name__)
            payload = {"items": [], "total": 0, "page": page, "page_size": page_size}
            posts = []
            selected = None
            message = str(exc)
        finally:
            telemetry.api(request.path, "GET", result, (time.perf_counter() - started) * 1000.0)
    return render_template("public_index.html", **build_public_context(selected, posts, payload, message=message, active_theme=theme, tag=tag, lane_key=lane_key, lane=lane, site_url=effective_site_url(True), static_page=static_page, active_page=page_key, is_homepage=False, show_article_section=False))


@app.get("/post/<slug>")
def public_post(slug: str):
    started = time.perf_counter()
    result = "success"
    page = 1
    page_size = 10
    lane_key, lane, host_lane_selected = resolve_request_lane(request.args.get("lane"))
    tag = request.args.get("tag")
    if lane and not tag:
        tag = lane["tag"]
    theme = request.args.get("theme") or (lane.get("theme") if lane else None)
    with event_scope(logger, "ui.public_post", slug=slug, tag=tag, theme=theme, lane=lane_key) as log:
        try:
            payload, posts, selected, redirect_slug = fetch_public_payload(page, page_size, slug, tag)
            if redirect_slug and redirect_slug != slug:
                return redirect(url_for("public_post", slug=redirect_slug, theme=theme, lane=lane_key), code=301)
            if not selected:
                abort(404)
            lane_redirect = redirect_for_lane_mismatch(selected, lane_key, host_lane_selected)
            if lane_redirect:
                return lane_redirect
        except Exception as exc:
            result = "error"
            log.exception("UI public post failed")
            telemetry.error("blog-ui", type(exc).__name__)
            raise
        finally:
            telemetry.api("/post/{slug}", "GET", result, (time.perf_counter() - started) * 1000.0)
    return render_template("public_index.html", **build_public_context(selected, posts, payload, active_theme=theme, tag=tag, lane_key=lane_key, lane=lane, site_url=effective_site_url(host_lane_selected)))


@app.get("/sitemap.xml")
def sitemap():
    posts = fetch_all_public_posts()
    xml = build_sitemap_xml(posts, SITE_URL)
    return Response(xml, mimetype="application/xml")


@app.get("/robots.txt")
def robots():
    text = f"User-agent: *\nAllow: /\nSitemap: {SITE_URL}/sitemap.xml\n"
    return Response(text, mimetype="text/plain")


@app.get("/rss.xml")
@app.get("/feed.xml")
def rss_feed():
    posts = fetch_all_public_posts()
    xml = build_rss_xml(posts, SITE_URL, SITE_NAME, SITE_DESCRIPTION)
    return Response(xml, mimetype="application/rss+xml")


@app.get("/admin")
def admin_index():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", next=request.path))
    started = time.perf_counter()
    result = "success"
    page = int(request.args.get("page", "1"))
    message = request.args.get("message")
    posts = []
    payload = {"items": [], "total": 0, "page": page, "page_size": 10}
    with event_scope(logger, "ui.admin_index", page=page) as log:
        try:
            payload = fetch_admin_payload(page, 10)
            posts = payload["items"]
        except Exception as exc:
            result = "error"
            log.exception("UI admin failed")
            telemetry.error("blog-ui", type(exc).__name__)
            message = str(exc)
        finally:
            telemetry.api("/admin", "GET", result, (time.perf_counter() - started) * 1000.0)
    return render_template("admin.html", posts=posts, total=payload["total"], theme_variants=THEME_VARIANTS, **admin_context(message))


@app.get("/admin/posts/<article_id>/edit")
def admin_edit(article_id: str):
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", next=request.path))
    message = request.args.get("message")
    article = fetch_admin_post(article_id)
    revisions = fetch_admin_revisions(article_id)
    return render_template("admin_edit.html", article=article, revisions=revisions, theme_variants=THEME_VARIANTS, **admin_context(message))


@app.get("/admin/login")
def admin_login():
    if is_admin_authenticated():
        return redirect(url_for("admin_index"))
    return render_template("admin_login.html", **admin_context(request.args.get("message")))


@app.post("/admin/login")
def admin_login_post():
    started = time.perf_counter()
    result = "success"
    email = request.form.get("email", "").strip().lower()
    access_code = request.form.get("access_code", "")
    with event_scope(logger, "ui.admin_login", email=email) as log:
        if _is_login_rate_limited():
            result = "rate_limited"
            message = "Too many failed admin login attempts. Wait and try again."
        elif google_oauth_ready():
            result = "error"
            message = "Google OAuth is enabled. Use the Google sign-in flow instead of the local code form."
        elif email == ADMIN_EMAIL.lower() and access_code == ADMIN_ACCESS_CODE:
            session.clear()
            session.permanent = True
            session["admin_email"] = ADMIN_EMAIL
            _record_login_attempt(success=True)
            telemetry.api("/admin/login", "POST", result, (time.perf_counter() - started) * 1000.0)
            return redirect(url_for("admin_index", message="Admin session established."))
        else:
            result = "denied"
            message = "Admin access denied."
            _record_login_attempt(success=False)
            log.warning("Admin login denied")
        telemetry.api("/admin/login", "POST", result, (time.perf_counter() - started) * 1000.0)
        return render_template("admin_login.html", **admin_context(message)), 401


@app.get("/admin/login/google")
def admin_login_google():
    if not google_oauth_ready():
        return redirect(url_for("admin_login", message="Google OAuth is not fully configured. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET."))
    state = uuid.uuid4().hex
    session["google_oauth_state"] = state
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": google_redirect_uri(),
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "prompt": "select_account",
        "access_type": "offline",
    }
    return redirect(f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}")


@app.get("/admin/login/google/callback")
def admin_login_google_callback():
    if not google_oauth_ready():
        return redirect(url_for("admin_login", message="Google OAuth is not fully configured."))
    state = request.args.get("state", "")
    code = request.args.get("code", "")
    if not code or state != session.get("google_oauth_state"):
        return redirect(url_for("admin_login", message="Google OAuth state validation failed."))
    try:
        token_response = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri": google_redirect_uri(),
                "grant_type": "authorization_code",
            },
            timeout=15,
        )
        token_response.raise_for_status()
        access_token = token_response.json()["access_token"]
        userinfo_response = requests.get(
            "https://openidconnect.googleapis.com/v1/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        userinfo_response.raise_for_status()
        profile = userinfo_response.json()
        email = profile.get("email", "").lower()
        verified = bool(profile.get("email_verified"))
        if email != ADMIN_EMAIL.lower() or not verified:
            _record_login_attempt(success=False)
            return redirect(url_for("admin_login", message="Google account is not allowed for this admin console."))
        session.clear()
        session.permanent = True
        session["admin_email"] = ADMIN_EMAIL
        _record_login_attempt(success=True)
        return redirect(url_for("admin_index", message="Google admin session established."))
    except Exception as exc:
        logger.exception("Google OAuth callback failed")
        return redirect(url_for("admin_login", message=str(exc)))


@app.post("/admin/logout")
def admin_logout():
    _clear_preview_state()
    session.clear()
    return redirect(url_for("public_index", message="Admin session cleared."))


def extract_post_form(form):
    return {
        "title": form["title"],
        "slug": form.get("slug", "").strip(),
        "summary": form.get("summary", ""),
        "markdown_body": form["markdown_body"],
        "body_format": form.get("body_format", "markdown"),
        "hero_image_url": form.get("hero_image_url") or None,
        "theme_variant": form.get("theme_variant", DEFAULT_THEME_VARIANT),
        "tags": [part.strip() for part in form.get("tags", "").split(",") if part.strip()],
        "status": form.get("status", "draft"),
        "seo_title": form.get("seo_title") or None,
        "seo_description": form.get("seo_description") or None,
        "canonical_url": form.get("canonical_url") or None,
        "og_image_url": form.get("og_image_url") or None,
    }


@app.post("/admin/create")
def create_post():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    payload = extract_post_form(request.form)
    response = api_post("/admin/posts", payload)
    message = f"Create status: {response.status_code} {response.text}"
    return redirect(url_for("admin_index", message=message))


@app.post("/admin/posts/<article_id>/update")
def update_post(article_id: str):
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    payload = extract_post_form(request.form)
    response = api_put(f"/admin/posts/{article_id}", payload)
    message = f"Update status: {response.status_code} {response.text}"
    return redirect(url_for("admin_edit", article_id=article_id, message=message))


@app.post("/admin/posts/<article_id>/publish")
def publish_post(article_id: str):
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    response = api_post(f"/admin/posts/{article_id}/publish", {})
    message = f"Publish status: {response.status_code} {response.text}"
    return redirect(request.form.get("return_to") or url_for("admin_index", message=message))


@app.post("/admin/posts/<article_id>/unpublish")
def unpublish_post(article_id: str):
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    response = api_post(f"/admin/posts/{article_id}/unpublish", {})
    message = f"Unpublish status: {response.status_code} {response.text}"
    return redirect(request.form.get("return_to") or url_for("admin_index", message=message))


@app.post("/admin/posts/<article_id>/delete")
def delete_post(article_id: str):
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    response = api_post(f"/admin/posts/{article_id}/delete", {})
    message = f"Delete status: {response.status_code} {response.text}"
    return redirect(request.form.get("return_to") or url_for("admin_index", message=message))


@app.post("/admin/posts/<article_id>/restore")
def restore_post(article_id: str):
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    payload = {"restore_status": request.form.get("restore_status") or None}
    response = api_post(f"/admin/posts/{article_id}/restore", payload)
    message = f"Restore status: {response.status_code} {response.text}"
    return redirect(request.form.get("return_to") or url_for("admin_index", message=message))


@app.post("/admin/posts/<article_id>/remirror")
def remirror_post(article_id: str):
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    response = api_post(f"/admin/posts/{article_id}/remirror", {})
    message = f"Re-mirror status: {response.status_code} {response.text}"
    return redirect(request.form.get("return_to") or url_for("admin_index", message=message))


@app.post("/admin/posts/<article_id>/hard-delete")
def hard_delete_post(article_id: str):
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    payload = {"confirm_article_id": request.form.get("confirm_article_id", "").strip()}
    response = api_post(f"/admin/posts/{article_id}/hard-delete", payload)
    message = f"Hard delete status: {response.status_code} {response.text}"
    return redirect(request.form.get("return_to") or url_for("admin_edit", article_id=article_id, message=message))


@app.post("/admin/posts/preview")
def preview_post():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    payload = extract_post_form(request.form)
    response = api_post("/admin/posts/preview", payload)
    response.raise_for_status()
    preview = response.json()
    selected = preview["article"]
    selected["html_body"] = preview["article"]["html_body"]
    payload_ctx = {"items": [selected], "total": 1, "page": 1, "page_size": 1}
    return render_template("public_index.html", **build_public_context(selected, [], payload_ctx, message="Draft preview", preview_mode=True))


@app.post("/admin/import-sample")
def import_sample():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    response = api_post("/admin/import-sample", {})
    return redirect(url_for("admin_index", message=f"Import status: {response.status_code} {response.text}"))


@app.post("/admin/import/drupal/preview")
def preview_drupal_import():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    site_url = request.form.get("site_url", "").strip() or request.form["endpoint_url"]
    source_type = request.form.get("source_type", "blog_post")
    explicit_endpoint = request.form.get("endpoint_url", "").strip()
    payload = {
        "endpoint_url": build_drupal_endpoint(site_url, source_type, explicit_endpoint),
        "source_base_url": request.form.get("source_base_url") or site_url,
        "status": request.form.get("status", "draft"),
        "body_format": request.form.get("body_format") or None,
        "theme_variant": request.form.get("theme_variant", DEFAULT_THEME_VARIANT),
        "allow_insecure_tls": request.form.get("allow_insecure_tls") == "on",
        "params": {},
        "dry_run": True,
    }
    if request.form.get("nid_filter"):
        payload["nid_filter"] = request.form["nid_filter"]
    if request.form.get("keyword_filter"):
        payload["keyword_filter"] = request.form["keyword_filter"]
    if request.form.get("include_value"):
        payload["params"]["include"] = request.form["include_value"]
    if request.form.get("page_limit"):
        payload["params"]["page[limit]"] = request.form["page_limit"]
    response = api_post("/admin/import/drupal", payload)
    response.raise_for_status()
    preview_payload = response.json()
    _store_preview_state(
        {
            "drupal_preview": preview_payload.get("items", []),
            "drupal_endpoints": preview_payload.get("endpoints", []),
            "drupal_form": {
                "endpoint_url": preview_payload.get("endpoint_url", payload["endpoint_url"]),
                "site_url": site_url,
                "source_type": source_type,
                "source_base_url": payload["source_base_url"],
                "include_value": request.form.get("include_value", ""),
                "page_limit": request.form.get("page_limit", ""),
                "nid_filter": request.form.get("nid_filter", ""),
                "keyword_filter": request.form.get("keyword_filter", ""),
                "status": payload["status"],
                "body_format": request.form.get("body_format", ""),
                "theme_variant": payload["theme_variant"],
                "allow_insecure_tls": payload["allow_insecure_tls"],
            },
        }
    )
    message = (
        f"Discovery loaded: {preview_payload.get('count', 0)} JSON:API endpoints."
        if preview_payload.get("status") == "DrupalEndpointDiscovery"
        else f"Preview loaded: {preview_payload.get('count', 0)} candidate articles."
    )
    return redirect(url_for("admin_index", message=message))


@app.post("/admin/import/filesystem/preview")
def preview_filesystem_import():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    payload = {
        "root_path": CONTENT_IMPORT_ROOT,
        "content_subdir": request.form.get("content_subdir", "").strip(),
        "status": request.form.get("status", "draft"),
        "theme_variant": request.form.get("theme_variant", DEFAULT_THEME_VARIANT),
        "keyword_filter": request.form.get("keyword_filter", "").strip(),
        "dry_run": True,
    }
    if request.form.get("page_limit"):
        payload["limit"] = request.form["page_limit"]
    response = api_post("/admin/import/filesystem", payload)
    response.raise_for_status()
    preview_payload = response.json()
    state = _load_preview_state()
    state["filesystem_preview"] = preview_payload.get("items", [])
    state["filesystem_form"] = {
        "content_subdir": payload["content_subdir"],
        "keyword_filter": payload["keyword_filter"],
        "page_limit": request.form.get("page_limit", ""),
        "status": payload["status"],
        "theme_variant": payload["theme_variant"],
    }
    _store_preview_state(state)
    return redirect(url_for("admin_index", message=f"Filesystem preview loaded: {preview_payload.get('count', 0)} candidate articles."))


@app.post("/admin/import/public-crawl/preview")
def preview_public_crawl_import():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    site_url = request.form.get("site_url", "").strip()
    payload = {
        "site_url": site_url,
        "listing_url": request.form.get("listing_url", "").strip(),
        "nid_filter": request.form.get("nid_filter", "").strip(),
        "keyword_filter": request.form.get("keyword_filter", "").strip(),
        "status": request.form.get("status", "draft"),
        "theme_variant": request.form.get("theme_variant", DEFAULT_THEME_VARIANT),
        "allow_insecure_tls": request.form.get("allow_insecure_tls") == "on",
        "dry_run": True,
    }
    if request.form.get("page_limit"):
        payload["limit"] = request.form["page_limit"]
    response = api_post("/admin/import/public-crawl", payload)
    response.raise_for_status()
    preview_payload = response.json()
    state = _load_preview_state()
    state["public_crawl_preview"] = preview_payload.get("items", [])
    state["public_crawl_form"] = {
        "site_url": site_url,
        "listing_url": payload["listing_url"],
        "nid_filter": payload["nid_filter"],
        "keyword_filter": payload["keyword_filter"],
        "page_limit": request.form.get("page_limit", ""),
        "status": payload["status"],
        "theme_variant": payload["theme_variant"],
        "allow_insecure_tls": payload["allow_insecure_tls"],
    }
    _store_preview_state(state)
    return redirect(url_for("admin_index", message=f"Public crawl preview loaded: {preview_payload.get('count', 0)} candidate articles."))


@app.post("/admin/import/public-crawl")
def import_public_crawl_selection():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    selected_ids = request.form.getlist("selected_source_ids")
    state = _load_preview_state()
    crawl_form = state.get("public_crawl_form", {})
    payload = {
        "site_url": crawl_form.get("site_url", ""),
        "listing_url": crawl_form.get("listing_url", ""),
        "nid_filter": crawl_form.get("nid_filter", ""),
        "keyword_filter": crawl_form.get("keyword_filter", ""),
        "status": crawl_form.get("status", "draft"),
        "theme_variant": crawl_form.get("theme_variant", DEFAULT_THEME_VARIANT),
        "allow_insecure_tls": crawl_form.get("allow_insecure_tls", False),
        "selected_source_ids": selected_ids,
    }
    if crawl_form.get("page_limit"):
        payload["limit"] = crawl_form["page_limit"]
    response = api_post("/admin/import/public-crawl", payload)
    response.raise_for_status()
    state["public_crawl_preview"] = []
    state["public_crawl_form"] = {}
    _store_preview_state(state)
    return redirect(url_for("admin_index", message=f"Public crawl import queued: {response.json().get('count', 0)} selected articles."))


@app.post("/admin/import/filesystem")
def import_filesystem_selection():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    selected_ids = request.form.getlist("selected_source_ids")
    state = _load_preview_state()
    filesystem_form = state.get("filesystem_form", {})
    payload = {
        "root_path": CONTENT_IMPORT_ROOT,
        "content_subdir": filesystem_form.get("content_subdir", ""),
        "status": filesystem_form.get("status", "draft"),
        "theme_variant": filesystem_form.get("theme_variant", DEFAULT_THEME_VARIANT),
        "selected_source_ids": selected_ids,
    }
    if filesystem_form.get("keyword_filter"):
        payload["keyword_filter"] = filesystem_form["keyword_filter"]
    if filesystem_form.get("page_limit"):
        payload["limit"] = filesystem_form["page_limit"]
    response = api_post("/admin/import/filesystem", payload)
    response.raise_for_status()
    state["filesystem_preview"] = []
    state["filesystem_form"] = {}
    _store_preview_state(state)
    return redirect(url_for("admin_index", message=f"Filesystem import queued: {response.json().get('count', 0)} selected articles."))


@app.post("/admin/bootstrap/filesystem-sync")
def bootstrap_filesystem_sync():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    payload = {
        "root_path": CONTENT_IMPORT_ROOT,
        "content_subdir": request.form.get("content_subdir", "").strip(),
        "status": request.form.get("status", "published"),
        "theme_variant": request.form.get("theme_variant", DEFAULT_THEME_VARIANT),
        "keyword_filter": request.form.get("keyword_filter", "").strip(),
        "sync_mode": request.form.get("sync_mode", "update"),
    }
    if request.form.get("page_limit"):
        payload["limit"] = request.form["page_limit"]
    response = api_post("/admin/bootstrap/filesystem-sync", payload)
    response.raise_for_status()
    state = _load_preview_state()
    state["bootstrap_form"] = {
        "content_subdir": payload["content_subdir"],
        "status": payload["status"],
        "theme_variant": payload["theme_variant"],
        "sync_mode": payload["sync_mode"],
        "keyword_filter": payload["keyword_filter"],
        "page_limit": request.form.get("page_limit", ""),
    }
    _store_preview_state(state)
    sync_payload = response.json()
    message = (
        f"Bootstrap sync queued: {sync_payload.get('count', 0)} upserts, "
        f"{sync_payload.get('skipped', 0)} skipped, {sync_payload.get('reset_deleted', 0)} resets."
    )
    return redirect(url_for("admin_index", message=message))


@app.get("/content-files/<path:relative_path>")
def content_files(relative_path: str):
    root = os.path.realpath(CONTENT_IMPORT_ROOT)
    target = os.path.realpath(os.path.join(root, relative_path))
    if not (target == root or target.startswith(f"{root}{os.sep}")):
        abort(404)
    if not os.path.exists(target):
        abort(404)
    return send_from_directory(os.path.dirname(target), os.path.basename(target))


@app.post("/admin/import/drupal")
def import_drupal_selection():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    selected_ids = request.form.getlist("selected_source_ids")
    drupal_form = _load_preview_state().get("drupal_form", {})
    payload = {
        "endpoint_url": drupal_form.get("endpoint_url"),
        "source_base_url": drupal_form.get("source_base_url", ""),
        "status": drupal_form.get("status", "draft"),
        "theme_variant": drupal_form.get("theme_variant", "aurora"),
        "selected_source_ids": selected_ids,
        "allow_insecure_tls": drupal_form.get("allow_insecure_tls", False),
        "params": {},
    }
    if drupal_form.get("nid_filter"):
        payload["nid_filter"] = drupal_form["nid_filter"]
    if drupal_form.get("keyword_filter"):
        payload["keyword_filter"] = drupal_form["keyword_filter"]
    if drupal_form.get("body_format"):
        payload["body_format"] = drupal_form["body_format"]
    if drupal_form.get("include_value"):
        payload["params"]["include"] = drupal_form["include_value"]
    if drupal_form.get("page_limit"):
        payload["params"]["page[limit]"] = drupal_form["page_limit"]
    response = api_post("/admin/import/drupal", payload)
    response.raise_for_status()
    _clear_preview_state()
    return redirect(url_for("admin_index", message=f"Drupal import queued: {response.json().get('count', 0)} selected articles."))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
