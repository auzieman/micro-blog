import logging
import os
import time
import uuid
from urllib.parse import urlencode

import requests
from flask import Flask, abort, redirect, render_template, request, send_from_directory, session, url_for, Response
from opentelemetry.instrumentation.flask import FlaskInstrumentor

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
DEFAULT_OG_IMAGE = os.getenv("DEFAULT_OG_IMAGE", "")
THEME_VARIANTS = ["aurora", "paper", "midnight"]
DEFAULT_THEME_VARIANT = os.getenv("DEFAULT_THEME_VARIANT", "midnight")
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


def fetch_public_payload(page: int, page_size: int, slug: str | None, tag: str | None):
    payload = {"items": [], "total": 0, "page": page, "page_size": page_size}
    posts = []
    selected = None
    redirect_slug = None
    response = api_get("/posts", page=page, page_size=page_size, tag=tag)
    response.raise_for_status()
    payload = response.json()
    posts = payload["items"]
    if slug:
        selected_response = api_get(f"/posts/{slug}")
        if selected_response.status_code == 404:
            return payload, posts, None, None
        selected_response.raise_for_status()
        selected = selected_response.json()
        redirect_slug = selected.get("redirect_slug")
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


def build_public_context(selected, posts, payload, message=None, active_theme=None, preview_mode=False, tag=None):
    metadata = article_public_metadata(selected, SITE_URL, SITE_NAME, DEFAULT_OG_IMAGE or None) if selected else {
        "title": f"{SITE_NAME} | Linux notes",
        "description": SITE_DESCRIPTION,
        "canonical_url": f"{SITE_URL}/blog",
        "og_image_url": DEFAULT_OG_IMAGE or None,
        "twitter_card": "summary_large_image" if DEFAULT_OG_IMAGE else "summary",
    }
    json_ld = article_json_ld(selected, SITE_URL, SITE_NAME, DEFAULT_OG_IMAGE or None) if selected else ""
    total_pages = max(1, (payload["total"] + payload["page_size"] - 1) // payload["page_size"])
    return {
        "posts": posts,
        "selected": selected,
        "total": payload["total"],
        "page": payload["page"],
        "page_size": payload["page_size"],
        "total_pages": total_pages,
        "page_sizes": [10, 20],
        "active_theme": active_theme or (selected.get("theme_variant") if selected else DEFAULT_THEME_VARIANT),
        "theme_variants": THEME_VARIANTS,
        "message": message,
        "is_admin_authenticated": is_admin_authenticated(),
        "site_name": SITE_NAME,
        "site_description": SITE_DESCRIPTION,
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
    tag = request.args.get("tag")
    theme = request.args.get("theme")
    message = request.args.get("message")
    with event_scope(logger, "ui.public_index", page=page, page_size=page_size, tag=tag, theme=theme) as log:
        try:
            payload, posts, selected, _redirect_slug = fetch_public_payload(page, page_size, None, tag)
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
    return render_template("public_index.html", **build_public_context(selected, posts, payload, message=message, active_theme=theme, tag=tag))


@app.get("/post/<slug>")
def public_post(slug: str):
    started = time.perf_counter()
    result = "success"
    page = 1
    page_size = 10
    theme = request.args.get("theme")
    with event_scope(logger, "ui.public_post", slug=slug, theme=theme) as log:
        try:
            payload, posts, selected, redirect_slug = fetch_public_payload(page, page_size, slug, None)
            if redirect_slug and redirect_slug != slug:
                return redirect(url_for("public_post", slug=redirect_slug, theme=theme), code=301)
            if not selected:
                abort(404)
        except Exception as exc:
            result = "error"
            log.exception("UI public post failed")
            telemetry.error("blog-ui", type(exc).__name__)
            raise
        finally:
            telemetry.api("/post/{slug}", "GET", result, (time.perf_counter() - started) * 1000.0)
    return render_template("public_index.html", **build_public_context(selected, posts, payload, active_theme=theme))


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
