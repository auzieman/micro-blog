import logging
import os
import time
import uuid

import requests
from flask import Flask, redirect, render_template, request, session, url_for
from opentelemetry.instrumentation.flask import FlaskInstrumentor

from blog_shared.observability import BlogTelemetry, configure_logging, event_scope

configure_logging()
logger = logging.getLogger("microblog.ui")
telemetry = BlogTelemetry("blog-ui")
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-me-for-real-deployments")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "false").lower() == "true"
app.config["PERMANENT_SESSION_LIFETIME"] = int(os.getenv("ADMIN_SESSION_SECONDS", "3600"))
FlaskInstrumentor().instrument_app(app)
API_BASE_URL = os.getenv("BLOG_API_BASE_URL", "http://localhost:8080")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "auzieman@gmail.com")
ADMIN_ACCESS_CODE = os.getenv("ADMIN_ACCESS_CODE", "local-admin")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
THEME_VARIANTS = ["aurora", "paper", "midnight"]
ADMIN_PREVIEW_TTL_SECONDS = int(os.getenv("ADMIN_PREVIEW_TTL_SECONDS", "1800"))
_ADMIN_PREVIEW_CACHE: dict[str, dict] = {}


def api_get(path: str, **params):
    return requests.get(f"{API_BASE_URL}{path}", params=params, timeout=10)


def api_post(path: str, payload: dict):
    payload.setdefault("admin_email", ADMIN_EMAIL)
    return requests.post(f"{API_BASE_URL}{path}", json=payload, timeout=10)


def api_put(path: str, payload: dict):
    payload.setdefault("admin_email", ADMIN_EMAIL)
    return requests.put(f"{API_BASE_URL}{path}", json=payload, timeout=10)


def is_admin_authenticated() -> bool:
    return session.get("admin_email") == ADMIN_EMAIL


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


def _store_preview_state(drupal_preview: list, drupal_endpoints: list, drupal_form: dict) -> None:
    _purge_preview_cache()
    token = session.get("admin_preview_token") or uuid.uuid4().hex
    session["admin_preview_token"] = token
    _ADMIN_PREVIEW_CACHE[token] = {
        "drupal_preview": drupal_preview,
        "drupal_endpoints": drupal_endpoints,
        "drupal_form": drupal_form,
        "expires_at": time.time() + ADMIN_PREVIEW_TTL_SECONDS,
    }


def _clear_preview_state() -> None:
    token = session.pop("admin_preview_token", None)
    if token:
        _ADMIN_PREVIEW_CACHE.pop(token, None)


def admin_context(message=None):
    preview_state = _load_preview_state()
    return {
        "admin_email": ADMIN_EMAIL,
        "auth_mode": "google" if GOOGLE_CLIENT_ID else "local-code",
        "message": message,
        "drupal_preview": preview_state.get("drupal_preview", []),
        "drupal_endpoints": preview_state.get("drupal_endpoints", []),
        "drupal_form": preview_state.get("drupal_form", {}),
    }


def fetch_public_payload(page: int, page_size: int, slug: str | None, tag: str | None):
    payload = {"items": [], "total": 0, "page": page, "page_size": page_size}
    posts = []
    selected = None
    response = api_get("/posts", page=page, page_size=page_size, tag=tag)
    response.raise_for_status()
    payload = response.json()
    posts = payload["items"]
    if slug:
        selected_response = api_get(f"/posts/{slug}")
        selected_response.raise_for_status()
        selected = selected_response.json()
    elif posts:
        selected_response = api_get(f"/posts/{posts[0]['slug']}")
        selected_response.raise_for_status()
        selected = selected_response.json()
    return payload, posts, selected


@app.after_request
def apply_security_headers(response):
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
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
    slug = request.args.get("slug")
    tag = request.args.get("tag")
    theme = request.args.get("theme")
    message = request.args.get("message")
    selected = None
    with event_scope(logger, "ui.public_index", page=page, page_size=page_size, slug=slug, tag=tag, theme=theme) as log:
        try:
            payload, posts, selected = fetch_public_payload(page, page_size, slug, tag)
        except Exception as exc:
            result = "error"
            log.exception("UI public index failed")
            telemetry.error("blog-ui", type(exc).__name__)
            payload = {"items": [], "total": 0, "page": page, "page_size": page_size}
            posts = []
            message = str(exc)
        finally:
            telemetry.api("/blog", "GET", result, (time.perf_counter() - started) * 1000.0)

    total_pages = max(1, (payload["total"] + payload["page_size"] - 1) // payload["page_size"])
    active_theme = theme or (selected.get("theme_variant") if selected else "aurora")
    return render_template(
        "public_index.html",
        posts=posts,
        selected=selected,
        total=payload["total"],
        page=payload["page"],
        page_size=payload["page_size"],
        total_pages=total_pages,
        page_sizes=[10, 20],
        tag=tag,
        active_theme=active_theme,
        theme_variants=THEME_VARIANTS,
        message=message,
        is_admin_authenticated=is_admin_authenticated(),
    )


@app.get("/post/<slug>")
def public_post(slug: str):
    return redirect(url_for("public_index", slug=slug))


@app.get("/admin")
def admin_index():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", next=request.path))

    started = time.perf_counter()
    result = "success"
    page = int(request.args.get("page", "1"))
    page_size = 10
    message = request.args.get("message")
    posts = []
    payload = {"items": [], "total": 0, "page": page, "page_size": page_size}
    with event_scope(logger, "ui.admin_index", page=page) as log:
        try:
            payload, posts, _selected = fetch_public_payload(page, page_size, None, None)
        except Exception as exc:
            result = "error"
            log.exception("UI admin failed")
            telemetry.error("blog-ui", type(exc).__name__)
            message = str(exc)
        finally:
            telemetry.api("/admin", "GET", result, (time.perf_counter() - started) * 1000.0)

    return render_template(
        "admin.html",
        posts=posts,
        total=payload["total"],
        theme_variants=THEME_VARIANTS,
        **admin_context(message),
    )


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
        if GOOGLE_CLIENT_ID:
            result = "error"
            message = "Google auth is configured for deployment work but not wired into this local build yet."
        elif email == ADMIN_EMAIL.lower() and access_code == ADMIN_ACCESS_CODE:
            session.clear()
            session.permanent = True
            session["admin_email"] = ADMIN_EMAIL
            message = "Admin session established."
            telemetry.api("/admin/login", "POST", result, (time.perf_counter() - started) * 1000.0)
            return redirect(url_for("admin_index", message=message))
        else:
            result = "denied"
            message = "Admin access denied."
            log.warning("Admin login denied")

        telemetry.api("/admin/login", "POST", result, (time.perf_counter() - started) * 1000.0)
        return render_template("admin_login.html", **admin_context(message)), 401


@app.post("/admin/logout")
def admin_logout():
    _clear_preview_state()
    session.clear()
    return redirect(url_for("public_index", message="Admin session cleared."))


@app.post("/admin/create")
def create_post():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    started = time.perf_counter()
    result = "success"
    payload = {
        "title": request.form["title"],
        "summary": request.form["summary"],
        "markdown_body": request.form["markdown_body"],
        "body_format": request.form.get("body_format", "markdown"),
        "hero_image_url": request.form.get("hero_image_url") or None,
        "theme_variant": request.form.get("theme_variant", "aurora"),
        "tags": [part.strip() for part in request.form["tags"].split(",") if part.strip()],
        "status": request.form.get("status", "draft"),
    }
    with event_scope(logger, "ui.create") as log:
        try:
            response = api_post("/admin/posts", payload)
            message = f"Create status: {response.status_code} {response.text}"
        except Exception as exc:
            result = "error"
            log.exception("UI create failed")
            telemetry.error("blog-ui", type(exc).__name__)
            message = str(exc)
        finally:
            telemetry.api("/admin/create", "POST", result, (time.perf_counter() - started) * 1000.0)
    return redirect(url_for("admin_index", message=message))


@app.post("/admin/publish")
def publish_post():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    article_id = request.form["article_id"]
    started = time.perf_counter()
    result = "success"
    with event_scope(logger, "ui.publish", article_id=article_id) as log:
        try:
            response = api_post(f"/admin/posts/{article_id}/publish", {})
            message = f"Publish status: {response.status_code} {response.text}"
        except Exception as exc:
            result = "error"
            log.exception("UI publish failed")
            telemetry.error("blog-ui", type(exc).__name__)
            message = str(exc)
        finally:
            telemetry.api("/admin/publish", "POST", result, (time.perf_counter() - started) * 1000.0)
    return redirect(url_for("admin_index", message=message))


@app.post("/admin/import-sample")
def import_sample():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    started = time.perf_counter()
    result = "success"
    with event_scope(logger, "ui.import_sample") as log:
        try:
            response = api_post("/admin/import-sample", {})
            message = f"Import status: {response.status_code} {response.text}"
        except Exception as exc:
            result = "error"
            log.exception("UI sample import failed")
            telemetry.error("blog-ui", type(exc).__name__)
            message = str(exc)
        finally:
            telemetry.api("/admin/import-sample", "POST", result, (time.perf_counter() - started) * 1000.0)
    return redirect(url_for("admin_index", message=message))


@app.post("/admin/import/drupal/preview")
def preview_drupal_import():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    started = time.perf_counter()
    result = "success"
    payload = {
        "endpoint_url": request.form["endpoint_url"],
        "source_base_url": request.form.get("source_base_url") or "",
        "status": request.form.get("status", "draft"),
        "body_format": request.form.get("body_format") or None,
        "theme_variant": request.form.get("theme_variant", "aurora"),
        "allow_insecure_tls": request.form.get("allow_insecure_tls") == "on",
        "params": {},
        "dry_run": True,
    }
    if request.form.get("keyword_filter"):
        payload["keyword_filter"] = request.form["keyword_filter"]
    if request.form.get("include_value"):
        payload["params"]["include"] = request.form["include_value"]
    if request.form.get("page_limit"):
        payload["params"]["page[limit]"] = request.form["page_limit"]

    with event_scope(logger, "ui.drupal_preview", endpoint_url=payload["endpoint_url"]) as log:
        try:
            response = api_post("/admin/import/drupal", payload)
            response.raise_for_status()
            preview_payload = response.json()
            _store_preview_state(
                preview_payload.get("items", []),
                preview_payload.get("endpoints", []),
                {
                "endpoint_url": preview_payload.get("endpoint_url", payload["endpoint_url"]),
                "source_base_url": payload["source_base_url"],
                "include_value": request.form.get("include_value", ""),
                "page_limit": request.form.get("page_limit", ""),
                "keyword_filter": request.form.get("keyword_filter", ""),
                "status": payload["status"],
                "body_format": request.form.get("body_format", ""),
                "theme_variant": payload["theme_variant"],
                "allow_insecure_tls": payload["allow_insecure_tls"],
                },
            )
            if preview_payload.get("status") == "DrupalEndpointDiscovery":
                message = f"Discovery loaded: {preview_payload.get('count', 0)} JSON:API endpoints."
            else:
                message = f"Preview loaded: {preview_payload.get('count', 0)} candidate articles."
        except Exception as exc:
            result = "error"
            log.exception("Drupal preview failed")
            telemetry.error("blog-ui", type(exc).__name__)
            message = str(exc)
        finally:
            telemetry.api("/admin/import/drupal/preview", "POST", result, (time.perf_counter() - started) * 1000.0)
    return redirect(url_for("admin_index", message=message))


@app.post("/admin/import/drupal")
def import_drupal_selection():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login", message="Admin authentication required."))
    started = time.perf_counter()
    result = "success"
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
    if drupal_form.get("keyword_filter"):
        payload["keyword_filter"] = drupal_form["keyword_filter"]
    if drupal_form.get("body_format"):
        payload["body_format"] = drupal_form["body_format"]
    if drupal_form.get("include_value"):
        payload["params"]["include"] = drupal_form["include_value"]
    if drupal_form.get("page_limit"):
        payload["params"]["page[limit]"] = drupal_form["page_limit"]

    with event_scope(logger, "ui.drupal_import", selected_count=len(selected_ids)) as log:
        try:
            if not payload.get("endpoint_url"):
                raise ValueError("Drupal preview data is missing. Run preview first.")
            response = api_post("/admin/import/drupal", payload)
            response.raise_for_status()
            import_payload = response.json()
            _clear_preview_state()
            message = f"Drupal import queued: {import_payload.get('count', 0)} selected articles."
        except Exception as exc:
            result = "error"
            log.exception("Drupal import failed")
            telemetry.error("blog-ui", type(exc).__name__)
            message = str(exc)
        finally:
            telemetry.api("/admin/import/drupal", "POST", result, (time.perf_counter() - started) * 1000.0)
    return redirect(url_for("admin_index", message=message))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
