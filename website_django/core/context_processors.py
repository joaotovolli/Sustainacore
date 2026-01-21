import json
from pathlib import Path

from django.conf import settings
from core.auth import (
    get_auth_email,
    get_localpart_from_session,
    get_masked_email_from_session,
    is_logged_in,
)


def _absolute_static_url(path: str) -> str:
    base = settings.SITE_URL.rstrip("/")
    static_url = settings.STATIC_URL or "/static/"
    if not static_url.startswith("/"):
        static_url = f"/{static_url}"
    return f"{base}{static_url}{path.lstrip('/')}"


def seo_defaults(request):
    canonical_url = request.build_absolute_uri(request.path)
    build_sha = _read_build_sha()
    logo_url = _absolute_static_url("img/sustainacore_logo_512.png")
    org_payload = {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": "SustainaCore",
        "url": settings.SITE_URL,
        "logo": {
            "@type": "ImageObject",
            "url": logo_url,
            "width": 512,
            "height": 512,
        },
        "image": logo_url,
        "contactPoint": {
            "@type": "ContactPoint",
            "email": "info@sustainacore.org",
            "contactType": "info",
        },
    }
    site_payload = {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": "SustainaCore",
        "alternateName": "SustainaCore.org",
        "url": settings.SITE_URL,
    }
    return {
        "canonical_url": canonical_url,
        "default_meta_description": settings.DEFAULT_META_DESCRIPTION,
        "site_url": settings.SITE_URL,
        "static_version": settings.STATIC_VERSION,
        "build_sha": build_sha,
        "telemetry_policy_version": settings.TELEMETRY_POLICY_VERSION,
        "org_json_ld": json.dumps(org_payload, ensure_ascii=True),
        "site_json_ld": json.dumps(site_payload, ensure_ascii=True),
        "seo_logo_url": logo_url,
        "seo_og_image_url": logo_url,
    }


_BUILD_SHA = None


def _read_build_sha() -> str:
    global _BUILD_SHA
    if _BUILD_SHA is not None:
        return _BUILD_SHA
    base_dir = Path(getattr(settings, "BASE_DIR", Path.cwd()))
    repo_root = base_dir.parent
    git_path = repo_root / ".git"
    if git_path.is_file():
        head_line = git_path.read_text().strip()
        if head_line.startswith("gitdir:"):
            git_dir = Path(head_line.split(":", 1)[1].strip())
        else:
            git_dir = git_path
    else:
        git_dir = git_path
    head_path = git_dir / "HEAD"
    try:
        head_text = head_path.read_text().strip()
        if head_text.startswith("ref:"):
            ref_path = repo_root / ".git" / head_text.split(" ", 1)[1].strip()
            sha = ref_path.read_text().strip()
        else:
            sha = head_text
        _BUILD_SHA = sha[:8] if sha else "unknown"
    except Exception:
        _BUILD_SHA = "unknown"
    return _BUILD_SHA


def build_sha(request):
    return {"build_sha": _read_build_sha()}


def preview_context(request):
    host = request.get_host().split(":")[0].lower()
    prod_hosts = {"sustainacore.org", "www.sustainacore.org"}
    preview_hosts = {h.lower() for h in settings.PREVIEW_HOSTS}
    host_is_preview = host in preview_hosts or host.startswith("preview.") or ".preview." in host
    env_is_preview = settings.SUSTAINACORE_ENV == "preview" or settings.PREVIEW_MODE
    show_preview_banner = (host not in prod_hosts) and (env_is_preview or host_is_preview)
    return {
        "is_preview": show_preview_banner,
        "show_preview_banner": show_preview_banner,
        "preview_host": host,
    }


def auth_context(request):
    return {
        "is_logged_in": is_logged_in(request),
        "auth_email_masked": get_masked_email_from_session(request),
        "auth_email": get_auth_email(request),
        "auth_email_local": get_localpart_from_session(request),
    }
