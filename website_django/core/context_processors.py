import json

from django.conf import settings
from core.auth import (
    get_auth_email,
    get_localpart_from_session,
    get_masked_email_from_session,
    is_logged_in,
)


def seo_defaults(request):
    canonical_url = request.build_absolute_uri(request.path)
    org_payload = {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": "SustainaCore",
        "url": settings.SITE_URL,
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
        "url": settings.SITE_URL,
    }
    return {
        "canonical_url": canonical_url,
        "default_meta_description": settings.DEFAULT_META_DESCRIPTION,
        "site_url": settings.SITE_URL,
        "static_version": settings.STATIC_VERSION,
        "telemetry_policy_version": settings.TELEMETRY_POLICY_VERSION,
        "org_json_ld": json.dumps(org_payload, ensure_ascii=True),
        "site_json_ld": json.dumps(site_payload, ensure_ascii=True),
    }


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
