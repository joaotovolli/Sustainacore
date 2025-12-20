import json

from django.conf import settings


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
        "org_json_ld": json.dumps(org_payload, ensure_ascii=True),
        "site_json_ld": json.dumps(site_payload, ensure_ascii=True),
    }


def preview_context(request):
    host = request.get_host().split(":")[0].lower()
    is_preview = settings.PREVIEW_MODE or host in {h.lower() for h in settings.PREVIEW_HOSTS}
    return {
        "is_preview": is_preview,
        "preview_host": host,
    }
