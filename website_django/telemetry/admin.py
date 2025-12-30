from django.contrib import admin

from telemetry.models import WebConsent, WebEvent, WebSession


@admin.register(WebConsent)
class WebConsentAdmin(admin.ModelAdmin):
    list_display = ("consent_id", "created_ts", "user_id", "consent_analytics", "consent_policy_version", "source")
    list_filter = ("consent_analytics", "consent_policy_version", "source", "created_ts")
    search_fields = ("user_agent", "ip_hash", "ip_trunc")


@admin.register(WebEvent)
class WebEventAdmin(admin.ModelAdmin):
    list_display = ("event_id", "event_ts", "event_type", "path", "status_code", "consent_analytics_effective")
    list_filter = ("event_type", "consent_analytics_effective", "event_ts")
    search_fields = ("path", "referrer", "user_agent", "ip_hash")


@admin.register(WebSession)
class WebSessionAdmin(admin.ModelAdmin):
    list_display = ("session_row_id", "session_key", "first_seen_ts", "last_seen_ts")
    list_filter = ("first_seen_ts", "last_seen_ts")
    search_fields = ("session_key", "user_agent", "ip_hash")
