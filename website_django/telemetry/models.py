import uuid

from django.db import models


class WebConsent(models.Model):
    consent_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user_id = models.IntegerField(null=True, blank=True)
    created_ts = models.DateTimeField(auto_now_add=True)
    updated_ts = models.DateTimeField(auto_now=True)
    consent_analytics = models.CharField(max_length=1, default="N")
    consent_functional = models.CharField(max_length=1, default="N")
    consent_policy_version = models.CharField(max_length=32)
    source = models.CharField(max_length=64, default="banner")
    user_agent = models.CharField(max_length=512, null=True, blank=True)
    ip_trunc = models.CharField(max_length=64, null=True, blank=True)
    ip_hash = models.CharField(max_length=128, null=True, blank=True)

    class Meta:
        db_table = "W_WEB_CONSENT"


class WebSession(models.Model):
    session_row_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    session_key = models.CharField(max_length=64, null=True, blank=True)
    user_id = models.IntegerField(null=True, blank=True)
    first_seen_ts = models.DateTimeField()
    last_seen_ts = models.DateTimeField()
    country_code = models.CharField(max_length=8, null=True, blank=True)
    region_code = models.CharField(max_length=16, null=True, blank=True)
    user_agent = models.CharField(max_length=512, null=True, blank=True)
    ip_hash = models.CharField(max_length=128, null=True, blank=True)

    class Meta:
        db_table = "W_WEB_SESSION"


class WebEvent(models.Model):
    event_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    event_ts = models.DateTimeField()
    user_id = models.IntegerField(null=True, blank=True)
    session_key = models.CharField(max_length=64, null=True, blank=True)
    consent_analytics_effective = models.CharField(max_length=1, default="N")
    event_type = models.CharField(max_length=64)
    event_name = models.CharField(max_length=64, null=True, blank=True)
    path = models.CharField(max_length=512)
    query_string = models.TextField(null=True, blank=True)
    http_method = models.CharField(max_length=16, null=True, blank=True)
    status_code = models.IntegerField(null=True, blank=True)
    response_ms = models.IntegerField(null=True, blank=True)
    referrer = models.CharField(max_length=512, null=True, blank=True)
    referrer_host = models.CharField(max_length=255, null=True, blank=True)
    user_agent = models.CharField(max_length=512, null=True, blank=True)
    is_bot = models.CharField(max_length=1, default="N")
    ip_trunc = models.CharField(max_length=64, null=True, blank=True)
    ip_hash = models.CharField(max_length=128, null=True, blank=True)
    country_code = models.CharField(max_length=8, null=True, blank=True)
    region_code = models.CharField(max_length=16, null=True, blank=True)
    payload_json = models.TextField(null=True, blank=True)
    debug_json = models.CharField(max_length=2000, null=True, blank=True)

    class Meta:
        db_table = "W_WEB_EVENT"


class WebEventDaily(models.Model):
    aggregate_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    bucket_date = models.DateField()
    event_type = models.CharField(max_length=64)
    event_name = models.CharField(max_length=64, null=True, blank=True)
    path = models.CharField(max_length=512)
    path_hash = models.CharField(max_length=64)
    referrer_host = models.CharField(max_length=255, null=True, blank=True)
    country_code = models.CharField(max_length=8, null=True, blank=True)
    consent_analytics_effective = models.CharField(max_length=1, default="N")
    is_bot = models.CharField(max_length=1, default="N")
    status_class = models.CharField(max_length=8, null=True, blank=True)
    event_count = models.IntegerField(default=0)
    unique_sessions = models.IntegerField(default=0)
    unique_users = models.IntegerField(default=0)
    unique_visitors = models.IntegerField(default=0)
    total_response_ms = models.IntegerField(default=0)
    max_response_ms = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = "W_WEB_EVENT_DAILY"
        indexes = [
            models.Index(
                fields=["bucket_date", "event_type"],
                name="W_WEB_EVENT_bucket__f8d81c_idx",
            ),
            models.Index(
                fields=["bucket_date", "path_hash"],
                name="W_WEB_EVENT_bucket__4538b0_idx",
            ),
        ]


class WebAsk2Conversation(models.Model):
    conversation_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField()
    last_message_at = models.DateTimeField()
    user_id = models.IntegerField(null=True, blank=True)
    session_key = models.CharField(max_length=64, null=True, blank=True)
    anon_id = models.CharField(max_length=64, null=True, blank=True)
    ip_hash = models.CharField(max_length=128, null=True, blank=True)
    ip_prefix = models.CharField(max_length=64, null=True, blank=True)
    user_agent = models.CharField(max_length=512, null=True, blank=True)
    path_first = models.CharField(max_length=512, null=True, blank=True)
    consent_analytics_effective = models.CharField(max_length=1, null=True, blank=True)
    metadata_json = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "W_WEB_ASK2_CONVERSATION"


class WebAsk2Message(models.Model):
    message_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    conversation = models.ForeignKey(
        WebAsk2Conversation,
        on_delete=models.CASCADE,
        db_column="conversation_id",
        related_name="messages",
    )
    created_at = models.DateTimeField()
    role = models.CharField(max_length=16)
    content = models.TextField()
    content_len = models.IntegerField()
    model_name = models.CharField(max_length=128, null=True, blank=True)
    latency_ms = models.IntegerField(null=True, blank=True)
    tokens_in = models.IntegerField(null=True, blank=True)
    tokens_out = models.IntegerField(null=True, blank=True)
    request_id = models.CharField(max_length=64, null=True, blank=True)
    status = models.CharField(max_length=32, null=True, blank=True)
    error_class = models.CharField(max_length=128, null=True, blank=True)
    error_msg = models.CharField(max_length=512, null=True, blank=True)

    class Meta:
        db_table = "W_WEB_ASK2_MESSAGE"
