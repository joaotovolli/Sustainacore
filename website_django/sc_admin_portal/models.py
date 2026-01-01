from django.db import models


class SocialDraftPost(models.Model):
    STATUS_DRAFT = "DRAFT"
    STATUS_APPROVED = "APPROVED"
    STATUS_REJECTED = "REJECTED"

    STATUS_CHOICES = [
        (STATUS_DRAFT, "Draft"),
        (STATUS_APPROVED, "Approved"),
        (STATUS_REJECTED, "Rejected"),
    ]

    title = models.CharField(max_length=200, blank=True)
    body_text = models.TextField()
    hashtags = models.TextField(blank=True)
    platform = models.CharField(max_length=64, default="linkedin_page")
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_DRAFT)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    approved_at = models.DateTimeField(null=True, blank=True)
    published_url = models.URLField(blank=True, null=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        if self.title:
            return self.title
        return f"Draft {self.pk}"

    @property
    def copy_ready(self) -> str:
        body = (self.body_text or "").rstrip()
        hashtags = (self.hashtags or "").strip()
        if hashtags:
            if body:
                return f"{body}\n\n{hashtags}"
            return hashtags
        return body
