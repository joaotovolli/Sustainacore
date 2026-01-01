from __future__ import annotations

from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_POST

from sc_admin_portal.auth import portal_not_found, require_sc_admin
from sc_admin_portal.models import SocialDraftPost


@never_cache
@require_sc_admin
def dashboard(request):
    error = ""
    if request.method == "POST":
        title = (request.POST.get("title") or "").strip()
        body_text = (request.POST.get("body_text") or "").strip()
        hashtags = (request.POST.get("hashtags") or "").strip()
        platform = (request.POST.get("platform") or "").strip() or "linkedin_page"
        if not body_text:
            error = "Body text is required."
        else:
            SocialDraftPost.objects.create(
                title=title,
                body_text=body_text,
                hashtags=hashtags,
                platform=platform,
                status=SocialDraftPost.STATUS_DRAFT,
            )
    drafts = SocialDraftPost.objects.filter(status=SocialDraftPost.STATUS_DRAFT)
    return render(
        request,
        "sc_admin_portal/admin_portal.html",
        {"drafts": drafts, "error": error},
    )


@never_cache
@require_sc_admin
@require_POST
def approve_draft(request, draft_id: int):
    draft = SocialDraftPost.objects.filter(pk=draft_id).first()
    if not draft:
        return portal_not_found()
    draft.status = SocialDraftPost.STATUS_APPROVED
    draft.approved_at = timezone.now()
    draft.save(update_fields=["status", "approved_at", "updated_at"])
    return redirect("sc_admin_portal:dashboard")


@never_cache
@require_sc_admin
@require_POST
def reject_draft(request, draft_id: int):
    draft = SocialDraftPost.objects.filter(pk=draft_id).first()
    if not draft:
        return portal_not_found()
    draft.status = SocialDraftPost.STATUS_REJECTED
    draft.approved_at = None
    draft.save(update_fields=["status", "approved_at", "updated_at"])
    return redirect("sc_admin_portal:dashboard")
