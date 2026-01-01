from django.urls import path

from sc_admin_portal import views

app_name = "sc_admin_portal"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("approve/<int:draft_id>/", views.approve_draft, name="approve"),
    path("reject/<int:draft_id>/", views.reject_draft, name="reject"),
]
