from django.urls import path

from sc_admin_portal import views

app_name = "sc_admin_portal"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("approvals/<int:approval_id>/approve/", views.approve_approval, name="approve"),
    path("approvals/<int:approval_id>/approve", views.approve_approval, name="approve_no_slash"),
    path("approvals/<int:approval_id>/reject/", views.reject_approval, name="reject"),
    path("approvals/<int:approval_id>/reject", views.reject_approval, name="reject_no_slash"),
    path("jobs/<int:job_id>/file/", views.job_file, name="job_file"),
    path("approval/<int:approval_id>/file/", views.approval_file, name="approval_file"),
]
