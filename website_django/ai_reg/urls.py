from django.urls import path

from ai_reg import views

urlpatterns = [
    path("", views.ai_regulation_page, name="ai_regulation"),
    path("data/as-of-dates", views.ai_reg_as_of_dates, name="ai_reg_as_of_dates"),
    path("data/heatmap", views.ai_reg_heatmap, name="ai_reg_heatmap"),
    path("data/jurisdiction/<str:iso2>/", views.ai_reg_jurisdiction, name="ai_reg_jurisdiction"),
    path(
        "data/jurisdiction/<str:iso2>/instruments",
        views.ai_reg_jurisdiction_instruments,
        name="ai_reg_jurisdiction_instruments",
    ),
    path(
        "data/jurisdiction/<str:iso2>/timeline",
        views.ai_reg_jurisdiction_timeline,
        name="ai_reg_jurisdiction_timeline",
    ),
]
