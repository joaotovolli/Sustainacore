from django.urls import path

from . import views

app_name = "ask2"

urlpatterns = [
    path("ask2/", views.ask2_page, name="ask2_page"),
    path("ask2/api/", views.ask2_api, name="ask2_api"),
]
