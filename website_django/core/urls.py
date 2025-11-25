from django.contrib import admin
from django.urls import path

from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("lab/", views.lab, name="lab"),
    path("methodology/", views.methodology, name="methodology"),
    path("privacy/", views.privacy, name="privacy"),
    path('admin/', admin.site.urls),
]
