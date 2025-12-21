"""
URL configuration for core project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.contrib.sitemaps.views import sitemap
from django.urls import include, path

from . import views
from . import sitemaps
from . import tech100_index_views

sitemaps_config = {
    "static": sitemaps.StaticViewSitemap,
}

urlpatterns = [
    path("robots.txt", views.robots_txt, name="robots_txt"),
    path("sitemap.xml", views.sitemap_xml, name="sitemap"),
    path("sitemap.xml", sitemap, {"sitemaps": sitemaps_config}, name="sitemap"),
    path("", views.home, name="home"),
    path("press/", views.press_index, name="press_index"),
    path("press/tech100/", views.press_tech100, name="press_tech100"),
    path("tech100/", views.tech100, name="tech100"),
    path("tech100/index/", tech100_index_views.tech100_index_overview, name="tech100_index"),
    path("tech100/performance/", tech100_index_views.tech100_performance, name="tech100_performance"),
    path("tech100/constituents/", tech100_index_views.tech100_constituents, name="tech100_constituents"),
    path("tech100/attribution/", tech100_index_views.tech100_attribution, name="tech100_attribution"),
    path("tech100/stats/", tech100_index_views.tech100_stats, name="tech100_stats"),
    path("tech100/export/", views.tech100_export, name="tech100_export"),
    path("api/tech100/index-levels", tech100_index_views.api_tech100_index_levels),
    path("api/tech100/index/attribution", tech100_index_views.api_tech100_performance_attribution),
    path("api/tech100/index/holdings", tech100_index_views.api_tech100_holdings),
    path("api/tech100/kpis", tech100_index_views.api_tech100_kpis),
    path("api/tech100/constituents", tech100_index_views.api_tech100_constituents),
    path("api/tech100/attribution", tech100_index_views.api_tech100_attribution),
    path("api/tech100/stats", tech100_index_views.api_tech100_stats),
    path("tech100/index-levels/", tech100_index_views.api_tech100_index_levels, name="tech100_index_levels_api"),
    path(
        "tech100/performance/attribution/",
        tech100_index_views.api_tech100_performance_attribution,
        name="tech100_performance_attribution_api",
    ),
    path(
        "tech100/constituents/data/",
        tech100_index_views.api_tech100_constituents,
        name="tech100_constituents_api",
    ),
    path(
        "tech100/attribution/data/",
        tech100_index_views.api_tech100_attribution,
        name="tech100_attribution_api",
    ),
    path("tech100/stats/data/", tech100_index_views.api_tech100_stats, name="tech100_stats_api"),
    path("news/", views.news, name="news"),
    path("news/admin/", views.news_admin, name="news_admin"),
    path("news/<str:news_id>/", views.news_detail, name="news_detail"),
    path("admin/", admin.site.urls),
    path("", include("ask2.urls")),
]
