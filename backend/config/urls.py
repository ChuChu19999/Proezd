from django.urls import path, include
from proezd.admin import admin_site

urlpatterns = [
    path("admin/", admin_site.urls),
    path("", include("proezd.urls")),
]
