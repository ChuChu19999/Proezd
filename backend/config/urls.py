from django.urls import path, include
from proezd.admin import admin_site

urlpatterns = [
    path("", admin_site.urls),
    path("api/", include("proezd.urls")),
]
