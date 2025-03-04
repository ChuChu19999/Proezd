from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import upload_potok, upload_propusk, generate_report
from .admin import admin_site

router = DefaultRouter()

urlpatterns = [
    path("api/", include(router.urls)),
    path("admin/", admin_site.urls),
    path("admin/upload-potok/", upload_potok, name="upload_potok"),
    path("admin/upload-propusk/", upload_propusk, name="upload_propusk"),
    path("admin/generate-report/", generate_report, name="generate_report"),
]
