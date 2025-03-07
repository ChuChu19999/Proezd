from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import upload_potok, upload_propusk, generate_report
from .admin import admin_site

router = DefaultRouter()

urlpatterns = [
    path("", include(router.urls)),
    path("upload-potok/", upload_potok, name="upload_potok"),
    path("upload-propusk/", upload_propusk, name="upload_propusk"),
    path("generate-report/", generate_report, name="generate_report"),
]
