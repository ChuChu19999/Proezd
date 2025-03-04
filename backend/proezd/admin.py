from django.contrib import admin
from django.contrib.auth.models import User, Group
from django.urls import path
from django.utils.html import format_html
from .views import (
    upload_potok,
    upload_propusk,
    analyze_numbers,
    replace_numbers,
    generate_report,
)


class CustomAdminSite(admin.AdminSite):
    site_header = "Администрирование проездов"
    site_title = "Администрирование проездов"
    index_title = "Управление проездами"

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("upload-potok/", self.admin_view(upload_potok), name="upload_potok"),
            path(
                "upload-propusk/",
                self.admin_view(upload_propusk),
                name="upload_propusk",
            ),
            path(
                "analyze-numbers/",
                self.admin_view(analyze_numbers),
                name="analyze_numbers",
            ),
            path(
                "replace-numbers/",
                self.admin_view(replace_numbers),
                name="replace_numbers",
            ),
            path(
                "generate-report/",
                self.admin_view(generate_report),
                name="generate_report",
            ),
        ]
        return custom_urls + urls

    def index(self, request, extra_context=None):
        extra_context = extra_context or {}

        # Кнопки для загрузки файлов
        upload_buttons = format_html(
            '<div style="display: flex; gap: 10px;">'
            '<a class="button" style="background: var(--button-bg); padding: 10px 15px; border-radius: 4px; color: var(--button-fg); text-decoration: none; display: inline-block; font-weight: bold;" href="{}">Загрузить файл проездов</a>'
            '<a class="button" style="background: var(--button-bg); padding: 10px 15px; border-radius: 4px; color: var(--button-fg); text-decoration: none; display: inline-block; font-weight: bold;" href="{}">Загрузить файл пропусков</a>'
            '<a class="button" style="background: var(--button-bg); padding: 10px 15px; border-radius: 4px; color: var(--button-fg); text-decoration: none; display: inline-block; font-weight: bold;" href="{}">Сформировать отчет</a>'
            "</div>",
            "upload-potok/",
            "upload-propusk/",
            "generate-report/",
        )

        # Кнопка для замены номеров в отдельном блоке
        analyze_button = format_html(
            '<div class="module" style="margin-top: 20px;">'
            "<h2>Инструменты анализа</h2>"
            '<div style="margin: 10px 0;">'
            '<p style="margin-bottom: 10px;">Инструмент для анализа и замены неверно распознанных номеров на предположительно правильные варианты</p>'
            '<a class="button" style="background: var(--primary); padding: 10px 15px; border-radius: 4px; color: white; text-decoration: none; display: inline-block; font-weight: bold;" href="{}">'
            "Заменить неверные номера на предположительные"
            "</a>"
            "</div>"
            "</div>",
            "analyze-numbers/",
        )

        extra_context["upload_button"] = upload_buttons
        extra_context["analyze_button"] = analyze_button
        return super().index(request, extra_context)


admin_site = CustomAdminSite(name="admin")

admin_site.register(User)
admin_site.register(Group)
