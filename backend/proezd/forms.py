from django import forms

PROPUSK_TYPES = [
    ("kronos", "База кронос"),
    ("razoviy", "Разовые пропуски"),
    ("gdya", "Список «ГДЯ»"),
]


class PotokUploadForm(forms.Form):
    file = forms.FileField(label="Excel файл", help_text="Выберите файл формата .xlsx")
    date_from = forms.DateTimeField(
        label="Дата с",
        widget=forms.DateTimeInput(
            attrs={"type": "datetime-local", "step": "1"}, format="%Y-%m-%dT%H:%M:%S"
        ),
        input_formats=["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"],
    )
    date_to = forms.DateTimeField(
        label="Дата по",
        widget=forms.DateTimeInput(
            attrs={"type": "datetime-local", "step": "1"}, format="%Y-%m-%dT%H:%M:%S"
        ),
        input_formats=["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"],
    )

    def clean_file(self):
        file = self.cleaned_data["file"]
        if not file.name.endswith(".xlsx"):
            raise forms.ValidationError("Файл должен быть формата .xlsx")
        return file


class PropuskUploadForm(forms.Form):
    file = forms.FileField(label="Excel файл", help_text="Выберите файл формата .xlsx")
    propusk_type = forms.ChoiceField(
        label="Тип пропуска",
        choices=PROPUSK_TYPES,
        widget=forms.Select(attrs={"class": "form-control"}),
    )

    def clean_file(self):
        file = self.cleaned_data["file"]
        if not file.name.endswith(".xlsx"):
            raise forms.ValidationError("Файл должен быть формата .xlsx")
        return file
