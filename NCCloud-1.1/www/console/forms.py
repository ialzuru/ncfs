from django import forms

class consoleForm(forms.Form):
    on = forms.BooleanField(required=False)
    mountDir = forms.CharField(max_length=140)
    filename = forms.FileField()