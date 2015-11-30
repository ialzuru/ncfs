from django import forms

class verifyForm(forms.Form):
    username = forms.CharField(max_length=100)
    #password = forms.PasswordInput()
    password = forms.CharField( widget=forms.PasswordInput(render_value = True) )