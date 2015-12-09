from django import forms

class consoleForm(forms.Form):
    on = forms.BooleanField(required=False)
    mountDir = forms.CharField(max_length=140)
    filename = forms.FileField()
    
class cloudForm(forms.Form):
    nodetype = forms.CharField(max_length=50)
    nodeloc = forms.CharField(max_length=50)
    accesskey = forms.CharField(max_length=200)
    secretkey = forms.CharField(max_length=200)
    nodeid = forms.IntegerField()
    nodekey = forms.IntegerField()
    bucketname = forms.CharField(max_length=140)
    