from django.shortcuts import render
from django.http import HttpResponseRedirect

from .forms import verifyForm

def verifyAdmin(request):
    if request.method == 'POST':
        form = verifyForm( request.POST )

        if form.is_valid():
            return HttpResponseRedirect('')

    else:
        form = verifyForm()

    return render(request, 'login.html', {'form': form})