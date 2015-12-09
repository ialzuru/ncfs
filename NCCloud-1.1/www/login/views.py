from django.shortcuts import render 
from django.http import HttpResponseRedirect

from .forms import verifyForm

def verifyAdmin(request):
    if request.method == 'POST':
        form = verifyForm( request.POST )

        if form.is_valid():
            username = request.POST['username']
            password = request.POST['password']
            
            if (username == 'admin') and (password == 'closser'):
                return HttpResponseRedirect('/console/')
            else:
                return render(request, 'login.html', {'error': True})

    else:
        form = verifyForm()

    return render(request, 'login.html', {'error': False})