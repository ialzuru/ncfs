from django.shortcuts import render
import ConfigParser
import subprocess

from .forms import cloudForm

settingsFile = "/home/ialzuru/NCCloud-1.1/st/setting.cfg"

def cloudModif(request):
    if request.method == 'POST':
        form = cloudForm( request.POST or None )

        if form.is_valid():
            print 'Entra'
            #username = request.POST['username']
            #password = request.POST['password']
        else:
            cfg = ConfigParser.ConfigParser()
            cfg.read(settingsFile)
            btn = ''
            if 'node0' in request.POST:
                btn = 'node0'
            elif 'node1' in request.POST:
                btn = 'node1'
            elif 'node2' in request.POST:
                btn = 'node2'
            
            form = cloudForm( initial= {'nodetype':cfg.get(btn, 'nodetype'), 
                                         'nodeloc':cfg.get(btn, 'nodeloc'), 
                                         'accesskey':cfg.get(btn, 'accesskey'), 
                                         'secretkey':cfg.get(btn, 'secretkey'), 
                                         'nodeid':cfg.get(btn, 'nodeid'), 
                                         'nodekey':cfg.get(btn, 'nodekey'), 
                                         'bucketname':cfg.get(btn, 'bucketname')
                                         })
    else:
        form = cloudForm()
    
    return render(request, "cloud.html", { 'form': form })


def findThisProcess( process_name ):
    ps     = subprocess.Popen("ps -eaf | grep "+process_name, shell=True, stdout=subprocess.PIPE)
    output = ps.stdout.read()
    ps.stdout.close()
    ps.wait()
    
    return output


def adminConsole(request):
    cfg = ConfigParser.ConfigParser()
    cfg.read(settingsFile)
    n = cfg.get('global', 'totalnode')
    
    dict_clouds = dict()
    i = 0
    for i in range(int(n)):
        node = "node" + str(i)
        nodetype = cfg.get(node, 'nodetype')
        bucketname = cfg.get(node, 'bucketname')
        dict_clouds[i] = [ nodetype, bucketname ]
    
    pointerDir = cfg.get('global', 'pointerdir')
    deduplication = cfg.get('global', 'deduplication')
    dedup = ''
    if deduplication == 'True':
        dedup = ' checked'

    context = {
        'n': n,
        'dict_clouds': dict_clouds,
        'pointerDir': pointerDir,
        'dedup': dedup
    }

    return render(request, "console.html", context)


