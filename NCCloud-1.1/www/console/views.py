from django.http import HttpResponse
import ConfigParser

settingsFile = "/home/ialzuru/NCCloud-1.1/st/setting.cfg"

def adminConsole(request):
    ConsoleForm = """
        <form id='ConsoleForm'>
            <div>
                <label>
                    NCCloud service state 
                    <input name="on" value="unchecked" type="checkbox">
                </label>
                <button type=submit>On/Off</button> 
            </div>
            <div>
                <label>
                    Mounting directory
                    <input id="mountDir" name="mountDir" type="text" required>
                </label>
            </div>
        </form> 
        """
    
    cfg = ConfigParser.ConfigParser()
    cfg.read(settingsFile)
    n = cfg.get('global', 'totalnode')
    
    NodesForm1 = """
        There are %s Cloud nodes configured:
        <form id='NodesForm'>
            <table>
                <tr>
                    <td>Id</td>
                    <td>Type</td>
                    <td>Bucket Name</td>
                </tr>
                """  % n
                
    NodesForm2 = ""
    i = 0
    for i in range(int(n)):
        node = "node" + str(i)
        nodetype = cfg.get(node, 'nodetype')
        bucketname = cfg.get(node, 'bucketname')
        row = "<tr><td>%i</td><td>%s</td><td>%s</td></tr>" % (i, nodetype, bucketname)
        NodesForm2 = NodesForm2 + row
                        
    NodesForm3 = """
            </table>
        </form> 
        """
        
    NodesForm = NodesForm1 + NodesForm2 + NodesForm3

    html = """
        <!DOCTYPE html>
        <html>
            <head>
                <title>Console Menu</title>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
                <style>
                    table, th, td {
                        border: 1px solid black;
                        border-collapse: collapse;
                        padding: 5px;
                        text-align: center;
                    }
                </style>
            </head>
            <body>
                %s
                <br/>
                %s
            </body>
        </html> 
        """ % (ConsoleForm, NodesForm)
    return HttpResponse(html)