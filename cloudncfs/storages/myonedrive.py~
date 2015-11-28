#!/usr/bin/python
#
# @name = 'onedrive.py'
#
# @description = 'onedrive storage module'
#
# @author = ['Huixiang Chen']

from __future__ import unicode_literals

import onedrivesdk
from onedrivesdk.helpers import GetAuthCodeServer
from PIL import Image
import os
import sys
import common
import storageutil


print "try to establish onedrive connection"
#onedrive connection: secretkey is the dropbox access token
#onedrive connection: accesskey is the client_id, secretkey is the client_secret
redirect_uri = "http://localhost:8080/"
#hardcode here#
current_client_id = "000000004C172140"
current_client_secret = "lqqj0dO80m9FQVUzYfghPlN0tiUwCjx5"

client = onedrivesdk.get_default_client(client_id=current_client_id,
                                            scopes=['wl.signin',
                                                    'wl.offline_access',
                                                    'onedrive.readwrite'])

auth_url = client.auth_provider.get_auth_url(redirect_uri)
# Block thread until we have the code
code = GetAuthCodeServer.get_auth_code(auth_url, redirect_uri)
# Finally, authenticate!
client.auth_provider.authenticate(code, redirect_uri, current_client_secret)



'''utility functions'''
def navigate(client, current_path):
    items = client.item(path=current_path).children.get()
    for count, item in enumerate(items):
        print("{} {}".format(count+1, item.name if item.folder is None else "/"+item.name))
    return items

def upload(client, bucketname, name, directory):
    client.item(path=bucketname).children[name].upload(directory)

def download(client, bucketname, name, directory):
    client.item(path=bucketname).children[name].download(directory)

def delete(client, curpath):
    client.item(path=curpath).delete()
'''end'''


def syncMirror(setting, nodeid, path):
    '''Synchronize file entries on cloud to local directory.'''
    #OneDrive connection:
    print "calling syncMirror"
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    items = navigate(conn, bucketname)
    for item in items:
        filename = item.name
        if setting.coding == 'replication':
            if filename.endswith('.node0'):
              storageutil.syncFile(setting, filename, path)
        else:
            if filename.endswith('.node%d' % nodeid):
              storageutil.syncFile(setting, filename, path)
    return True


def checkHealth(setting, nodeid):
    print "call checkhealth in onedrive"
    #conn = connectCloud(setting, nodeid)
    conn = client
    if conn == False:
        return False
    else:
        bucketname = setting.nodeInfo[nodeid].bucketname
        try:
            split_path = bucketname.split('/')
            parent_split_path = split_path[0:-1]
            folder_name = split_path[-1]
            if folder_name == '':
                print "Error: please input a bucketname under the dropbox app directory"
                return False
            parent_path = '/'.join(parent_split_path)
            if parent_path == '':
                parent_path = '/'
            items = navigate(conn, parent_path)
            for item in items:
                if item.name == folder_name:
                    print "successfully found the bucket"
                    return True
            print "Fail to connect to ", bucketname
            return False
        except:
            return False

def uploadFile(setting, nodeid, localpath, dest):
    '''Upload a file to cloud'''
    #OneDrive connection:
    print "Calling uploadfile in onedrive"
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    #upload a file to the bucket
    try:
        #conn.item(path=bucketname).children[dest].upload(localpath)
        upload(conn, bucketname, dest, localpath)
        return True
    except IOError:
        print >> sys.stderr, "Error: The destination location is not writable."
        return False

def uploadMetadata(setting, nodeid, metadatapath, dest):
    '''Upload metadata to cloud.'''
    #OneDrive connection:
    print "Calling uploadmetadata in onedrive"
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    try:
        upload(conn, bucketname, dest+'.metadata', metadatapath)
        return True
    except IOError:
        print >> sys.stderr, "Error: The destination location is not writable."   
        return False

def uploadFileAndMetadata(setting, nodeid, localpath, metadatapath, dest):
    '''Upload file and metadata to cloud'''
    #OneDrive connection:
    print "Calling uploadfileandMetadata in onedrive"
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    try:
        upload(conn, bucketname, dest, localpath)
        upload(conn, bucketname, dest+'.metadata', metadatapath)
        return True
    except IOError:
        print >> sys.stderr, "Error: The destination location is not writable."
        return False

def downloadFile(setting, nodeid, src, path):
    '''Download an object from cloud. '''
    #OneDrive connection:
    print "Calling downloadfile in onedrive"
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Download object from bucket:
    try:
        conn.item(path=bucketname).children[src].download(path)
        return True
    except:
        print "Fail to download item."
        return False


def partialDownloadFile(setting, nodeid, src, path, offset, size):
    '''Partial download an object from cloud.'''
    #OneDrive connection:
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    '''write code here'''

def downloadMetadata(setting, nodeid, src, metadatapath):
    '''Download metadata from cloud.'''
    #OneDrive connection:
    print "Calling downloadmetadata in onedrive"
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Download metadata from bucket:
    abssrc = os.path.join(bucketname,src+'.metadata')
    try:
        conn.item(path=bucketname).children[src+'.metadata'].download(metadatapath)
        return True
    except:
        print "Fail to download metadata item"
        return False

def existsFile(setting, nodeid, name):
    '''Check existence of an object on cloud.'''
    '''Return True for exist, False for not exist.'''
    #OneDrive:
    print "Calling existfile in onedrive"
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    items = navigate(conn, bucketname)
    for item in items:
        if item.name == name:
            return True
    return False

def deleteFile(setting, nodeid, name):
    '''Delete an object from cloud.'''
    #OneDrive connection:
    print "Calling deletefile in onedrive"
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Delete an object from bucket:
    absdest = os.path.join(bucketname,name)
    delete(conn, absdest)
    #Delete the associated metadata
    if setting.coding != 'replication':
        delete(conn, absdest.rsplit('.',1)[0]+'.metadata')
    else:
        delete(conn, absdest + '.metadata')
    return True

#functions for deduplication support
def downloadPointers(setting, nodeid):
    print "Calling downloadPointers in onedrive"
    conn = client
    if setting.coding != 'replication':
        print "don't support download pointers for coding modes other than replication"
        return False

    pointerdir = setting.pointerdir
    bucketname = setting.nodeInfo[nodeid].bucketname
    items = navigate(conn, bucketname)
    for item in items:
        if item.name.endswith('.pt'):
            destFilePath = os.path.join(pointerdir, item.name)
            download(conn, bucketname, item.name, destFilePath)
    return True


def detectFile(setting, filename, nodeid):
    '''detect the existence of a given file named filename on a given node with ID nodeid'''
    print "Calling detectFile in onedrive"
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    items = navigate(conn, bucketname)
    for item in items:
        if item.name == filename:
            return True
    return False


def deletePointer(setting, nodeid, name):
    '''Delete an object from cloud.'''
    print "Calling deletePointer in onedrive"
    conn = client
    bucketname = setting.nodeInfo[nodeid].bucketname
    absdest = os.path.join(bucketname,name+'.pt')
    delete(conn, absdest)
    return True


def downloadPointer (setting, nodeid, filename):
    '''download the pointer related to filename in a given cloud specified by nodeid'''
    print "Calling downloadPointer in onedrive"
    conn = client
    if setting.coding != 'replication':
        print "Error in downloadPointer. Only support replication coding mode"
        return False
    else:
        pointerdir = setting.pointerdir
        bucketname = setting.nodeInfo[nodeid].bucketname
        destPath = os.path.join(pointerdir,filename + '.pt')
        download(conn, bucketname, filename + '.pt', destPath)
        return True


