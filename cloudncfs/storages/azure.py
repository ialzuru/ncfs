#!/usr/bin/python
#
# @name = 'azure.py'
#
# @description = "Windows Azure storage module."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import base64
import sys
import common
import storageutil

try:
    import libazure
    from libazure import azure_storage
except ImportError, e:
    print >> sys.stderr, 'Warning: Cannot import libazure package.'
    raise e

BLOB_HOST = 'blob.core.windows.net'
TABLE_HOST = 'table.core.windows.net'
QUEUE_HOST = 'queue.core.windows.net'


def connectCloud(setting, nodeid):
    '''Establish connection.'''
    try:
        account_name = setting.nodeInfo[nodeid].accesskey
        access_key = setting.nodeInfo[nodeid].secretkey
        return azure_storage.AzureStorage(BLOB_HOST, TABLE_HOST, QUEUE_HOST, account_name, access_key, False)
    except:
        return False


def checkHealth(setting, nodeid):
    '''Check if successful connection and existing container.'''
    conn = connectCloud(setting, nodeid)
    if conn == False:
        return False
    else:
        container_name = setting.nodeInfo[nodeid].bucketname
        try:
            objects = conn.list_blobs(container_name)
            return True
        except:
            return False


def syncMirror(setting, nodeid, path):
    '''Synchronize file entries on cloud to local directory.'''
    #Azure connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Sync cloud to mirror dir:
    objects = conn.list_blobs(container_name)
    for obj in objects:
        #Touch files in local directory:
        filename = obj
        # if filename.endswith('.metadata'):
        if filename.endswith('.node%d' % nodeid):
            storageutil.syncFile(setting, filename, path)
    return True


def uploadFile(setting, nodeid, path, dest):
    '''Upload a file to cloud.'''
    #Azure connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Upload file to container:
    fobj = open(path, 'rb')
    conn.put_blob(container_name, dest, fobj.read(), 'binary/octet-stream')
    fobj.close()
    return True


def uploadMetadata(setting, nodeid, metadatapath, dest):
    '''Upload metadata to cloud.'''
    #Azure connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Upload metadata to container:
    fobj = open(metadatapath, 'rb')
    conn.set_metadata(container_name, dest, {'0': base64.urlsafe_b64encode(fobj.read())})
    fobj.close()
    return True


def uploadFileAndMetadata(setting, nodeid, path, metadatapath, dest):
    '''Upload file and metadata to cloud.'''
    #Azure connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Upload file and metadata to container:
    fobj = open(path, 'rb')
    fobj_meta = open(metadatapath, 'rb')
    conn.put_blob(container_name, dest, fobj.read(), 'binary/octet-stream',
                  {'0': base64.urlsafe_b64encode(fobj_meta.read())})
    fobj_meta.close()
    fobj.close()
    return True


def downloadFile(setting, nodeid, src, path):
    '''Download an object from cloud.'''
    #Azure connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Download object from container:
    fobj = open(path, 'wb')
    fobj.write(conn.get_blob(container_name, src))
    fobj.close()
    return True


def partialDownloadFile(setting, nodeid, src, path, offset, size):
    '''Partial download an object from cloud.'''
    #Azure connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Partial download object from container:
    fobj = open(path, 'wb')
    fobj.write(conn.get_blob(container_name, src, offset, size))
    fobj.close()
    return True


def downloadMetadata(setting, nodeid, src, metadatapath):
    '''Download metadata from cloud.'''
    #Azure connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Download metadata from container:
    fobj = open(metadatapath, 'wb')
    fobj.write(base64.urlsafe_b64decode(conn.get_metadata(container_name, src)['0']))
    fobj.close()
    return True


def existsFile(setting, nodeid, name):
    '''Check existence of an object on cloud.'''
    '''Return True for exist, False for not exist.'''
    #Azure connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Check existence of an object in container:
    objects = conn.list_blobs(container_name)
    return name in objects


def deleteFile(setting, nodeid, name):
    '''Delete an object from cloud.'''
    #Azure connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Delete an object from container:
    conn.delete_blob(container_name, name)
    return True
