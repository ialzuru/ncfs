#!/usr/bin/python
#
# @name = 'rackspace.py'
#
# @description = "Rackspace storage module."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import base64
import sys
import common
import storageutil

try:
    import cloudfiles
except ImportError, e:
    print >> sys.stderr, 'Warning: Cannot import cloudfiles package.'
    raise e


def connectCloud(setting, nodeid):
    '''Establish connection.'''
    try:
        user = setting.nodeInfo[nodeid].accesskey
        key = setting.nodeInfo[nodeid].secretkey
        return cloudfiles.get_connection(username=user, api_key=key, timeout=666)
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
            container = conn.get_container(container_name)
            return True
        except:
            return False


def syncMirror(setting, nodeid, path):
    '''Synchronize file entries on cloud to local directory.'''
    #Rackspace connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Sync cloud to mirror dir:
    container = conn.get_container(container_name)
    objects = container.get_objects()
    for obj in objects:
        #Touch files in local directory:
        filename = obj.name
        # if filename.endswith('.metadata'):
        if filename.endswith('.node%d' % nodeid):
            storageutil.syncFile(setting, filename, path)
    return True


def uploadFile(setting, nodeid, path, dest):
    '''Upload a file to cloud.'''
    #Rackspace connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Upload file to container:
    container = conn.get_container(container_name)
    obj = container.create_object(dest)
    obj.load_from_filename(path)
    return True


def uploadMetadata(setting, nodeid, metadatapath, dest):
    '''Upload metadata to cloud.'''
    #Rackspace connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Upload metadata to container:
    container = conn.get_container(container_name)
    obj = container[dest]
    fobj = open(metadatapath, 'rb')
    meta = base64.urlsafe_b64encode(fobj.read())
    fobj.close()
    # In Rackspace, each metadata value is no more than 256 bytes.
    # We split the metadata into multiple 256-byte parts.
    obj.metadata = {}
    for i in xrange((len(meta) - 1) / 256 + 1):
        obj.metadata[str(i)] = meta[i * 256: (i + 1) * 256]
    obj.sync_metadata()
    return True


def uploadFileAndMetadata(setting, nodeid, path, metadatapath, dest):
    '''Upload file and metadata to cloud.'''
    #Rackspace connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Upload file and metadata to container:
    container = conn.get_container(container_name)
    obj = container.create_object(dest)
    obj.load_from_filename(path)
    fobj = open(metadatapath, 'rb')
    meta = base64.urlsafe_b64encode(fobj.read())
    fobj.close()
    # In Rackspace, each metadata value is no more than 256 bytes.
    # We split the metadata into multiple 256-byte parts.
    obj.metadata = {}
    for i in xrange((len(meta) - 1) / 256 + 1):
        obj.metadata[str(i)] = meta[i * 256: (i + 1) * 256]
    obj.sync_metadata()
    return True


def downloadFile(setting, nodeid, src, path):
    '''Download an object from cloud.'''
    #Rackspace connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Download object from container:
    container = conn.get_container(container_name)
    obj = container.get_object(src)
    obj.save_to_filename(path)
    return True


def partialDownloadFile(setting, nodeid, src, path, offset, size):
    '''Partial download an object from cloud.'''
    #Rackspace connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Partial download object from container:
    container = conn.get_container(container_name)
    obj = container.get_object(src)
    fobj = open(path, 'wb')
    try:
        obj.read(size, offset, buffer=fobj)
    finally:
        fobj.close()
    return True


def downloadMetadata(setting, nodeid, src, metadatapath):
    '''Download metadata from cloud.'''
    #Rackspace connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Download metadata from container:
    container = conn.get_container(container_name)
    obj = container[src]
    # In Rackspace, each metadata value is no more than 256 bytes.
    # We combine the splitted 256-byte parts.
    meta = ''
    for metakey in sorted(obj.metadata.keys(), key=int):
        meta += obj.metadata[metakey]
    fobj = open(metadatapath, 'wb')
    fobj.write(base64.urlsafe_b64decode(meta))
    fobj.close()
    return True


def existsFile(setting, nodeid, name):
    '''Check existence of an object on cloud.'''
    '''Return True for exist, False for not exist.'''
    #Rackspace connection:
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Check existence of an object in container:
    container = conn.get_container(container_name)
    try:
        obj = container.get_object(name)
    except cloudfiles.errors.NoSuchObject:
        return False
    return True


def deleteFile(setting, nodeid, name):
    '''Delete an object from cloud.'''
    #Rackspace connection
    conn = connectCloud(setting, nodeid)
    container_name = setting.nodeInfo[nodeid].bucketname
    #Delete an object from container
    container = conn.get_container(container_name)
    container.delete_object(name)
    return True
