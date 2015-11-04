#!/usr/bin/python
#
# @name = 'amazonS3.py'
#
# @description = "amazon S3 storage module."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#


import base64

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import common
import storageutil


def connectCloud(setting, nodeid):
    '''Establish connection.'''
    #S3connection:
    try:
        accesskey = setting.nodeInfo[nodeid].accesskey
        secretkey = setting.nodeInfo[nodeid].secretkey
        return S3Connection(accesskey,secretkey)
    except:
        return False


def checkHealth(setting, nodeid):
    '''Check if successful connection and existing bucket.'''
    conn = connectCloud(setting, nodeid)
    if conn == False:
        return False
    else:
        bucketname = setting.nodeInfo[nodeid].bucketname
        try:
            bucket = conn.get_bucket(bucketname)
            return True
        except:
            return False


def syncMirror(setting, nodeid, path):
    '''Synchronize file entries on cloud to local directory.'''
    #S3connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Sync cloud to mirror dir:
    bucket = conn.get_bucket(bucketname)
    objects = bucket.get_all_keys()
    for obj in objects:
        #Touch files in local directory:
        filename = obj.name
        # if filename.endswith('.metadata'):
        if filename.endswith('.node%d' % nodeid):
            storageutil.syncFile(setting, filename, path)
    return True


def uploadFile(setting, nodeid, path, dest):
    '''Upload a file to cloud.'''
    #S3connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Upload file to bucket:
    bucket = conn.get_bucket(bucketname)
    k = Key(bucket)
    k.key = dest
    k.set_contents_from_filename(path)
    return True


def uploadMetadata(setting, nodeid, metadatapath, dest):
    '''Upload metadata to cloud.'''
    #S3connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Upload metadata to bucket:
    bucket = conn.get_bucket(bucketname)
    k = Key(bucket)
    k.key = dest
    fobj = open(metadatapath, 'rb')
    k.set_metadata('0', base64.urlsafe_b64encode(fobj.read()))
    fobj.close()
    return True


def uploadFileAndMetadata(setting, nodeid, path, metadatapath, dest):
    '''Upload file and metadata to cloud.'''
    #S3connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Upload file and metadata to bucket:
    bucket = conn.get_bucket(bucketname)
    k = Key(bucket)
    k.key = dest
    fobj = open(metadatapath, 'rb')
    k.set_metadata('0', base64.urlsafe_b64encode(fobj.read()))
    fobj.close()
    k.set_contents_from_filename(path)
    return True


def downloadFile(setting, nodeid, src, path):
    '''Download an object from cloud.'''
    #S3connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Download object from bucket:
    bucket = conn.get_bucket(bucketname)
    k = Key(bucket)
    k.key = src
    k.get_contents_to_filename(path)
    return True


def partialDownloadFile(setting, nodeid, src, path, offset, size):
    '''Partial download an object from cloud.'''
    #S3connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Partial download object from bucket:
    bucket = conn.get_bucket(bucketname)
    k = Key(bucket)
    k.key = src
    k.get_contents_to_filename(path, {'Range': 'bytes=%d-%d' % (offset, offset + size - 1)})
    return True


def downloadMetadata(setting, nodeid, src, metadatapath):
    '''Download metadata from cloud.'''
    #S3connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Download metadata from bucket:
    bucket = conn.get_bucket(bucketname)
    k = bucket.get_key(src)
    fobj = open(metadatapath, 'wb')
    fobj.write(base64.urlsafe_b64decode(k.get_metadata('0').encode('utf-8')))
    fobj.close()
    return True


def existsFile(setting, nodeid, name):
    '''Check existence of an object on cloud.'''
    '''Return True for exist, False for not exist.'''
    #S3connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Check existence of an object in bucket:
    bucket = conn.get_bucket(bucketname)
    k = Key(bucket)
    k.key = name
    return k.exists()


def deleteFile(setting, nodeid, name):
    '''Delete an object from cloud.'''
    #S3connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Delete an object from bucket:
    bucket = conn.get_bucket(bucketname)
    k = Key(bucket)
    k.key = name
    k.delete()
    return True

