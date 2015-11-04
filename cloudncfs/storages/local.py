#!/usr/bin/python
#
# @name = 'local.py'
#
# @description = "local filesystem storage module."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import os
import sys
import shutil
import common
import storageutil


def checkHealth(setting, nodeid):
    '''Check if successful connection and existing container.'''
    return os.path.isdir(setting.nodeInfo[nodeid].bucketname)


def syncMirror(setting, nodeid, path):
    '''Synchronize file entries on cloud to local directory.'''
    basedir = setting.nodeInfo[nodeid].bucketname
    #Sync cloud to mirror dir:
    for root, dirs, files in os.walk(basedir):
        #Touch files in local directory:
        for obj in files:
            filename = os.path.relpath(os.path.join(root, obj), basedir)
            # if filename.endswith('.metadata'):
            if setting.coding == 'replication': #cq
                if filename.endswith('.node0'):
                    storageutil.syncFile(setting, filename, path)
            else:
                if filename.endswith('.node%d' % nodeid):
                    storageutil.syncFile(setting, filename, path)
    return True


def uploadFile(setting, nodeid, path, dest):
    '''Upload a file to cloud.'''
    basedir = setting.nodeInfo[nodeid].bucketname
    #Upload file to local filesystem:
    try:
        absdest = os.path.join(basedir, dest)
        if not os.path.exists(os.path.dirname(absdest)):
            #Modified by Michael.
            try:
                os.makedirs(os.path.dirname(absdest))
            except:
                pass
        shutil.copyfile(path, absdest)
    except IOError:
        print >> sys.stderr, 'Error: The destination location is not writable.'
        return False
    return True


def uploadMetadata(setting, nodeid, metadatapath, dest):
    '''Upload metadata to cloud.'''
    basedir = setting.nodeInfo[nodeid].bucketname
    #Upload metadata to local filesystem:
    try:
        absdest = os.path.join(basedir, dest + '.metadata')
        if not os.path.exists(os.path.dirname(absdest)):
            try:
                os.makedirs(os.path.dirname(absdest))
            except:
                pass
        shutil.copyfile(metadatapath, absdest)
    except IOError:
        print >> sys.stderr, 'Error: The destination location is not writable.'
        return False
    return True


def uploadFileAndMetadata(setting, nodeid, path, metadatapath, dest):
    '''Upload file and metadata to cloud.'''
    # Local filesystem does not have overhead of connection establishment,
    # so we just call the two functions separately.
    uploadFile(setting, nodeid, path, dest)
    uploadMetadata(setting, nodeid, metadatapath, dest)


def downloadFile(setting, nodeid, src, path):
    '''Download an object from cloud.'''
    basedir = setting.nodeInfo[nodeid].bucketname
    #Download object from local filesystem:
    try:
        shutil.copyfile(os.path.join(basedir, src), path)
    except IOError:
        print >> sys.stderr, 'Error: Source file not found.'
        return False
    return True


def partialDownloadFile(setting, nodeid, src, path, offset, size):
    '''Partial download an object from cloud.'''
    basedir = setting.nodeInfo[nodeid].bucketname
    #Partial download object from local filesystem:
    try:
        #Partial copy from srcfd to dstfd:
        srcfd = os.open(os.path.join(basedir, src), os.O_RDONLY)
        os.lseek(srcfd, offset, os.SEEK_SET)
        dstfd = os.open(path, os.O_WRONLY|os.O_CREAT)
        os.write(dstfd, os.read(srcfd, size))
        os.close(dstfd)
        os.close(srcfd)
    except IOError:
        print >> sys.stderr, 'Error: Source file not found.'
        return False
    return True


def downloadMetadata(setting, nodeid, src, metadatapath):
    '''Download metadata from cloud.'''
    basedir = setting.nodeInfo[nodeid].bucketname
    #Download metadata from local filesystem:
    try:
        shutil.copyfile(os.path.join(basedir, src + '.metadata'), metadatapath)
    except IOError:
        print >> sys.stderr, 'Error: Metadata for the source file not found.'
        return False
    return True


def existsFile(setting, nodeid, name):
    '''Check existence of an object on cloud.'''
    '''Return True for exist, False for not exist.'''
    basedir = setting.nodeInfo[nodeid].bucketname
    #Check existence of a file in local filesystem:
    return os.path.exists(os.path.join(basedir, name))


def deleteFile(setting, nodeid, name):
    '''Delete an object from cloud.'''
    basedir = setting.nodeInfo[nodeid].bucketname
    #Delete a file from local filesystem:
    os.unlink(os.path.join(basedir, name))
    #Delete the associated metadata
    #os.unlink(os.path.join(basedir, rsplit(name, '.', 1)[0]+'.metadata')) #cq
    # comment this line because the metadata file of replication is basedir+filename+'.node0'+'.metatdata'. Need to verify the filename of other coding scheme
    if setting.coding != 'replication':
        os.unlink(os.path.join(basedir, rsplit(name, '.', 1)[0]+'.metadata'))
    else:
        os.unlink(os.path.join(basedir, name+'.metadata'))
    return True
