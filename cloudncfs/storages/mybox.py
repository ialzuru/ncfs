from __future__ import print_function, unicode_literals
from boxsdk import Client
from boxsdk.exception import BoxAPIException
from boxsdk.object.collaboration import CollaborationRole
from auth import authenticate
import os
import sys
import common
import storageutil

oauth, _, _ = authenticate()
conn = Client(oauth)

def getBucketFolder(client, bucketname):
    root_folder = client.folder(folder_id='0').get()
    items = root_folder.get_items(limit=1000, offset=0)
    for item in items:
        if item.name == bucketname:
            return item
    return None

def getFileInFolder(folder, filename):
    items = folder.get_items(limit=1000, offset=0)
    for item in items:
        if item.name == filename:
            return item
    return Node

def syncMirror(setting, nodeid, path):
    '''Synchronize file entries on cloud to local directory.'''
    #box connection:
    print "calling syncMirror in box"
    bucketname = setting.nodeInfo[nodeid].bucketname
    bucketfolder = getBucketFolder(conn, bucketname)
    items = bucketfolder.get_items(limit=1000, offset=0)
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
    if conn == False:
        return False
    else:
        bucketname = setting.nodeInfo[nodeid].bucketname
        root_folder = client.folder(folder_id='0').get()
        items = root_folder.get_items(limit=1000, offset=0)
        for item in items:
            if item.name == bucketname.split('/')[-1]
                print "successfully found the bucket"
                return True
        print "Failed to connect to %s" % (bucketname)
        return False
        

def uploadFile(setting, nodeid, localpath, dest):
    '''Upload a file to cloud'''
    #box connection:
    print "Calling uploadfile in box"
    bucketname = setting.nodeInfo[nodeid].bucketname
    try:
        bucketfolder = getBucketFolder(conn, bucketname)
        bucketfolder.upload(localpath, filename = dest)
        return True
    except IOError:
        print >> sys.stderr, "Error: The destination location is not writable."
        return False

def uploadMetadata(setting, nodeid, metadatapath, dest):
    '''Upload metadata to cloud.'''
    #box connection:
    print "Calling uploadmetadata in box"
    bucketname = setting.nodeInfo[nodeid].bucketname
    try:
        bucketfolder = getBucketFolder(conn, bucketname)
        bucketfolder.upload(localpath, filename = dest+'.metadata')
        return True
    except IOError:
        print >> sys.stderr, "Error: The destination location is not writable."
        return False


def uploadFileAndMetadata(setting, nodeid, localpath, metadatapath, dest):
    '''Upload file and metadata to cloud'''
    #box connection:
    print "Calling uploadfileandMetadata in box"
    bucketname = setting.nodeInfo[nodeid].bucketname
    try:
        bucketfolder = getBucketFolder(conn, bucketname)
        bucketfolder.upload(localpath, filename = dest)
        bucketfolder.upload(metadatapath, filename = dest+'.metadata')
        return True
    except IOError:
        print >> sys.stderr, "Error: The destination location is not writable."
        return False


def downloadFile(setting, nodeid, src, path):
    '''Download an object from cloud. '''
    #box connection:
    print "Calling downloadfile in box"
    bucketname = setting.nodeInfo[nodeid].bucketname
    try:
        bucketfolder = getBucketFolder(conn, bucketname)
        filetodownload = getFileInFolder(bucketfolder, src)
        out_file = open(path, 'wb')
        out_file.write(filetodownload.content())
        return True
    except:
        print "Fail to download item."
        return False


def partialDownloadFile(setting, nodeid, src, path, offset, size):
    '''Partial download an object from cloud.'''
    #box connection:
    bucketname = setting.nodeInfo[nodeid].bucketname
    '''write code here'''


def downloadMetadata(setting, nodeid, src, metadatapath):
    '''Download metadata from cloud.'''
    #box connection:
    print "Calling downloadmetadata in box"
    bucketname = setting.nodeInfo[nodeid].bucketname
    try:
        bucketfolder = getBucketFolder(conn, bucketname)
        metafiletodownload = getFileInFolder(bucketfolder, src+'.metadata')
        out_file = open(metadatapath, 'wb')
        out_file.write(metafiletodownload.content())
        return True
    except:
        print "Fail to download metadata item"
        return False


def existsFile(setting, nodeid, name):
    '''Check existence of an object on cloud.'''
    '''Return True for exist, False for not exist.'''
    #box connection:
    print "Calling existfile in box"
    bucketname = setting.nodeInfo[nodeid].bucketname
    bucketfolder = getBucketFolder(conn, bucketname)
    files = bucketfolder.get_items(limit=1000, offset=0)
    for curfile in files:
        if curfile.name == name:
             return True
    return False


def deleteFile(setting, nodeid, name):
    '''Delete an object from cloud.'''
    #box connection:
    print "Calling deletefile in box"
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Delete an object from bucket:
    try:
        bucketfolder = getBucketFolder(conn, bucketname)
        filetodelete = getFileInFolder(bucketfolder, name)
        filetodelete.delete()
        #Delete the associated metadata
        metafiletodelete = getFileInFolder(bucketfolder, name+'.metadata')
        metafiletodelete.delete()
    except:
        print "Fail to delete item."
        return False


#functions for deduplication support
def downloadPointers(setting, nodeid):
    print "Calling downloadPointers in onedrive"
    conn = client
    if setting.coding != 'replication':
        print "don't support download pointers for coding modes other than replication"
        return False
    else:
        pointerdir = setting.pointerdir
        bucketname = setting.nodeInfo[nodeid].bucketname
        try:
            bucketfolder = getBucketFolder(conn, bucketname)
            items = bucketfolder.get_items(limit=1000, offset=0)
            for item in items:
                if item.name.endswith('.pt'):
                    destFilePath = os.path.join(pointerdir, item.name)
                    out_file = open(destFilePath, 'wb')
                    out_file.write(item.content())
            return True
        except:
            print "Fail to downloadPointer in box."
            return False



def detectFile(setting, filename, nodeid):
    '''detect the existence of a given file named filename on a given node with ID nodeid'''
    print "Calling detectFile in box"
    bucketname = setting.nodeInfo[nodeid].bucketname
    bucketfolder = getBucketFolder(conn, bucketname)
    files = bucketfolder.get_items(limit=1000, offset=0)
    for curfile in files:
        if curfile.name == filename:
            return True
    return False


def deletePointer(setting, nodeid, name):
    '''Delete an object from cloud.'''
    print "Calling deletePointer in box"
    bucketname = setting.nodeInfo[nodeid].bucketname
    try:
        bucketfolder = getBucketFolder(conn, bucketname)
        filetodelete = getFileInFolder(bucketfolder, name+'.pt')
        filetodelete.delete()
        return True
    except:
        print "Fail to delete pointer file"
        return False


def downloadPointer (setting, nodeid, filename):
    '''download the pointer related to filename in a given cloud specified by nodeid'''
    print "Calling downloadPointer in box"
    if setting.coding != 'replication':
        print "Error in downloadPointer. Only support replication coding mode"
        return False
    else:
        pointerdir = setting.pointerdir
        bucketname = setting.nodeInfo[nodeid].bucketname
        try:
            bucketfolder = getBucketFolder(conn, bucketname)
            filetodownload = getFileInFolder(bucketfolder, filename+'.pt')
            destPath = os.path.join(pointerdir, filename + '.pt')
            out_file = open(destPath, 'wb')
            out_file.write(filetodownload.content())
            return True
        except:
            print "Fail to downloadPointer in box."
            return False

