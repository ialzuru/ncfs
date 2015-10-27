#!/usr/bin/python
#
# @name = 'storage.py'
#
# @description = "Controller of storage modules."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#


import Queue
import threading
import os
import gzip
import sys

import common
import storages
from storages import *

storagelist = storages.__all__

storageQueue = Queue.Queue()

class StorageThread(threading.Thread):
    '''Class of storage-related threads.'''
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            method, args = self.queue.get()
            getattr(self, method)(*args)
            self.queue.task_done()

    def syncMirror(self, setting, nodeid, path):
        '''Sync file entries from cloud to file system.'''
        nodetype = setting.nodeInfo[nodeid].nodetype
        if nodetype in storagelist:
            module = globals()[nodetype]
            func = getattr(module, 'syncMirror')
            return func(setting, nodeid, path)
        else:
            return False

    def uploadFile(self, setting, nodeid, path, dest):
        '''Upload a file to cloud.'''
        nodetype = setting.nodeInfo[nodeid].nodetype
        if nodetype in storagelist:
            module = globals()[nodetype]
            func = getattr(module, 'uploadFile')
            return func(setting, nodeid, path, dest)
        else:
            return False

    def uploadMetadata(self, setting, nodeid, metadatapath, dest):
        '''Upload metadata to cloud.'''
        nodetype = setting.nodeInfo[nodeid].nodetype
        if nodetype in storagelist:
            module = globals()[nodetype]
            func = getattr(module, 'uploadMetadata')
            return func(setting, nodeid, metadatapath, dest)
        else:
            return False

    def uploadFileAndMetadata(self, setting, nodeid, path, metadatapath, dest):
        '''Upload file and metadata to cloud.'''
        nodetype = setting.nodeInfo[nodeid].nodetype
        if nodetype in storagelist:
            module = globals()[nodetype]
            func = getattr(module, 'uploadFileAndMetadata')
            return func(setting, nodeid, path, metadatapath, dest)
        else:
            return False

    def downloadFile(self, setting, nodeid, path, dest):
        '''Download an object from cloud.'''
        nodetype = setting.nodeInfo[nodeid].nodetype
        print >> sys.stderr, "[downloadFile] Got job: %s." % ((nodeid, path, dest),)
        if nodetype in storagelist:
            module = globals()[nodetype]
            func = getattr(module, 'downloadFile')
            return func(setting, nodeid, path, dest)
        else:
            return False

    def partialDownloadFile(self, setting, nodeid, path, dest, offset, chunksize):
        '''Partial download an object from cloud.'''
        nodetype = setting.nodeInfo[nodeid].nodetype
        print >> sys.stderr, "[partialDownloadFile] Got job: %s." % \
            ((nodeid, path, dest, offset, chunksize),)
        if nodetype in storagelist:
            module = globals()[nodetype]
            func = getattr(module, 'partialDownloadFile')
            return func(setting, nodeid, path, dest, offset, chunksize)
        else:
            return False


def initStorages(setting):
    '''Spawn a pool of threads, and pass them queue instance.'''
    for i in range(setting.storagethreads):
        t = StorageThread(storageQueue)
        t.daemon = True
        t.start()


def resetStorageQueue():
    '''Re-initialize the storage queue.'''
    storageQueue.__init__()


def waitForStorages():
    '''Wait on the queue until everything has been processed.'''
    storageQueue.join()

def allocateJobs(listChunks, listSubChunks, numThreads, numChunks, minSize=0, method=0):
    '''Split chunk downloading into jobs for multi-threading'''
    '''
    WARNING: buggy when a whole chunk is to be downloaded, to be fixed
    Input:
    listChunks: a list with each entry the tuple (no. of chunks in a node, job, chunksize)
        [job represents parameters passed to 'downloadFile' / 'partialDownloadFile']
    numThreads: number of storage layer threads
    numChunks: number of chunks to download in total
    minSize: minimum size a sub-chunk can have
    method: allocation algorithm with n chunks, m threads
    0. Divide each chunk into m parts
    1. Divide each chunk into m/n parts
    2. One chunk per thread until m > n
       Divide each remaining chunks into m parts
    3. One chunk per thread until m > n
       Divide each remaining chunk into sub-chunks of size n/m of chunksize
       (The last sub-chunk is usually < n/m of chunksize)
    4.- (To be added)

    Output:
    listSubChunks: A list with each entry the tuple
        (chunkpath, (subchunkpath1, subchunksize1), ...)
        Each tuple matches one from listChunks in exact order

    Note:
    Size and number of sub chunks can vary for different chunks.
    '''

    del listSubChunks[:]
    print "[Multithread] Job allocation method %d." % (method,)

    def splitChunk(numSubChunks, chunkSize, chunkPath, subChunkSize=-1):
        if subChunkSize == -1:
            if minSize > 0 and numSubChunks > chunkSize / minSize:
                numSubChunks = chunkSize / minSize - 1
            if numSubChunks < 1: numSubChunks = 1
            subChunkSize = chunkSize / numSubChunks

        subChunkInfo = [chunkPath]
        if numSubChunks > 1:
            for i in range(0, numSubChunks-1):
                subChunk = chunkPath + '.' + str(i)
                subChunkInfo.append((subChunk, subChunkSize))
            subChunk = chunkPath + '.' + str(numSubChunks-1)
            subChunkInfo.append((subChunk, chunkSize - (numSubChunks-1)*subChunkSize))
        listSubChunks.append(list(subChunkInfo))

    if method == 0:
        for chunkInfo in listChunks:
            splitChunk(numThreads, chunkInfo[-1], chunkInfo[3])
    elif method == 1:
        for chunkInfo in listChunks:
            splitChunk(numThreads/numChunks, chunkInfo[-1], chunkInfo[3])
    elif method == 2:
        for j in range(0, numChunks-numThreads+1):
            chunkInfo = listChunks[j]
            splitChunk(1, chunkInfo[-1], chunkInfo[3])
        start = 0 if numChunks < numThreads else numChunks-numThreads
        for j in range(start, numChunks):
            chunkInfo = listChunks[j]
            splitChunk(numThreads, chunkInfo[-1], chunkInfo[3])
    elif method == 3:
        for j in range(0, numChunks-numThreads+1):
            chunkInfo = listChunks[j]
            splitChunk(1, chunkInfo[-1], chunkInfo[3])
        start = 0 if numChunks < numThreads else numChunks-numThreads
        for j in range(start, numChunks):
            chunkInfo = listChunks[j]
            splitChunk( (numThreads-1)/numChunks + 1, \
                chunkInfo[-1], chunkInfo[3], \
                numChunks * chunkInfo[-1]/numThreads)
    else:
        print "[Multithread] Job allocation method not implemented."
        return False

def checkHealth(setting):
    '''Check healthiness of nodes and update setting.'''
    setting.healthynode = setting.totalnode
    for i in range(setting.totalnode):
        nodetype = setting.nodeInfo[i].nodetype
        if nodetype in storagelist:
            module = globals()[nodetype]
            func = getattr(module, 'checkHealth')
            health = func(setting, i)
            setting.nodeInfo[i].healthy = health
            if health == False:
                setting.healthynode -= 1


def syncMirror(setting, path):
    '''Synchronize file entries on cloud to local directory.'''
    for i in range(setting.totalnode):
        if setting.nodeInfo[i].healthy == True:
            nodeid = i
            storageQueue.put(('syncMirror', (setting, nodeid, path)))
    waitForStorages()


def uploadFileAndMetadata(setting, metadata, mode):
    '''Upload file and the metadata to cloud.'''
    checkHealth(setting)
    if setting.healthynode == setting.totalnode:
        #Generate metadata file:
        configname = metadata.filename + ".metadata"
        configpath = setting.metadatadir + '/' + configname
        metadata.write(configpath)

        #Upload big-chunks and metadata:
        for i in range(metadata.totalnode):
            if metadata.fileNodeInfo[i].action == 'upload':
                #Upload big-chunk:
                storageQueue.put(('uploadFileAndMetadata', \
                    (setting, i, metadata.fileNodeInfo[i].bigchunkpath, \
                     configpath, metadata.fileNodeInfo[i].bigchunkname)))
            else:
                #Upload metadata only:
                storageQueue.put(('uploadMetadata', \
                    (setting, i, configpath, \
                     metadata.filename + ".node%d" % i)))
        waitForStorages()
        #print "*** upload file finished: " + metadata.filename
    return True


def deleteFile(setting, nodeid, name):
    '''Delete an object on cloud.'''
    nodetype = setting.nodeInfo[nodeid].nodetype
    if nodetype in storagelist:
        module = globals()[nodetype]
        func = getattr(module, 'deleteFile')
        return func(setting, nodeid, name)
    else:
        return False


def downloadMetadata(setting, metadata):
    '''Download metadata file and get metadata.'''
    checkHealth(setting)
    configpath = setting.metadatadir + '/' + metadata.filename + ".metadata"
    retstat = False
    #Download metadata file from cloud:
    for i in range(setting.totalnode):
        if setting.nodeInfo[i].healthy == True:
            nodetype = setting.nodeInfo[i].nodetype
            if nodetype in storagelist:
                module = globals()[nodetype]
                func = getattr(module, 'downloadMetadata')
                if func(setting, i, metadata.filename + ".node%d" % i, configpath) == True:
                    retstat = True
                    break

    if retstat == True:
        metadata.read(configpath, setting)
    return retstat


def downloadFile(setting, metadata):
    '''Download an object from cloud.'''
    checkHealth(setting)

    #Variables for new storage job algorithm
    numChunks, listChunks = 0, []  #No. of chunks to download and list of their info
    numSubChunks, listSubChunks = 0, [] #No. of sub chunks per chunk and their paths

    for i in range(metadata.totalchunk):
        if metadata.chunkInfo[i].action == 'download':
            if setting.smartthreads:
                #Pre-processing for new storage job algorithm
                #Mark chunks to download and their properties
                nodeid = metadata.chunkInfo[i].nodeid
                chunkpath = metadata.chunkInfo[i].chunkpath
                dest = setting.chunkdir + '/' + chunkpath
                chunksize = metadata.chunkInfo[i].chunksize
                metadata.chunkInfo[i].chunkpath = dest
                numChunks += 1
                chunknum = metadata.fileNodeInfo[nodeid].chunknum
                if chunknum > 1:
                    bigchunkpath = metadata.fileNodeInfo[nodeid].bigchunkpath
                    offset = chunksize * metadata.chunkInfo[i].position
                    listChunks.append((chunknum, nodeid, bigchunkpath, dest, offset, chunksize))
                else:
                    listChunks.append((chunknum, nodeid, chunkpath, dest, chunksize))
            else:
                nodeid = metadata.chunkInfo[i].nodeid
                if metadata.fileNodeInfo[nodeid].chunknum == 1:
                    #For one chunk per node:
                    chunkpath = metadata.chunkInfo[i].chunkpath
                    dest = setting.chunkdir + '/' + chunkpath
                    storageQueue.put(('downloadFile', \
                        (setting, nodeid, chunkpath, dest)))
                elif metadata.fileNodeInfo[nodeid].chunknum > 1:
                    #For multiple chunks per node:
                    bigchunkpath = metadata.fileNodeInfo[nodeid].bigchunkpath
                    dest = setting.chunkdir + '/' + metadata.chunkInfo[i].chunkpath
                    chunksize = metadata.chunkInfo[i].chunksize
                    offset = chunksize * metadata.chunkInfo[i].position
                    storageQueue.put(('partialDownloadFile', \
                        (setting, nodeid, bigchunkpath, dest, offset, chunksize)))
                else:
                    #For invalid chunk number:
                    return False
                #Update metadata:
                metadata.chunkInfo[i].chunkpath = dest
                metadata.chunkInfo[i].healthy = True

    if setting.smartthreads:
        #Job processing for new storage job algorithm
        #Determine sub-chunk splits and submit each split as a storage job
        allocateJobs(listChunks, listSubChunks, setting.storagethreads, \
            numChunks, setting.smartthreadslim, setting.smartthreadsmethod)
        print "[Multithread] listChunks: %s." % (listChunks,)
        print "[Multithread] listSubChunks: %s." % (listSubChunks,)
        for i in range(len(listChunks)):
            chunkInfo, subChunkInfo = listChunks[i], listSubChunks[i]
            if len(subChunkInfo) == 1:
                #No splitting
                storageQueue.put( \
                    ('downloadFile' if chunkInfo[0]==1 else 'partialDownloadFile', \
                    (setting,) + tuple(chunkInfo[1:-1])) )
            else:
                #Splitting needed
                jobInfo = [setting] + list(chunkInfo[1:3]) + [0,0,0]
                subChunkOffset = 0 if chunkInfo[0]==1 else chunkInfo[4]
                for (subChunk, subChunkSize) in subChunkInfo[1:]:
                    jobInfo[3:] = [subChunk, subChunkOffset, subChunkSize]
                    storageQueue.put( ('partialDownloadFile', tuple(jobInfo)) )
                    subChunkOffset += subChunkSize
    waitForStorages()

    if setting.smartthreads:
        #Post-processing for new storage job algorithm
        #Concatenate splits back into chunks
        for subChunkInfo in listSubChunks:
            if len(subChunkInfo) > 1:
                print "[Multithread] Constructing: %s" % (subChunkInfo[0],)
                f = open(subChunkInfo[0], 'wb')
                [f.write(open(subChunk[0], 'rb').read()) for subChunk in subChunkInfo[1:]]
                f.close()

    return True


def deleteChunkAndMetadata(setting, metadata):
    '''Delete chunks and metadata of a file on cloud.'''
    #Delete chunks:
    for fileNode in metadata.fileNodeInfo:
        try:
            deleteFile(setting, fileNode.nodeid, fileNode.bigchunkpath)
        except:
            return False
    return True
