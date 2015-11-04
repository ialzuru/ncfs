#!/usr/bin/python
#
# @name = 'rebuild.py'
# 
# @description = "Utility module for node rebuild."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import os
import sys
import shutil
import copy
import Queue
import threading

import clean
import common
import workflow
import storage

settingpath = common.settingpath
rebuildQueue = Queue.Queue()


class RebuildThread(threading.Thread):
    '''Class of threads for rebuild.'''
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            method, args = self.queue.get()
            getattr(self, method)(*args)
            self.queue.task_done()

    def rebuildFile(self, settingOld, settingNew, filename):
        '''Rebuild a file.'''
        metadata = common.FileMetadata(filename,0,0,0)
        #Rebuild file:
        workflow.rebuildFile(settingOld, settingNew, metadata, filename)


def initRebuild(threadnum):
    '''Spawn a pool of threads, and pass them queue instance.'''
    for i in range(threadnum):
        t = RebuildThread(rebuildQueue)
        t.setDaemon(True)
        t.start()


def waitForRebuild():
    '''Wait on the queue until everything has been processed.'''
    rebuildQueue.join()


def getNewSetting(settingOld, settingPath):
    '''Construct new setting.'''
    settingNew = copy.deepcopy(settingOld)
    count = 0
    for node in settingNew.nodeInfo:
        if node.healthy == False and settingOld.totalsparenode > 0:
            #Read spare node info from setting file.
            #Currently only support 1-node failure.
            node.nodetype = settingOld.spareNodeInfo[count].nodetype
            node.bucketname = settingOld.spareNodeInfo[count].bucketname
            node.accesskey = settingOld.spareNodeInfo[count].accesskey
            node.secretkey = settingOld.spareNodeInfo[count].secretkey
            node.nodekey = settingOld.spareNodeInfo[count].nodekey
            node.nodeloc = settingOld.spareNodeInfo[count].nodeloc
            node.healthy = True
            settingNew.healthynode += 1
            settingNew.spareNodeInfo.pop(0)
            settingNew.totalsparenode -= 1
            count += 1
    return settingNew


def rebuildFile(settingOld, settingNew, filename):
    '''Rebuild a file.'''
    metadata = common.FileMetadata(filename,0,0,0)
    #Rebuild file:
    workflow.rebuildFile(settingOld, settingNew, metadata, filename)


def rebuildNode(filter=[]):
    '''Overall rebuild process.'''
    settingOld = common.Setting()
    settingOld.read(settingpath)
    rebuildthreads = settingOld.rebuildthreads
    if rebuildthreads > 0:
        initRebuild(rebuildthreads)

    storage.initStorages(settingOld)
    storage.checkHealth(settingOld)
    #for i in range(settingOld.totalnode):
    #    print "Node " + str(i) + " healthy = " + str(settingOld.nodeInfo[i].healthy) + "\n"
    #print "HealthyNodes = " + str(settingOld.healthynode) + "\n"
    storage.syncMirror(settingOld, settingOld.mirrordir)
    #Get file list recursively from directory:
    fileList = []
    for root, dirs, files in os.walk(settingOld.mirrordir):
        for fl in files:
            filepath = '/'.join([root, fl])
            filepath = filepath[len(settingOld.mirrordir)+1:]
            if filepath[0] == '/':
                filepath = filepath[1:]
            fileList.append(filepath)
    #Get new setting:
    settingNew = getNewSetting(settingOld, settingpath)

    #Rebuild files recursively:
    for targetFile in fileList:
        if (filter == []) or (targetFile in filter):
            if rebuildthreads > 0:        
                rebuildQueue.put(('rebuildFile',(settingOld, settingNew, targetFile)))
            else:
                rebuildFile(settingOld, settingNew, targetFile)
    if rebuildthreads > 0:
        waitForRebuild()

    #Backup old setting file, write new setting file:
    #print "Rebuild finishes. Writing new setting file."
    shutil.copyfile(settingpath, settingpath+".old")
    settingNew.write(settingpath)


if __name__ == '__main__':
    '''Rebuild all files if no argument. '''
    '''Rebuild filter files if filter argument exists.'''
    '''The filter argument is a list of strings.'''
    filter = []
    if len(sys.argv) > 1:
        filter = sys.argv[1:]
    #Clean temporary directories:
    clean.cleanAll()
    rebuildNode(filter)
