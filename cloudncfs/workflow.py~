#!/usr/bin/python
#
# @name = 'workflow.py'
# 
# @description = "Define workflows."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import common
import storage
import coding
#import zk #cq


def downloadFile(setting, metadata):
    '''Workflow of Download file.'''
    if setting.deduplication == False:
        #Acquire read lock if using ZooKeeper:
        #if setting.zookeeper: setting.zk.getReadLock(metadata) #cq
        #Download metadata:
        storage.downloadMetadata(setting, metadata)
        #Update metadata for requesting chunks for decode:
        coding.updateMetadataForDecode(setting, metadata, 'download')
        #Download chunk files:
        storage.downloadFile(setting, metadata)
        #Release read lock:
        #if setting.zookeeper: setting.zk.releaseReadLock(metadata) #cq
        #Decode file:
        coding.decodeFile(setting, metadata, 'download')
    else:
        retState = storage.detectPointer(setting,metadata)
        if retState == False:
            '''The file detected is non_dup or org file'''
            storage.downloadMetadata(setting, metadata)
            coding.updateMetadataForDecode(setting, metadata, 'download')
            storage.downloadFile(setting, metadata)
            coding.decodeFile(setting, metadata, 'download')
        else:
            '''The file detected is a leaf'''
            storage.downloadPointer(setting,metadata)
            targetFile = storage.getPointerContent(setting, metadata)
            targetMetadata = common.FileMetadata(targetFile,0,setting.totalnode,setting.coding)
            storage.downloadMetadata(setting, targetMetadata)
            coding.updateMetadataForDecode(setting, targetMetadata, 'download')
            storage.downloadFile(setting, targetMetadata)
            coding.decodeFile(setting,targetMetadata, 'download')
            storage.genPtLocalFile(setting,targetMetadata,metadata)


def deleteFile(setting, metadata):
    '''Workflow of Delete file.'''
    if setting.deduplication == False:
        #Acquire write lock if using ZooKeeper:
        #if setting.zookeeper: setting.zk.getWriteLock(metadata) #cq
        #Download metadata:
        storage.downloadMetadata(setting, metadata)
        #Delete chunk files and metadata:
        storage.deleteChunkAndMetadata(setting, metadata)
        #Release write lock:
        #if setting.zookeeper: setting.zk.releaseWriteLock(metadata) #cq
    else:
        orgState = storage.detectOrgState(setting, metadata)
        if orgState == 3:
            # non_dup
            print "remove non_dup"      
            #Download metadata:
            storage.downloadMetadata(setting, metadata)
            #Delete chunk files and metadata:
            storage.deleteChunkAndMetadata(setting, metadata)
        elif orgState == 2:
            # leaf
            print "remove leaf"
            storage.deletePointer(setting, metadata)
        elif orgState == 1:
            # org
            print "remove org"
            storage.downloadMetadata(setting, metadata)
            storage.deleteChunkAndMetadata(setting, metadata)
            dupFileName = storage.findOnePointer(setting, metadata)
            dupMetadata = common.FileMetadata(dupFileName, 0, setting.totalnode, setting.coding)
            storage.deletePointer(setting, dupMetadata)
            coding.encodeFile(setting,dupMetadata)
            storage.uploadFileAndMetadata(setting, dupMetadata,'upload')
            storage.updatePointers(setting,metadata,dupMetadata)


def uploadFile(setting, metadata):
    '''Workflow of Upload file.'''
    if setting.deduplication == False:
        #Encode file:
        print "deduplication False, workflow upload file"        
        coding.encodeFile(setting, metadata)
        #Acquire write lock if using ZooKeeper:
        #if setting.zookeeper: setting.zk.getWriteLock(metadata) #cq
        #Upload chunk files and metadata:
        storage.uploadFileAndMetadata(setting, metadata, 'upload')
        #Release write lock:
        #if setting.zookeeper: setting.zk.releaseWriteLock(metadata) #cq
    else:
        print "deduplication True, workflow upload file"
        orgState = storage.detectOrgState(setting, metadata)
        print "orgState %d" % (orgState)
        currState = storage.detectCurrState(setting, metadata)
        print "currState %d" % (currState[0])
        if orgState == 3 and currState[0] == 0:
            # non_dup => non_dup    or       new => non_dup
            print 'orgState %d to currState %d' % (orgState, currState[0])
            coding.encodeFile(setting, metadata)
            storage.uploadFileAndMetadata(setting,metadata,'upload')
        elif orgState == 3 and currState[0] == 1:
            # non_dup => leaf       or    new => leaf
            print 'orgState %d to currState %d' % (orgState, currState[0])
            detState = storage.detectReplica(setting, metadata)
            if detState == True:
                deleteFile(setting, metadata)            
            storage.createPointer(setting, metadata, currState[1])
            storage.uploadPointer(setting, metadata)
        elif orgState == 2 and currState[0] == 1:
            # leaf => leaf
            print 'orgState %d to currState %d' % (orgState, currState[0])
            storage.deletePointer(setting,metadata)
            storage.createPointer(setting,metadata,currState[1])
            storage.uploadPointer(setting,metadata)
        elif orgState == 2 and currState[0] == 0:
            # leaf => non_dup
            print 'orgState %d to currState %d' % (orgState, currState[0])
            storage.deletePointer(setting,metadata)
            coding.encodeFile(setting,metadata)
            storage.uploadFileAndMetadata(setting,metadata,'upload')
        elif orgState == 1 and currState[0] == 0:
            # org => non_dup
            print 'orgState %d to currState %d' % (orgState, currState[0])
            coding.encodeFile(setting,metadata)
            storage.uploadFileAndMetadata(setting,metadata,'upload')
            dupFileName = storage.findOnePointer(setting,metadata)
            dupMetadata = common.FileMetadata(dupFileName,0,setting.totalnode,setting.coding)
            storage.deletePointer(setting,dupMetadata)
            coding.encodeFile(setting,dupMetadata)
            storage.uploadFileAndMetadata(setting,dupMetadata,'upload')
            storage.updatePointers(setting,metadata,dupMetadata)
        elif orgState == 1 and currState[0] == 1:
            # org => leaf
            print 'orgState %d to currState %d' % (orgState, currState[0])
            deleteFile(setting,metadata)
            storage.createPointer(setting,metadata,currState[1])
            storage.uploadPointer(setting,metadata)
            '''
            dupFileName = storage.findOnePointer(setting,metadata)        
            dupMetadata = common.FileMetadata(dupFileName,0,setting.totalnode,setting.coding)
            storage.deletePointer(setting,dupMetadata)
            coding.encodeFile(setting, dupMetadata)       
            storage.uploadFileAndMetadata(setting,dupMetadata,'upload')
            storage.updatePointers(setting,metadata,dupMetadata)
            '''

def rebuildFile(settingOld, settingNew, metadata, filename):
    '''Workflow of File rebuid.'''
    if settingNew.deduplication == False:
        #Acquire write lock if using ZooKeeper:
        #if settingOld.zookeeper: settingOld.zk.getWriteLock(metadata) #cq
        #Download metadata:
        print "downloadMetadata"
        storage.downloadMetadata(settingOld, metadata)
        #Update metadata for repair:
        print "updateMetadataForDecode"
        coding.updateMetadataForDecode(settingOld, metadata, 'repair')
        #Download file chunks:
        print "downloadFile"
        storage.downloadFile(settingOld, metadata)
        #Reapir file:
        print "repair File"
        coding.repairFile(settingOld, settingNew, metadata)
        #Upload repaired chunks:
        print "upload file and metadata"
        storage.uploadFileAndMetadata(settingNew, metadata, 'repair')
        #Release write lock:
        #if settingOld.zookeeper: settingOld.zk.releaseWriteLock(metadata) #cq
        coding.decodeFile(settingNew, metadata, 'download')
    else:
        retState = storage.detectPointer(settingNew, metadata)
        if retState == False:
            #org or non_dup
           print "rebuild org or non_dup %s" % (metadata.filename)
           storage.downloadMetadata(settingOld, metadata)
           coding.updateMetadataForDecode(settingOld, metadata, 'repair')
           storage.downloadFile(settingOld, metadata)
           coding.repairFile(settingOld, settingNew, metadata)
           storage.uploadFileAndMetadata(settingNew, metadata, 'repair')
           coding.decodeFile(settingNew, metadata, 'download')
        else:
            print "rebuild pointer %s" % (metadata.filename)
            storage.repairPointer(settingOld, settingNew, metadata)

