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


def uploadFile(setting, metadata):
    '''Workflow of Upload file.'''
    #Encode file:
    coding.encodeFile(setting, metadata)
    #Acquire write lock if using ZooKeeper:
    #if setting.zookeeper: setting.zk.getWriteLock(metadata) #cq
    #Upload chunk files and metadata:
    storage.uploadFileAndMetadata(setting, metadata, 'upload')
    #Release write lock:
    #if setting.zookeeper: setting.zk.releaseWriteLock(metadata) #cq


def deleteFile(setting, metadata):
    '''Workflow of Delete file.'''
    #Acquire write lock if using ZooKeeper:
    #if setting.zookeeper: setting.zk.getWriteLock(metadata) #cq
    #Download metadata:
    storage.downloadMetadata(setting, metadata)
    #Delete chunk files and metadata:
    storage.deleteChunkAndMetadata(setting, metadata)
    #Release write lock:
    #if setting.zookeeper: setting.zk.releaseWriteLock(metadata) #cq


def rebuildFile(settingOld, settingNew, metadata, filename):
    '''Workflow of File rebuid.'''
    #Acquire write lock if using ZooKeeper:
    #if settingOld.zookeeper: settingOld.zk.getWriteLock(metadata) #cq
    #Download metadata:
    storage.downloadMetadata(settingOld, metadata)
    #Update metadata for repair:
    coding.updateMetadataForDecode(settingOld, metadata, 'repair')
    #Download file chunks:
    storage.downloadFile(settingOld, metadata)
    #Reapir file:
    coding.repairFile(settingOld, settingNew, metadata)
    #Upload repaired chunks:
    storage.uploadFileAndMetadata(settingNew, metadata, 'repair')
    #Release write lock:
    #if settingOld.zookeeper: settingOld.zk.releaseWriteLock(metadata) #cq
