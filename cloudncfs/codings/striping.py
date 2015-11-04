#!/usr/bin/python
#
# @name = 'striping.py'
# 
# @description = "Striping coding."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#


import os

import common
import codingutil

def encodeFile(setting, metadata):
    '''Encode file.'''
    src = setting.mirrordir + '/' + metadata.filename
    dest = []
    for i in range(metadata.totalnode):
        chunkname = setting.chunkdir + '/' + metadata.filename + '.node' + str(i)
        dest.append(chunkname)
    #Split file into chunks:
    codingutil.split(src, dest)
    for i in range(metadata.totalnode):
        chunk = common.ChunkMetadata(i)
        chunk.chunkname = metadata.filename + '.node' + str(i)
        chunk.chunksize = os.path.getsize(dest[i])
        chunk.chunktype = 'native'
        chunk.chunkpath = dest[i]
        chunk.nodeid = i
        chunk.nodekey = setting.nodeInfo[i].nodekey
        chunk.nodetype = setting.nodeInfo[i].nodetype
        chunk.bucketname = setting.nodeInfo[i].bucketname
        chunk.action = 'upload'
        chunk.position = 0    #Position inside big-chunk
        metadata.chunkInfo.append(chunk)
    metadata.totalchunk = metadata.totalnode

    #Add support for big-chunk:
    for i in range(metadata.totalnode):
        fileNode = common.FileNodeMetadata(i)
        fileNode.nodekey = setting.nodeInfo[i].nodekey
        fileNode.nodetype = setting.nodeInfo[i].nodetype
        fileNode.bucketname = setting.nodeInfo[i].bucketname
        fileNode.bigchunksize = metadata.chunkInfo[i].chunksize
        fileNode.bigchunkpath = metadata.chunkInfo[i].chunkpath
        fileNode.bigchunkname = metadata.chunkInfo[i].chunkname
        fileNode.chunknum = 1
        fileNode.action = 'upload'
        metadata.fileNodeInfo.append(fileNode)


def updateMetadataForDecode(setting, metadata, mode):
    '''Update metadata for requesting chunks for decode.'''
    if setting.totalnode == setting.healthynode:
        for i in range(metadata.totalchunk):
            if metadata.chunkInfo[i].chunktype == 'native':
                metadata.chunkInfo[i].action = 'download'
        return True
    else:
        return False


def decodeFile(setting, metadata, mode):
    '''Decode chunks into original file.'''
    if setting.healthynode == setting.totalnode:
        dest = setting.mirrordir + '/' + metadata.filename
        src = []
        for i in range(metadata.totalchunk):
            chunkname = setting.chunkdir + '/' + metadata.filename + '.node' + str(i) 
            src.append(chunkname)
        codingutil.join(src, dest, metadata.filesize)
    else:
        return False


def repairFile(settingOld, settingNew, metadata):
    '''Repair file by rebuilding failed chunks. No file repair for striping.'''
    print "No file repair for striping.\n"
    return False
