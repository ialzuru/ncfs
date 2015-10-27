#!/usr/bin/python
#
# @name = 'replication.py'
# 
# @description = "Replication coding."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import shutil

import common


def encodeFile(setting, metadata):
    '''Encode file into chunks.'''
    src = setting.mirrordir + '/' + metadata.filename
    dest = setting.chunkdir + '/' + metadata.filename + ".node0"
    shutil.copyfile(src, dest)
    for i in range(metadata.totalnode):
        chunk = common.ChunkMetadata(i)
        chunk.chunksize = metadata.filesize
        if i == 0:
            chunk.chunktype = 'native'
        else:
            chunk.chunktype = 'replica'
        chunk.chunkpath = dest
        chunk.chunkname = metadata.filename  + ".node0"
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
        fileNode.bigchunksize = metadata.filesize
        fileNode.bigchunkpath = dest
        fileNode.bigchunkname = metadata.filename +".node0"
        fileNode.chunknum = 1
        fileNode.action = 'upload'
        metadata.fileNodeInfo.append(fileNode)


def updateMetadataForDecode(setting, metadata, mode):
    '''Update metadata for retrieving chunks for decode.'''
    if setting.totalnode == setting.healthynode:
        for i in range(metadata.totalchunk):
            if metadata.chunkInfo[i].chunktype == 'native':
                metadata.chunkInfo[i].action = 'download'
                return True
    elif setting.healthynode > 0:
        for i in range(metadata.totalchunk):
            nodeid = metadata.chunkInfo[i].nodeid
            if setting.nodeInfo[nodeid].healthy == True:
                metadata.chunkInfo[i].action = 'download'
                return True
    else:
        return False


def decodeFile(setting, metadata, mode):
    '''Decode chunks into original file.'''
    '''For replication, a chunk is already the file.'''
    if setting.healthynode > 0:
        src = setting.chunkdir + '/' + metadata.filename + ".node0"
        dest = setting.mirrordir + '/' + metadata.filename
        try:
            shutil.copyfile(src, dest)
            return True
        except:
            return False
    else:
        return False


def repairFile(settingOld, settingNew, metadata):
    '''Repair file by rebuilding failed chunks.'''
    src = settingNew.chunkdir + '/' + metadata.filename + ".node0"
    for chunk in metadata.chunkInfo:
        if chunk.action == 'sos':
            nodeid = chunk.nodeid
            chunk.chunkname = metadata.filename + ".node0"
            chunk.chunkpath = src
            chunk.nodekey = settingNew.nodeInfo[nodeid].nodekey
            chunk.nodetype = settingNew.nodeInfo[nodeid].nodetype
            chunk.bucketname = settingNew.nodeInfo[nodeid].bucketname
            chunk.action = 'upload'
            #Add support for big-chunk:
            metadata.fileNodeInfo[nodeid].nodekey = chunk.nodekey
            metadata.fileNodeInfo[nodeid].nodetype = chunk.nodetype
            metadata.fileNodeInfo[nodeid].bucketname = chunk.bucketname
            metadata.fileNodeInfo[nodeid].bigchunksize = metadata.filesize
            metadata.fileNodeInfo[nodeid].bigchunkpath = src
            metadata.fileNodeInfo[nodeid].bigchunkname = metadata.filename + ".node0"
            metadata.fileNodeInfo[nodeid].chunknum = 1
            metadata.fileNodeInfo[nodeid].action = 'upload'
        else:
            chunk.chunkname = metadata.filename + ".node0"
            chunk.chunkpath = metadata.filename + ".node0"
