#!/usr/bin/python
#
# @name = 'parityrs.py'
# 
# @description = "Striping with any-parity coding by Reed Solomon Code."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import os

import common
import codingutil

def encodeFile(setting, metadata):
    '''Encode a file into chunks.'''
    metadata.datanode = setting.datanode
    src = setting.mirrordir + '/' + metadata.filename
    dest = []
    dataChunks = []
    for i in range(metadata.datanode):
        chunkname = setting.chunkdir + '/' + metadata.filename + '.node' + str(i)
        dataChunks.append(chunkname)
        dest.append(chunkname)
    parityChunk = []
    for i in range(metadata.datanode, metadata.totalnode):
        parityname = setting.chunkdir + '/' + metadata.filename + '.node' + str(i)
        parityChunk.append(parityname)
        dest.append(parityname)
    codingutil.ecEncode(src, dataChunks, parityChunk, setting)

    for i in range(metadata.totalnode):
        chunk = common.ChunkMetadata(i)
        chunk.chunkname = metadata.filename + '.node' + str(i)
        if i >= metadata.datanode:
            chunk.chunktype = 'parity'
        else:
            chunk.chunktype = 'native'
        chunk.chunkpath = dest[i]
        chunk.chunksize = setting.chunksize
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
    '''Update metadata for retrieving chunks for decode.'''
    if setting.healthynode == setting.totalnode:
        #Case of no failure:
        for i in range(metadata.totalchunk):
            if metadata.chunkInfo[i].chunktype == 'native':
                metadata.chunkInfo[i].action = 'download'
        return True
    elif setting.healthynode >= setting.datanode:
        #Case of one-node or two-node failure:
        count = 0
        for i in range(metadata.totalchunk):
            nodeid = metadata.chunkInfo[i].nodeid
            if setting.nodeInfo[nodeid].healthy == True:
                metadata.chunkInfo[i].action = 'download'
                count += 1
                #Only need to get sufficient chunks for decode:
                if count == metadata.datanode:
                    break
        return True
    else:
        return False


def decodeFile(setting, metadata, mode):
    '''Decode chunks into original file.'''
    if setting.healthynode == setting.totalnode:
        #Case of no failure:
        dest = setting.mirrordir + '/' + metadata.filename
        src = []
        for i in range(metadata.datanode):
            chunkname = setting.chunkdir + '/' + metadata.filename + '.node' + str(i)
            src.append(chunkname)
        codingutil.join(src, dest, metadata.filesize)

    elif setting.healthynode >= metadata.datanode:
        #Case of one-node or two-node failure:
        dest = setting.mirrordir + '/' + metadata.filename
        n = metadata.totalnode
        k = metadata.datanode
        src = []
        blocknums = []
        chunklist = []
        failedchunks = []
        failedchunkname = []
        failedchunktype = []
        for i in range(metadata.totalchunk):
            nodeid = metadata.chunkInfo[i].nodeid
            chunkname = setting.chunkdir + '/' + metadata.filename + '.node' + str(i)
            chunktype = metadata.chunkInfo[i].chunktype
            chunklist.append(chunkname)
            if metadata.chunkInfo[i].action == 'download':
                src.append(chunkname)
                blocknums.append(i)
            else:
                failedchunkname.append(chunkname)
                failedchunktype.append(chunktype)
                if metadata.chunkInfo[i].action == 'sos':
                    failedchunks.append(i)

        if mode == 'download' and ('native' not in failedchunktype):
            codingutil.join(src, dest, metadata.filesize)
        elif mode == 'download' and ('native' in failedchunktype):
            codingutil.ecDecodeFile(n, k, src, blocknums, dest, metadata.filesize, setting)
        elif mode == 'repair' and ('native' in failedchunktype):
            codingutil.ecDecodeChunks(n, k, src, blocknums, chunklist, setting, failed=failedchunks)
        elif mode == 'repair':
            codingutil.ecDecodeChunks(n, k, src, blocknums, chunklist, setting, failed=failedchunks, nativeFailed=False)
        else:
            return False

        return True
    else:
        return False


def repairFile(settingOld, settingNew, metadata):
    '''Repair file by rebuilding failed chunks.'''
    decodeFile(settingOld, metadata, 'repair')

    for i in range(len(metadata.chunkInfo)):
        chunkname = metadata.filename + '.node' + str(i)
        src = settingNew.chunkdir + '/' + chunkname
        if metadata.chunkInfo[i].action == 'sos':
            nodeid = metadata.chunkInfo[i].nodeid
            metadata.chunkInfo[i].chunkname = chunkname
            metadata.chunkInfo[i].chunkpath = src
            metadata.chunkInfo[i].nodekey = settingNew.nodeInfo[nodeid].nodekey
            metadata.chunkInfo[i].nodetype = settingNew.nodeInfo[nodeid].nodetype
            metadata.chunkInfo[i].bucketname = settingNew.nodeInfo[nodeid].bucketname
            metadata.chunkInfo[i].action = 'upload'
            #Add support for big-chunk:
            metadata.fileNodeInfo[nodeid].nodekey = metadata.chunkInfo[i].nodekey
            metadata.fileNodeInfo[nodeid].nodetype = metadata.chunkInfo[i].nodetype
            metadata.fileNodeInfo[nodeid].bucketname = metadata.chunkInfo[i].bucketname
            metadata.fileNodeInfo[nodeid].bigchunkpath = src
            metadata.fileNodeInfo[nodeid].bigchunkname = chunkname
            metadata.fileNodeInfo[nodeid].chunknum = 1
            metadata.fileNodeInfo[nodeid].action = 'upload'
        else:
            metadata.chunkInfo[i].chunkname = chunkname
            metadata.chunkInfo[i].chunkpath = chunkname
