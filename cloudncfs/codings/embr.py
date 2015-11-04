#!/usr/bin/python
#
# @name = 'embr.py'
# 
# @description = "E-MBR(n,k) coding."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#


import os

import common
import codingutil
import embrutil

def encodeFile(setting, metadata):
    '''Encode file into chunks.'''
    metadata.datanode = setting.datanode
    src = setting.mirrordir + '/' + metadata.filename
    dest = []
    dataChunks = []
    parityChunks = []
    n = metadata.totalnode
    k = metadata.datanode
    nativeBlockNum = embrutil.getNativeBlockNum(n, k)
    parityBlockNum = embrutil.getParityBlockNum(n, k)
    authenticNum = nativeBlockNum + parityBlockNum
    nodeIdList = embrutil.getNodeIdList(n, k)
    for i in range(nativeBlockNum):
        chunkname = setting.chunkdir + '/' + metadata.filename + '.chunk' + str(i)
        dataChunks.append(chunkname)
        dest.append(chunkname)
    for i in range(parityBlockNum):
        parityname = setting.chunkdir + '/'+ metadata.filename + '.chunk' \
                                             + str(authenticNum-parityBlockNum+i)
        parityChunks.append(parityname)
        dest.append(parityname)
    codingutil.ecEncode(src, dataChunks, parityChunks, setting)

    #Generate info for big-chunk:
    bigChunkPaths = []
    for i in range(metadata.totalnode):
        fileNode = common.FileNodeMetadata(i)
        fileNode.nodekey = setting.nodeInfo[i].nodekey
        fileNode.nodetype = setting.nodeInfo[i].nodetype
        fileNode.bucketname = setting.nodeInfo[i].bucketname
        fileNode.bigchunksize = 0
        fileNode.chunknum = 0
        metadata.fileNodeInfo.append(fileNode)
        bigChunkPaths.append([])        

    #Generate info for small chunks:
    for i in range(authenticNum*2):
        chunk = common.ChunkMetadata(i)
        if i < authenticNum:
        #Case of non-replica:
            chunk.chunkname = metadata.filename + '.chunk' + str(i)
            chunk.chunksize = os.path.getsize(dest[i])
            if i < nativeBlockNum:
                chunk.chunktype = 'native'
            else:
                chunk.chunktype = 'parity'
            chunk.chunkpath = dest[i]
        else:
        #Case of replica:
            j = i - authenticNum
            chunk.chunkname = metadata.chunkInfo[j].chunkname
            chunk.chunksize = metadata.chunkInfo[j].chunksize
            chunk.chunktype = 'replica'
            chunk.chunkpath = metadata.chunkInfo[j].chunkpath
        nodeid = nodeIdList[i]
        chunk.nodeid = nodeid
        chunk.nodekey = setting.nodeInfo[nodeid].nodekey
        chunk.nodetype = setting.nodeInfo[nodeid].nodetype
        chunk.bucketname = setting.nodeInfo[nodeid].bucketname
        chunk.action = 'upload'
        #Add chunk position inside big-chunk:
        chunk.position = metadata.fileNodeInfo[nodeid].chunknum
        metadata.chunkInfo.append(chunk)
        #Add support for big-chunk:
        metadata.fileNodeInfo[nodeid].bigchunksize += chunk.chunksize
        metadata.fileNodeInfo[nodeid].chunknum += 1
        bigChunkPaths[nodeid].append(chunk.chunkpath)
    metadata.totalchunk = authenticNum * 2

    #Generate big-chunks
    for i in range(metadata.totalnode):
        dest = setting.chunkdir + '/' + metadata.filename + '.node' + str(i)
        codingutil.join(bigChunkPaths[i],dest,metadata.fileNodeInfo[i].bigchunksize)
        metadata.fileNodeInfo[i].bigchunkpath = dest
        metadata.fileNodeInfo[i].bigchunkname = metadata.filename + '.node' + str(i)
        metadata.fileNodeInfo[i].action = 'upload'


def updateMetadataForDecode(setting, metadata, mode):
    '''Update metadata for retrieving chunks for decode.'''
    if setting.healthynode == setting.totalnode:
        for i in range(metadata.totalchunk):
            if metadata.chunkInfo[i].chunktype == 'native':
                metadata.chunkInfo[i].action = 'download'
        return True
    elif setting.healthynode == metadata.totalnode - 1:
        #Case of one-node failure:
        n = metadata.totalnode
        k = metadata.datanode
        nativeBlockNum = embrutil.getNativeBlockNum(n, k)
        parityBlockNum = embrutil.getParityBlockNum(n, k)
        authenticNum = nativeBlockNum + parityBlockNum
        for i in range(metadata.totalchunk):
            chunktype = metadata.chunkInfo[i].chunktype
            if chunktype == 'native' or (mode == 'repair' and chunktype == 'parity'):
                nodeid = metadata.chunkInfo[i].nodeid
                if setting.nodeInfo[nodeid].healthy == True:
                    if mode == 'download':
                        metadata.chunkInfo[i].action = 'download'
                    elif mode == 'repair':
                        nodeid_dup = metadata.chunkInfo[i+authenticNum].nodeid
                        if setting.nodeInfo[nodeid_dup].healthy == False:
                            metadata.chunkInfo[i].action = 'download'
                else:
                    #Case of failed node. Find replica to replace:
                    metadata.chunkInfo[i+authenticNum].action = 'download'
        return True
    elif setting.healthynode >= metadata.datanode:
        #Case of two or more node failure:
        n = metadata.totalnode
        k = metadata.datanode
        nativeBlockNum = embrutil.getNativeBlockNum(n, k)
        parityBlockNum = embrutil.getParityBlockNum(n, k)
        authenticNum = nativeBlockNum + parityBlockNum
        count = 0
        for i in range(metadata.totalchunk):
            chunktype = metadata.chunkInfo[i].chunktype
            if chunktype == 'native' or chunktype == 'parity':
                nodeid = metadata.chunkInfo[i].nodeid
                nodeidReplica = metadata.chunkInfo[i+authenticNum].nodeid
                if setting.nodeInfo[nodeid].healthy == True:
                    metadata.chunkInfo[i].action = 'download'
                    count += 1
                elif setting.nodeInfo[nodeidReplica].healthy == True:
                #case of failed node. find replica to replace
                    metadata.chunkInfo[i+authenticNum].action = 'download'
                    count += 1
                else:
                    metadata.chunkInfo[i].action = 'sos'
                #Only need to get sufficient chunks for decode:    
                if count == nativeBlockNum:
                    break
        return True
    else:
        return False


def decodeFile(setting, metadata, mode):
    '''Decode chunks into original file.'''
    n = metadata.totalnode
    k = metadata.datanode
    nativeBlockNum = embrutil.getNativeBlockNum(n, k)
    parityBlockNum = embrutil.getParityBlockNum(n, k)
    authenticNum = nativeBlockNum + parityBlockNum
    if setting.healthynode >= setting.totalnode-1:
        dest = setting.mirrordir + '/' + metadata.filename
        src = []
        for i in range(nativeBlockNum):
            chunkname = setting.chunkdir + '/' + metadata.filename + '.chunk' + str(i) 
            src.append(chunkname)
        if mode == 'download':
            codingutil.join(src, dest, metadata.filesize)
    elif setting.healthynode >= metadata.datanode:
        #Case of two or more node failure:
        dest = setting.mirrordir + '/' + metadata.filename
        n = int(metadata.totalchunk/2)
        k = nativeBlockNum
        src = []
        blocknums = []
        chunklist = []
        failedchunks = []
        failedchunktype = []
        for i in range(int(metadata.totalchunk/2)):
            chunkname = setting.chunkdir + '/' + metadata.filename + '.chunk' + str(i)
            chunktype = metadata.chunkInfo[i].chunktype
            authenticChunkAction = metadata.chunkInfo[i].action
            replicaChunkAction = metadata.chunkInfo[i+authenticNum].action
            chunklist.append(chunkname)
            if authenticChunkAction == 'sos' and replicaChunkAction == 'sos':
                failedchunktype.append(chunktype)
                failedchunks.append(i)
            elif authenticChunkAction == 'download' or replicaChunkAction == 'download':
                src.append(chunkname)
                blocknums.append(i)

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
    
    #Decode chunk for two or more node failure:
    if settingOld.healthynode <= settingOld.totalnode - 2:
        decodeFile(settingOld, metadata, 'repair')
    
    n = metadata.totalnode
    k = metadata.datanode
    nativeBlockNum = embrutil.getNativeBlockNum(n, k)
    parityBlockNum = embrutil.getParityBlockNum(n, k)
    authenticNum = nativeBlockNum + parityBlockNum
    #Paths of chunk files for generating a big-chunk:
    bigChunkPaths = []
    for i in range(metadata.totalnode):
        bigChunkPaths.append([])

    #Find replicas of failed chunks:
    for i in range(len(metadata.chunkInfo)):
        if i < authenticNum:
            chunkname = metadata.filename + '.chunk' + str(i)
        else:
            chunkname = metadata.filename + '.chunk' + str(i-authenticNum)
        src = settingNew.chunkdir + '/' + chunkname
        if metadata.chunkInfo[i].action == 'sos':
            nodeid = metadata.chunkInfo[i].nodeid
            metadata.chunkInfo[i].chunkname = chunkname
            metadata.chunkInfo[i].chunkpath = src
            metadata.chunkInfo[i].nodekey = settingNew.nodeInfo[nodeid].nodekey
            metadata.chunkInfo[i].nodetype = settingNew.nodeInfo[nodeid].nodetype
            metadata.chunkInfo[i].bucketname = settingNew.nodeInfo[nodeid].bucketname
            metadata.chunkInfo[i].action = 'upload'
            #Add chunk path to bigChunkPaths:
            bigChunkPaths[nodeid].append(src)
        else:
            metadata.chunkInfo[i].chunkname = chunkname
            metadata.chunkInfo[i].chunkpath = chunkname

    #Add support for big-chunk:
    for i in range(metadata.totalnode):
        if settingOld.nodeInfo[i].healthy == False:
            dest = settingOld.chunkdir + '/' + metadata.filename + '.node' + str(i)
            codingutil.join(bigChunkPaths[i],dest,metadata.fileNodeInfo[i].bigchunksize)
            metadata.fileNodeInfo[i].bigchunkpath = dest
            metadata.fileNodeInfo[i].bigchunkname = metadata.filename + '.node' + str(i)
            metadata.fileNodeInfo[i].action = 'upload'
