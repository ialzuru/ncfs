#!/usr/bin/python
#
# @name = 'fmsrtwo.py'
# 
# @description = "F-MSR(n,n-2) coding."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#


import os
import random
import sys

import common
import codingutil
import fmsrutil

from finitefield import GF256int
from coeffvector import CoeffVector
from coeffvector import CoeffMatrix

def encodeFile(setting, metadata):
    '''Encode file into chunks.'''
    metadata.datanode = setting.datanode
    src = setting.mirrordir + '/' + metadata.filename

    n = metadata.totalnode
    k = n - 2
    nativeBlockNum = fmsrutil.getNativeBlockNum(n, k)
    parityBlockNum = fmsrutil.getParityBlockNum(n, k)
    parityCoeff = []
    path = setting.coeffdir + '/' + setting.uuid
    if not os.path.exists(path):
      parityCoeff = fmsrutil.getParityCoeff(n, k)
    else:
      with open(path, 'rb') as f:
        for i in f.read()[2:]:
          parityCoeff.append(GF256int(ord(i)))

    #Encode src file into parity chunks:
    fmsrutil.encode(n, k, src, parityCoeff, setting, metadata)


def updateMetadataForDecode(setting, metadata, mode):
    '''Update metadata for retrieving chunks for decode.'''
    if setting.healthynode >= setting.totalnode - 2:
        #Case of one-node failure:
        n = metadata.totalnode
        k = n - 2
        nativeBlockNum = fmsrutil.getNativeBlockNum(n, k)
        requiredBlockNum = 0
        if mode == 'download':
            requiredBlockNum = nativeBlockNum
        elif mode == 'repair' and setting.healthynode == setting.totalnode-1:
            requiredBlockNum = metadata.totalnode - 1
        elif mode == 'repair' and setting.healthynode == setting.totalnode-2:
            #Repair of 2-node failure is not supported.
            return False
        else:
            return False

        count = 0
        if mode == 'download':
            for i in range(metadata.totalchunk):
                nodeid = metadata.chunkInfo[i].nodeid
                if setting.nodeInfo[nodeid].healthy == True:
                    metadata.chunkInfo[i].action = 'download'
                    count += 1
                    if count == requiredBlockNum:
                        break
        elif mode == 'repair':
            #Get parity coefficients:
            parityCoeff = metadata.parityCoeff[:]
            #Get the first failed node:
            failedNode = 0
            for i in range(metadata.totalnode):
                if setting.nodeInfo[i].healthy == False:
                    failedNode = i
                    break
            #Get coding information:
            nativeBlockNum = fmsrutil.getNativeBlockNum(n, k)
            parityBlockNum = fmsrutil.getParityBlockNum(n, k)
            checkNum = fmsrutil.getCheckNum(parityBlockNum)

            #Initialize strongMDSPropertyDegrees:
            strongMDSPropertyDegrees = []
            MDSproperty = False
            for i in range(n):
                strongMDSPropertyDegrees.append(0)

            fn = setting.coeffdir + '/' + setting.uuid + '.' + str(failedNode)
            if os.path.exists(fn):
              #Load enc_matrix and repairCodingCoeff directly from file (if it exists)
              print >> sys.stderr, "Using offline coefficent generation."
              with open(fn) as f:
                content = f.read()
                enc_matrix_len = ord(content[0]) + (ord(content[1]) << 8)
                enc_matrix = [[ord(content[2+row*nativeBlockNum+col]) for col in range(nativeBlockNum)] for row in range(parityBlockNum)]
                s = enc_matrix_len + 2
                num_chunks_download = ord(content[s])
                s += 1
                for chunk in metadata.chunkInfo:
                  if chunk.action == 'download':
                    chunk.action = ''
                for i in xrange(s, s+num_chunks_download):
                  metadata.chunkInfo[ord(content[i])].action = 'download'
                s += num_chunks_download
                num_chunks_repair = ord(content[s])
                cur = s + 1
                repairCodingCoeff = []
                for i in xrange(num_chunks_repair):
                  num_coeff = ord(content[cur])
                  cur += 1
                  tempCoeff = [ord(content[cur+j]) for j in xrange(num_coeff)]
                  repairCodingCoeff.append(tempCoeff)
                  cur += num_coeff
                metadata.enc_matrix = enc_matrix
                metadata.repairCodingCoeff = repairCodingCoeff
            else:
              #Loop of checking until suitable chunks are found:
              while MDSproperty == False or \
                      fmsrutil.testStrongMDSProperty(strongMDSPropertyDegrees, checkNum,n) == False:
                  blocknums = []
                  for chunk in metadata.chunkInfo:
                      if chunk.action == 'download':
                          chunk.action = ''
                  count = 0
                  for i in range(0, metadata.totalchunk, 2):
                      selectChunkInt = random.randint(0,1)
                      j = i + selectChunkInt
                      #Randomly select a chunk of each node:
                      nodeid = j / 2
                      if setting.nodeInfo[nodeid].healthy == True:
                          blocknums.append(j)
                          metadata.chunkInfo[j].action = 'download'
                          count += 1
                          if count == requiredBlockNum:
                              break

                  #Generate the encoding matrix:
                  enc_matrix = [[GF256int(0) for col in range(nativeBlockNum)] for row in range(parityBlockNum)]
                  counter = 0
                  for i in range(parityBlockNum):
                      for j in range(nativeBlockNum):
                          enc_matrix[i][j] = GF256int(parityCoeff[counter])
                          counter += 1

                  #for fmsr(k=n-2) only
                  repairCodingCoeff = [[GF256int(0) for col in range(n-1)] for row in range(n-k)]
                  for i in range(n-k):
                      for j in range(n-1):
                          repairCodingCoeff[i][j] = GF256int(random.randint(0,255))

                  #Update the encoding matrix:
                  #for fmsr(k=n-2) only
                  for i in range(nativeBlockNum):
                      for p in range(n-k):
                          enc_matrix[failedNode*2+p][i] = 0
                          for q in range(n-1):
                              enc_matrix[failedNode*2+p][i] \
                                  += repairCodingCoeff[p][q] * enc_matrix[blocknums[q]][i]

                  #Get the strongMDS property degree:
                  strongMDSPropertyDegrees = fmsrutil.checkstongMDS(n,k,nativeBlockNum,parityBlockNum,enc_matrix)
                  #Get the MDS property value:
                  MDSproperty = fmsrutil.checkMDS(n, k, enc_matrix)

                  #Store matrix for repair use
                  metadata.enc_matrix = enc_matrix
                  metadata.repairCodingCoeff = repairCodingCoeff

        if count == requiredBlockNum:
            return True
        else:
            return False
    else:
        return False


def decodeFile(setting, metadata, mode):
    '''Decode chunks into original file.'''
    n = metadata.totalnode
    k = n - 2
    nativeBlockNum = fmsrutil.getNativeBlockNum(n, k)
    parityBlockNum = fmsrutil.getParityBlockNum(n, k)

    if setting.healthynode >= setting.totalnode-2:
        dest = setting.mirrordir + '/' + metadata.filename
        src = []
        blocknums = []
        failedNodes = []
        repairChunks = []
        #Read retrieved chunks:
        for i in range(metadata.totalchunk):
            nodeid = metadata.chunkInfo[i].nodeid
            chunkname = setting.chunkdir + '/' + metadata.filename + '.chunk' + str(i)
            chunktype = metadata.chunkInfo[i].chunktype
            if metadata.chunkInfo[i].action == 'download':
                src.append(chunkname)
                blocknums.append(i)
            elif metadata.chunkInfo[i].action == 'sos':
                repairChunks.append(chunkname)
                #if nodeid not in failedNodes:
                #    failedNodes.append(nodeid)
        for i in range(metadata.totalnode):
            if setting.nodeInfo[i].healthy == False:
                failedNodes.append(i)
        destChunks = []
        if mode == 'download':
            #Decode chunks into file:
            fmsrutil.decode(n, k, src, blocknums, metadata.parityCoeff, \
                            dest, metadata.filesize, setting)
            return True
        elif mode == 'repair':
            if setting.healthynode == setting.totalnode-1:
                #Repair failed chunks by functional repair:
                #Only support 1-node recovery now.
                fmsrutil.functionalRepair(n, k, src, blocknums, failedNodes[0], \
                                          metadata.parityCoeff, repairChunks, \
                                          setting, metadata)
                return True
            else:
                return False
        else:
            return False
    else:
        return False


def repairFile(settingOld, settingNew, metadata):
    '''Repair file by rebuilding failed chunks.'''
    #Currently only support 1-node repair for 1-node failure.
    if settingOld.healthynode == settingOld.totalnode - 1:
        #decode for 1-node failure
        decodeFile(settingOld, metadata, 'repair')

    for i in range(len(metadata.chunkInfo)):
        chunkname = metadata.filename + '.chunk' + str(i)
        src = settingNew.chunkdir + '/' + chunkname
        nodeid = metadata.chunkInfo[i].nodeid
        #if metadata.chunkInfo[i].action == 'sos':
        if settingOld.nodeInfo[nodeid].healthy == False:    
            metadata.chunkInfo[i].chunkname = chunkname
            metadata.chunkInfo[i].chunkpath = src
            metadata.chunkInfo[i].nodekey = settingNew.nodeInfo[nodeid].nodekey
            metadata.chunkInfo[i].nodetype = settingNew.nodeInfo[nodeid].nodetype
            metadata.chunkInfo[i].bucketname = settingNew.nodeInfo[nodeid].bucketname
            metadata.chunkInfo[i].action = 'upload'
