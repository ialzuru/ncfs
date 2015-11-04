#!/usr/bin/python
#
# @name = 'fmsrutil.py'
# 
# @description = "F-MSR utilities module."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import sys
import os
import random

from finitefield import GF256int
from coeffvector import CoeffVector
from coeffvector import CoeffMatrix

import common

#Check if C library of F-MSR is installed:
import codings.clibfmsr.clibfmsr
useClibfmsr = True


def getNativeBlockNum(n, k):
    '''Get number of native blocks.'''
    return k*(n-k)


def getParityBlockNum(n, k):
    '''Get number of parity blocks.'''
    return n*(n-k)


def getNodeIdList(n, k):
    '''Find the node id for a segment of blocks.'''
    '''Return a list of node id for the blocks.'''
    nodeidList = []
    segmentSize = n-k
    blockNum = getParityBlockNum(n, k)
    for i in range(int(blockNum/segmentSize)):
        for j in range(segmentSize):
            nodeidList.append(i)
    return nodeidList


def getParityCoeff(n, k):
    '''Get the parity coefficients of the blocks.'''
    nativeBlockNum = getNativeBlockNum(n, k)
    parityBlockNum = getParityBlockNum(n, k)
    parityCoeff = []
    for i in range(parityBlockNum):
        for j in range(nativeBlockNum):
            parityCoeff.append(GF256int(i+1)**j)
    return parityCoeff


def encode(n, k, src, parityCoeff, setting, metadata):
    '''Encode src file to parity chunks.'''

    nativeBlockNum = getNativeBlockNum(n, k)
    parityBlockNum = getParityBlockNum(n, k)

    infile = open(src, 'rb')
    indatalist = infile.read()
    infile.close()
    totalchunk = nativeBlockNum
    filesize = len(indatalist)

    #Generate info for big-chunk:
    for i in range(metadata.totalnode):
        fileNode = common.FileNodeMetadata(i)
        fileNode.nodekey = setting.nodeInfo[i].nodekey
        fileNode.nodetype = setting.nodeInfo[i].nodetype
        fileNode.bucketname = setting.nodeInfo[i].bucketname
        fileNode.bigchunksize = 0
        fileNode.chunknum = 0
        metadata.fileNodeInfo.append(fileNode)

    #Encode indatalist to outdatalist
    if filesize > 0:
        chunksize = filesize/totalchunk + 1
        indatalist += '\0'*(chunksize*totalchunk - filesize)
        parityCoeff_temp = ''.join([chr(parity) for parity in parityCoeff])
        outdatalist = codings.clibfmsr.clibfmsr.encodeComputation(indatalist, \
                               parityCoeff_temp, nativeBlockNum, parityBlockNum, chunksize)
    else:
        chunksize = 0

    #Generate info for small chunks:
    nodeIdList = getNodeIdList(n, k)
    for i in range(parityBlockNum):
        chunk = common.ChunkMetadata(i)
        chunk.chunkname = metadata.filename + '.chunk' + str(i)
        chunk.chunksize = chunksize
        chunk.chunktype = 'parity'
        chunk.chunkpath = setting.chunkdir + '/' + chunk.chunkname
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
    metadata.totalchunk = parityBlockNum
    metadata.parityCoeff = parityCoeff[:]

    #Generate big-chunks:
    startchunk = 0
    writelen = 1048576
    for i in range(metadata.totalnode):
        dest = setting.chunkdir + '/' + metadata.filename + '.node' + str(i)
        if chunksize > 0:
            f = open(dest, 'wb')
            numchunks = nodeIdList.count(i)
            writenext = startchunk*chunksize
            for j in range(startchunk*chunksize, (startchunk+numchunks)*chunksize-writelen, writelen):
                writenext = j+writelen
                f.write(outdatalist[j:writenext])
            f.write(outdatalist[writenext:(startchunk+numchunks)*chunksize])
            f.close()
            startchunk += numchunks
        else:
            open(dest, 'wb').close()
        metadata.fileNodeInfo[i].bigchunkpath = dest
        metadata.fileNodeInfo[i].bigchunkname = metadata.filename + '.node' + str(i)
        metadata.fileNodeInfo[i].action = 'upload'


def reversematrix(n, k, gj_matrix):
    '''Reverse matrix.'''

    ## The first elimination: decoding matrix -> lower unit triangular matrix
    nativeBlockNum = getNativeBlockNum(n, k)
    parityBlockNum = getParityBlockNum(n, k)

    for rowNo in range(nativeBlockNum):    
        ##1.find the rowNo row vector with 1st-coeff of valve non-zero    
        A = GF256int(0)  
        for i in range(rowNo,nativeBlockNum,1):
            if gj_matrix[i][rowNo]!=0:
                A = gj_matrix[i][rowNo]
                break

        ##2. permutation between the rowNo row vector and the ith row vector
        temp_vector = [GF256int(0)]*(nativeBlockNum*2)

        if i!= rowNo:
            for j in range(nativeBlockNum*2):
                temp_vector[j]  = gj_matrix[i][j]        
                gj_matrix[i][j] = gj_matrix[rowNo][j]
                gj_matrix[rowNo][j] = temp_vector[j]    
        ##3. in rowNo-th row vector, all the coeffs/1st coeff

        for m in range(nativeBlockNum*2):
            gj_matrix[rowNo][m] = gj_matrix[rowNo][m]/A    

        ##4. The row vectors below rowNo-th row vector eliminate the rowNo-th coeff
        for j in range(rowNo+1,nativeBlockNum,1):
            B = gj_matrix[j][rowNo]
            for m in range(rowNo,nativeBlockNum*2,1):
                gj_matrix[j][m] = gj_matrix[j][m] - gj_matrix[rowNo][m]*B

    # The second elimination: decoding matrix -> unit matrix
    ##5. The row vectors above rowNo-th row vector eliminate the rowNo-th coeff 
    for rowNo in range(nativeBlockNum-1,0,-1):    
        for j in range(0,rowNo,1):
            C = gj_matrix[j][rowNo]
            for m in range(nativeBlockNum*2):
                gj_matrix[j][m] = gj_matrix[j][m] - gj_matrix[rowNo][m]*C


def decode(n, k, src, blocknums, parityCoeff, dest, filesize, setting):
    '''Decode chunk files to dest file.'''

    ## special handling for 0B files
    if filesize <= 0:
        open(dest,'wb').close()
        return

    cv_temp=[]
    nativeBlockNum = getNativeBlockNum(n, k)
    parityBlockNum = getParityBlockNum(n, k)
    enc_matrix = [[GF256int(0) for col in range(nativeBlockNum)] for row in range(parityBlockNum)]
    dec_matrix = [[GF256int(0) for col in range(nativeBlockNum)] for row in range(nativeBlockNum)]
    rev_matrix = [[GF256int(0) for col in range(nativeBlockNum)] for row in range(nativeBlockNum)]
    gj_matrix = [[GF256int(0) for col in range(nativeBlockNum*2)] for row in range(nativeBlockNum)]

    ## generate the encoding matrix
    counter = 0
    for i in range(parityBlockNum):
        for j in range(nativeBlockNum):
            enc_matrix[i][j] = GF256int(parityCoeff[counter])
            counter += 1

    cm1 = CoeffMatrix(nativeBlockNum)
    for i in range(parityBlockNum):
        cv_temp.append(CoeffVector(nativeBlockNum))
        for j in range(nativeBlockNum):
            cv_temp[i].coeff_[j] = enc_matrix[i][j]
        cv_temp[i].first()
        cm1.addcoeffvector(cv_temp[i])

    ## generate the decoding matrix
    i=0
    for selectChunkNo in blocknums:
        for j in range(nativeBlockNum):
            dec_matrix[i][j]=enc_matrix[selectChunkNo][j]
        i += 1

    ## initialize the reverse matrix
    for i in range(nativeBlockNum):
        for j in range(nativeBlockNum):
            if j==i:
                rev_matrix[i][j]= GF256int(1)

    ## initialize the Gauss-Jordan matrix = [decoding,reverse]
    for i in range(nativeBlockNum):
        for j in range(nativeBlockNum*2):
            if j<nativeBlockNum:
                gj_matrix[i][j]= dec_matrix[i][j]
            else:
                gj_matrix[i][j]= rev_matrix[i][j-nativeBlockNum]

    reversematrix(n, k, gj_matrix)

    for i in range(nativeBlockNum):
        for j in range(nativeBlockNum):
            dec_matrix[i][j] = gj_matrix[i][j+nativeBlockNum]

    ##generate decode data chunks
    selectchunk=[]
    for filename in src:
        infile = open(filename,'rb')
        selectchunk.append(infile.read())
        infile.close()
    chunksize = os.path.getsize(src[0])
    indatalist = ''.join(selectchunk)

    ##rebuild the original chunks
    parityCoeff_temp = ''.join([chr(dec_matrix[i][j]) \
                                for i in range(nativeBlockNum) \
                                for j in range(nativeBlockNum)])
    outdatalist = codings.clibfmsr.clibfmsr.decodeComputation(indatalist, \
                           parityCoeff_temp, nativeBlockNum, chunksize)

    outfile = open(dest,'wb')
    writelen = 1048576
    writenext = 0
    for i in range(0,filesize-writelen,writelen):
        writenext = i+writelen
        outfile.write(outdatalist[i:writenext])
    outfile.write(outdatalist[writenext:filesize])
    outfile.close()


def getCheckNum(parityBlockNum):
    '''Get check number for checking strong MDS, for fmsr(k=n-2) only.'''
    return int((parityBlockNum-2)*(parityBlockNum-2-1)/2 - ((parityBlockNum/2)-1))

def getStrongMDSPropertyDegree(repairNodeno, nativeBlockNum, parityBlockNum, checkNum, enc_matrix):
    '''Get strong MDS property degree.'''

    currentStrongMDSPropertyDegree = 0
    survivalcoeffvectorset = []
    flag = 0
    for i in range(parityBlockNum):
    #get coeff vectors of survival parity blocks
        if int(i/2)!= repairNodeno:
            survivalcoeffvectorset.append(CoeffVector(nativeBlockNum))
            for j in range(nativeBlockNum):
                survivalcoeffvectorset[i - flag*2].coeff_[j] = enc_matrix[i][j]
            survivalcoeffvectorset[i - flag*2].first()  
        else:
            flag =1

    s = 0
    for i in range(parityBlockNum-2):
        for j in range(parityBlockNum-2):
            if i<j:
                checkmatrix = CoeffMatrix(nativeBlockNum)
                for k in range (parityBlockNum-2):
                    if k!=i and k!=j:
                        checkmatrix.addcoeffvector(survivalcoeffvectorset[k].copy())
                if checkmatrix.rank_ == nativeBlockNum:
                    currentStrongMDSPropertyDegree += 1
                s += 1
    return currentStrongMDSPropertyDegree

def checkMDS(MSR_n, MSR_k, enc_matrix):
    '''Check MDS property, for fmsr(k=n-2) only.'''
    '''Return a MDS property value.'''

    nativeBlockNum = getNativeBlockNum(MSR_n, MSR_k)
    parityBlockNum = getParityBlockNum(MSR_n, MSR_k)
    MDSpropery = True
    allcoeffvectors = []
    for i in range(parityBlockNum):
        allcoeffvectors.append(CoeffVector(nativeBlockNum))
        for j in range(nativeBlockNum):
            allcoeffvectors[i].coeff_[j] = enc_matrix[i][j]
        allcoeffvectors[i].first()
    permutation = int(MSR_n * (MSR_n - 1) / 2)
    #permutation of selecting n-2 nodes from n nodes
    checkmatrix = [CoeffMatrix(nativeBlockNum) for col in range(permutation)]
    s = 0
    for i in range (MSR_n):
        for j in range(MSR_n):
            if i<j:
                for b in range(MSR_n):
                    if b !=i and b!=j:
                        checkmatrix[s].addcoeffvector(allcoeffvectors[b*2].copy())
                        checkmatrix[s].addcoeffvector(allcoeffvectors[b*2+1].copy())
                if checkmatrix[s].rank_ != nativeBlockNum:
                    MDSpropery = False
                s += 1
    return MDSpropery

def checkstongMDS(n, k, nativeBlockNum, parityBlockNum, enc_matrix):
    '''Check strong MDS property, for fmsr(k=n-2) only.'''
    '''Return list of MDS property degrees.'''

    strongMDSPropertyDegrees = []
    #get check-combination number
    checkNum = getCheckNum(parityBlockNum)
    #Calculate total strong MDS property degree
    for i in range(n):
        strongMDSPropertyDegrees.append(getStrongMDSPropertyDegree(i, \
                      nativeBlockNum, parityBlockNum, checkNum, enc_matrix))
    return strongMDSPropertyDegrees


def testStrongMDSProperty(strongMDSPropertyDegrees, checkNum,n):
    '''Decide whether the current parity coefficient set passes the strong MDS property.'''

    result = True
    #threshold = checkNum
    threshold = 2*(n-1)*(n-2)-(n-2)*(n-3)/2
    #Important: currently the threshold value is hardcode
    for degree in strongMDSPropertyDegrees:
        if degree < threshold:
            result = False
    return result


def functionalRepair(n, k, src, blocknums, failedNode, parityCoeff, repairChunks, setting, metadata):
    '''Functional repair by generating new parity chunks.'''

    nativeBlockNum = getNativeBlockNum(n, k)
    parityBlockNum = getParityBlockNum(n, k)
    checkNum = getCheckNum(parityBlockNum)

    ## read the encoding matrix and repair
    enc_matrix = metadata.enc_matrix
    repairCodingCoeff = metadata.repairCodingCoeff

    indatalist = []
    for filepath in src:
        infile = open(filepath, 'rb')
        indatalist.append(infile.read())
        infile.close()
    chunksize = os.path.getsize(src[0])

    if chunksize > 0:
        #Repair computation:
        indatalist_temp = ''.join(indatalist)
        parityCoeff_temp = []
        for i in range(n-k):
            for j in range(n-1):
                parityCoeff_temp.append(chr(repairCodingCoeff[i][j]))
        parityCoeff_temp = ''.join(parityCoeff_temp)
        outdatalist = codings.clibfmsr.clibfmsr.repairComputation(indatalist_temp, \
                           parityCoeff_temp, n, k, chunksize)

    counter = 0
    for i in range(parityBlockNum):
        for j in range(nativeBlockNum):
            parityCoeff[counter] = enc_matrix[i][j]
            counter += 1

    #Add support for big-chunk:
    writelen = 1048576
    writenext = 0
    for i in range(metadata.totalnode):
        if setting.nodeInfo[i].healthy == False:
            dest = setting.chunkdir + '/' + metadata.filename + '.node' + str(i)
            filesize = metadata.fileNodeInfo[i].bigchunksize
            if chunksize <= 0:
                open(dest,'wb').close()
            else:
                outfile = open(dest, 'wb')
                for j in range(0,filesize-writelen,writelen):
                    writenext = j+writelen
                    outfile.write(outdatalist[j:writenext])
                outfile.write(outdatalist[writenext:filesize])
                outfile.close()
            metadata.fileNodeInfo[i].bigchunkpath = dest
            metadata.fileNodeInfo[i].bigchunkname = metadata.filename + '.node' + str(i)
            metadata.fileNodeInfo[i].action = 'upload'

