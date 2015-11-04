#!/usr/bin/python
#
# @name = 'embrutil.py'
# 
# @description = "E-MBR utilities module."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

def getNativeBlockNum(n, k):
    '''Calculate number of native blocks.'''
    nativeNum =  k*(2*n-k-1)/2
    return nativeNum


def getParityBlockNum(n, k):
    '''Calculate number of parity blocks.'''
    if n-k == 1:
        parityNum = 0
    elif n-k == 2:
        parityNum = 1
    else:
        parityNum = n*(n-1)/2 - k*(2*n-k-1)/2
    return parityNum


def getSegmentSize(n, k):
    '''#Calculate segment size, that is the number of blocks stored per node.'''
    segmentSize = n - 1
    return segmentSize


def getNodeIdList(n, k):
    '''Find the node id for a segment of blocks.'''
    '''Return a list of node id for the segment.'''
    segmentSize = n - 1
    nodeidlist = []

    count = segmentSize
    for i in range(n-1):
        for j in range(count):
            nodeidlist.append(i)
        count -= 1

    count = segmentSize
    for i in range(1,n):
        for j in range(count):
            nodeidlist.append(i+j)
        count -= 1

    return nodeidlist
