#!/usr/bin/python
#
# @name = 'coding.py'
# 
# @description = "Controller of coding modules."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#


import os

import common
import codings
from codings import *

codinglist = codings.__all__


def encodeFile(setting,metadata):
    '''Encode file into chunks.'''
    filesize = os.path.getsize(setting.mirrordir + "/" + metadata.filename)
    print "meta file path", setting.mirrordir + "/" + metadata.filename
    print "encode file filesize", filesize
    metadata.filesize = filesize
    #Call encodeFile function in corresponding module:
    if metadata.coding in codinglist:
        module = globals()[metadata.coding]
        func = getattr(module, 'encodeFile')
        return func(setting, metadata)
    else:
        return False


def updateMetadataForDecode(setting, metadata, mode):
    '''Update metadata for retrieving chunks for decode.'''
    #Call updateMetadataForDecode function in corresponding module:
    ret = False
    if metadata.coding in codinglist:
        module = globals()[metadata.coding]
        func = getattr(module, 'updateMetadataForDecode')
        ret = func(setting, metadata, mode)

    if ret == True:
        #Update health information of the chunks:
        for i in range(metadata.totalchunk):
            nodeid = metadata.chunkInfo[i].nodeid
            if setting.nodeInfo[nodeid].healthy == False:
                metadata.chunkInfo[i].healthy = False
                metadata.chunkInfo[i].action = 'sos'
                #Add support for big-chunk:
                metadata.fileNodeInfo[nodeid].healthy = False
                metadata.fileNodeInfo[nodeid].action = 'sos'
        return True
    else:
        return False


def decodeFile(setting, metadata, mode):
    '''Decode chunks into original file.'''
    #Call decodeFile function in corresponding module:
    if metadata.coding in codinglist:
        module = globals()[metadata.coding]
        func = getattr(module, 'decodeFile')
        return func(setting, metadata, mode)
    else:
        return False


def repairFile(settingOld, settingNew, metadata):
    '''Repair a file by generating failed chunks.'''
    #Call repairFile function in corresponding module:
    if metadata.coding in codinglist:
        module = globals()[metadata.coding]
        func = getattr(module, 'repairFile')
        ret = func(settingOld, settingNew, metadata)
    else:
        return False
