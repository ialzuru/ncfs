#!/usr/bin/python
#
# @name = 'info.py'
# 
# @description = "Utility module for display information of files."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import os
import sys

import common
import storage

settingpath = common.settingpath

def infoNode(setting):
    '''Display information of health of nodes.'''
    for node in setting.nodeInfo:
        print node.nodeid,
        print node.nodetype,
        print node.bucketname,
        if node.healthy == True:
            print "Available"
        else:
            print "Unavailable"


def infoFile(setting, filename):
    '''Display information of a file.'''
    metadata = common.FileMetadata(filename,0,0,0)
    storage.downloadMetadata(setting, metadata)
    #Display file info:
    print metadata.filename,
    print metadata.filesize,
    print metadata.coding


def infoAll(mode='all'):
    '''Overall info process.'''
    setting = common.Setting()
    setting.read(settingpath)
    storage.initStorages(setting)
    storage.checkHealth(setting)
    storage.syncMirror(setting, setting.mirrordir)
    #Get file list recrusively from directory:
    fileList = []
    for root, dirs, files in os.walk(setting.mirrordir):
        for fl in files:
            filepath = '/'.join([root, fl])
            filepath = filepath[len(setting.mirrordir):]
            if filepath[0] == '/':
                filepath = filepath[1:]
            fileList.append(filepath)
    if mode == 'all' or mode == 'node':
        #Info nodes:
        print "*** Node information ***"
        infoNode(setting)
    if mode == 'all' or mode == 'file':    
        #Info files:
        print "*** File information ***"
        for targetFile in fileList:
            infoFile(setting, targetFile)   

if __name__ == '__main__':
    mode = 'all'
    if len(sys.argv) > 1:
        if 'node' in sys.argv[1]:
            mode = 'node'
        elif 'file' in sys.argv[1]:
            mode = 'file'
        elif sys.argv[1] == 'help':
            mode = 'help'
    
    if mode == 'help':
        #Help information
        print "This program is used to display information of the nodes and files."
        print "Optional arguments:"
        print "   node -- Display information of nodes only."
        print "   file -- Display information of files only."
        print "   help -- Display this help information."
    else:
        #Print information
        infoAll(mode)
