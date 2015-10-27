#!/usr/bin/python
#
# @name = 'clean.py'
# 
# @description = "Utility module for cleaning temporary files."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import os

import common

settingpath = common.settingpath

def cleanDir(targetdir):
    '''Clean all files under the directory targetdir.'''
    fileList = []
    dirList = []
    #Find the files and sub-directories in targetdir.
    for root, dirs, files in os.walk(targetdir):
        for fl in files:
            filepath = '/'.join([root, fl])
            fileList.append(filepath)
        for dr in dirs:
            dirpath = '/'.join([root, dr])
            dirList.append(dirpath)
    #Remove files
    for filepath in fileList:
        os.unlink(filepath)
    #Remove sub-directories
    for dirpath in dirList:
        os.rmdir(dirpath)

def cleanAll():
    '''Overall clean process.'''
    setting = common.Setting()
    setting.read(settingpath)
    #Clean the files in mirrordir, chunkdir and metadatadir.
    cleanDir(setting.mirrordir)
    cleanDir(setting.chunkdir)
    cleanDir(setting.metadatadir)

if __name__ == '__main__':
    cleanAll()
