#!/usr/bin/python
#
# @name = 'storageutil.py'
#
# @description = "storage utility module."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import os


def syncFile(setting, filename, path):
    '''Sync a file in cloud to mirror dir.'''
    #Get directory entry:
    dirent = filename.split("/")
    #Get filename:
    filename = dirent[-1]
    filename = filename[0:filename.rindex('.node')]
    if len(dirent) > 1:
        #Require to mkdir:
        directory = ""
        for i in range(len(dirent)-1):
            directory = directory + dirent[i] + "/"
        try:
            os.makedirs(setting.chunkdir + "/" + directory)
            os.makedirs(setting.metadatadir + "/" + directory)
            os.makedirs(path + "/" + directory)
        except:
            #Pass if directory already existed:
            pass
        objFile = open(path + "/" + directory + filename,"a+")
        objFile.close()
    else:
        #Simply touch file:
        objFile = open(path + "/" + filename,"a+")
        objFile.close()
