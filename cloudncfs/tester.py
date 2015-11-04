#!/usr/bin/python
#
# @name = 'tester.py'
# 
# @description = "Unit test program."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#


import unittest
import sys
import os
import shutil
import filecmp

import common
import workflow
import storage
import clean

settingpath = common.settingpath
infilename = 'data.in'
outfilename = 'data.out'
tempfilename = 'data.temp'


class TestFunctions(unittest.TestCase):
    '''Test the functions of CloudNCFS.'''
    
    def setUp(self):
        #Setup for testing:
        self.setting = common.Setting()
        self.setting.read(settingpath)
        storage.initStorages(self.setting)
        storage.checkHealth(self.setting)
        
        
    def test_consistency(self):
        #Test the upload and download consistency:
        print "Test consistency of upload and download."
        tempfilepath = self.setting.mirrordir + "/" + tempfilename
        #Copy infile to tempfile in mirror dir:
        shutil.copyfile(infilename, tempfilepath) 
        filesize = os.path.getsize(tempfilepath)
        #Upload tempfile:
        metadata = common.FileMetadata(tempfilename,filesize,self.setting.totalnode,self.setting.coding)
        workflow.uploadFile(self.setting, metadata)
        print "Upload finishes."
        #Clean temporary directories:
        clean.cleanAll()
        print "Clean finishes."
        #Download tempfile:
        metadata = common.FileMetadata(tempfilename,0,self.setting.totalnode,self.setting.coding)
        workflow.downloadFile(self.setting, metadata)
        print "Download finishes."
        #Copy tempfile to outfile:
        shutil.copyfile(tempfilepath, outfilename)
        #Clean data in cloud and temporary directories:
        metadata = common.FileMetadata(tempfilename,0,self.setting.totalnode,self.setting.coding)
        workflow.deleteFile(self.setting, metadata)
        clean.cleanAll()
        #Check if infile is same as outfile:
        print "test file difference"
        self.assertEqual(filecmp.cmp(infilename,outfilename), 1)
        #Delete outfile:
        os.unlink(outfilename)


if __name__ == '__main__':
    #Test functions of CloudNCFS:
    if os.path.exists(infilename):
        print "Test data file: " + infilename
        print "File size: " + str(os.path.getsize(infilename))
        unittest.main()
    else:
        print "No test data file found."
        print "Please store test data file as " + infilename
