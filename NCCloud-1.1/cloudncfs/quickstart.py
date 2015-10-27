#!/usr/bin/python
#
# @name = 'quickstart.py'
# 
# @description = "Generate setting file by asking user's preference."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import os

import common
import codings
import storages

settingpath = common.settingpath

def quickstart():
    '''Generate setting file in interactive mode.'''
    
    print "********************************************"
    print "*** CloudNCFS Quickstart program"
    print "*** Helps you to generate a setting file."
    print "********************************************"
    setting = common.Setting()

    #Pre-set values:
    setting.storagethreads = int(1)
    setting.rebuildthreads = int(0)
    setting.autorepair = bool(False)
    setting.writerlock = bool(False)
    setting.zookeeper = bool(False)
    setting.zookeeperloc = '127.0.0.1:2181'
    setting.zookeeperroot = '/'
    setting.testmode = bool(True)
    setting.smartthreads = bool(False)
    setting.smartthreadsmethod = int(3)
    setting.smartthreadslim = int(1024)
    
    setting.uuid = raw_input("Please input a unique identifier: ")

    prompt = "Please input the full path for the coefficient-storing directory: \n"
    setting.coeffdir = raw_input(prompt)
    if not os.path.exists(setting.coeffdir):
        os.mkdir(setting.coeffdir)
        print "Directory created at: " + setting.coeffdir

    #Create temporary directory paths:    
    value = ''
    prompt = "Please input the full path of an existing empty temporary directory: \n"
    while not os.path.exists(value):
        value = raw_input(prompt)
        
    if value[-1] != '/':
        value = value + '/'
        
    setting.mirrordir = value + "mirrordir/" 
    setting.chunkdir = value + "chunkdir/" 
    setting.metadatadir = value + "metadatadir/" 

    if not os.path.exists(setting.mirrordir):
        os.mkdir(setting.mirrordir)
        print "Temporary mirror directory is created at: " + setting.mirrordir

    if not os.path.exists(setting.chunkdir):
        os.mkdir(setting.chunkdir)
        print "Temporary chunk directory is created at: " + setting.chunkdir

    if not os.path.exists(setting.metadatadir):
        os.mkdir(setting.metadatadir)
        print "Temporary metadata directory is created at: " + setting.metadatadir

    #Get storage information:
    print "*******************************"
    print "Please input the details of the storages:"
    
    value = ''
    prompt = "Coding scheme [" + ",".join(codings.__all__) + "]? "
    while value not in codings.__all__:
        value = raw_input(prompt)
    setting.coding = value
    
    value = int(0)
    while value <= 0:
        inputstr = raw_input("Number of storage nodes [1 or more]? ")
        try: value = int(inputstr)
        except: pass
    setting.totalnode = value
    
    value = int(0)
    while value <= 0 or value > setting.totalnode:
        inputstr = raw_input("Number of data nodes among the storage nodes [1 or more]? ")
        try: value = int(inputstr)
        except: pass
    setting.datanode = value

    #Get individual node data:
    print "*******************"
    for i in range(setting.totalnode):
        node = common.NodeData(i,str(i),'','','','','')
        print "*** Information of storage node " + str(i) + ":"

        value = ''
        prompt = "Storage type [" + ",".join(storages.__all__) + "]? "
        while value not in storages.__all__:
            value = raw_input(prompt)
        node.nodetype = value

        value = ''
        value = raw_input("Node location? ")
        node.nodeloc = value

        value = ''
        value = raw_input("Access key? ")
        node.accesskey = value

        value = ''
        value = raw_input("Secret key? ")
        node.secretkey = value

        value = ''
        value = raw_input("Bucket name? ")
        node.bucketname = value
        
        setting.nodeInfo.append(node)
    print "*******************************"

    print ""
    print "Please input the details of spare nodes:"
        
    value = int(-1)
    while value < 0:
        inputstr = raw_input("Number of spare nodes [0 or more]? ")
        try: value = int(inputstr)
        except: pass
    setting.totalsparenode = value

    #Get individual spare node data:
    print "*******************"
    for i in range(setting.totalsparenode):
        node = common.NodeData(i,str(i),'','','','','')
        print "*** Information of spare node " + str(i) + ":"

        value = ''
        prompt = "Storage type [" + ",".join(storages.__all__) + "]? "
        while value not in storages.__all__:
            value = raw_input(prompt)
        node.nodetype = value

        value = ''
        value = raw_input("Node location? ")
        node.nodeloc = value

        value = ''
        value = raw_input("Access key? ")
        node.accesskey = value

        value = ''
        value = raw_input("Secret key? ")
        node.secretkey = value

        value = ''
        value = raw_input("Bucket name? ")
        node.bucketname = value
        
        setting.spareNodeInfo.append(node)
    print "*******************************"

    #Write setting to setting file:
    setting.write(settingpath)
    print "***"
    print "*** The setting file is written to: " + settingpath
    print "*** To start CloudNCFS, you can run:"
    print "*** python cloudncfs/cloudncfs.py"
    print "********************************************"

if __name__ == '__main__':
    quickstart()
