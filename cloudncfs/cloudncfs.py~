#!/usr/bin/python
#
# @name = 'cloudncfs.py'
# 
# @description = "File system interface by FUSE."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import os
import sys

from errno import *
from stat import *
import fcntl
import fuse
from fuse import Fuse

import common
import workflow
import storage
import rebuild
import clean
#import zk #cq


#Check fuse version:
if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."

#Use the stateful_files and has_init options to enable stateful file IO:
fuse.fuse_python_api = (0, 2)
fuse.feature_assert('stateful_files','has_init')

#Get default path of system setting file:
setting = common.Setting()
settingpath = common.settingpath
#Define global state for the system:
globalstate = "normal"


def flag2mode(flags):
    '''Convert file flag to mode.'''
    md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags & os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m

class CloudNCFS(Fuse):
    '''Class of CloudNCFS file system on FUSE.'''

    def __init__(self, *args, **kw):
        '''Initialize Fuse class object.'''
        global settingpath
        Fuse.__init__(self, *args, **kw)

        #Read file system settings:
        setting.read(settingpath)
        self.root = setting.mirrordir
        self.settingpath = os.path.join(os.path.dirname(setting.mirrordir),'setting.cfg')
        settingpath = self.settingpath
        #Clean temporary directories:
        clean.cleanAll()
        """
        #Connect to ZooKeeper servers if needed:
        if setting.zookeeper:
            self.zkLock = zk.ZooKeeperLock(setting)
            setting.zk = self.zkLock
        """
        #Check health of nodes:
        storage.checkHealth(setting)
        #Print system setting and status:
        #for i in range(setting.totalnode):
        #    print "Node " + str(i) + " healthy = " + str(setting.nodeInfo[i].healthy) + "\n"
        #print "HealthyNodes = " + str(setting.healthynode) + "\n"

        #Handle auto-repair:
        if (setting.autorepair == True) and (setting.healthynode < setting.totalnode) \
                and (setting.totalnode - setting.healthynode <= setting.totalsparenode):
            print "Auto-repair starts."
            globalstate = "repair"
            rebuild.rebuildNode()
            setting.__init__()
            setting.read(settingpath)
            globalstate = "normal"
            print "Auto-repair finishes."
            #Update settings for ZooKeeper
            zkLock.updateSetting(setting)
            setting.zk = self.zkLock
            #Check health of nodes after repair:
            storage.checkHealth(setting)
            #Print system setting and status:
            for i in range(setting.totalnode):
                print "Node " + str(i) + " healthy = " + str(setting.nodeInfo[i].healthy) + "\n"
            print "HealthyNodes = " + str(setting.healthynode) + "\n"
            #It is necessary to reset the storage queue for later initStorages():
            storage.resetStorageQueue()

    def getattr(self, path):
        return os.lstat("." + path)

    def readlink(self, path):
        return os.readlink("." + path)

    def readdir(self, path, offset):
        for e in os.listdir("." + path):
            yield fuse.Direntry(e)

    def unlink(self, path):
        '''Remove file.'''
        setting = common.Setting()
        setting.read(self.settingpath)#cq update setting before unlink. repair operation may change setting.cfg
        filename = path[path.index("/")+1:]
        metadata = common.FileMetadata(filename,0,setting.totalnode,setting.coding)
        #Delete chunks and metadata of the file on cloud:
        workflow.deleteFile(setting, metadata)
        #Delete in chunkdir:
        #Delete chunks:
        for chunk in metadata.chunkInfo:
            if chunk.chunktype != 'replica':
                try:
                    os.unlink(setting.chunkdir + "/" + chunk.chunkpath)
                except:
                    pass
        #Delete big-chunks:        
        for bigchunk in metadata.fileNodeInfo:
            try:
                os.unlink(setting.chunkdir + "/" + bigchunk.bigchunkpath)
            except:
                pass
        if setting.coding == 'replication':
            try:
                os.unlink(setting.chunkdir + '/' + metadata.filename + '.node0')
            except:
                pass
        #Delete in metadatadir:
        try:
            if setting.coding != 'replication':   #cq there is no .meta file in replication
                os.unlink(setting.metadatadir + "/" + filename + ".meta")
            os.unlink(setting.metadatadir + "/" + filename + ".metadata")
        except:
            pass
        #Delete file in mirror dir
        os.unlink("." + path)

    def rmdir(self, path):
        '''Remove directory.'''
        #Rmdir in chunkdir and metadatadir:
        os.rmdir(setting.chunkdir + "/" + path)
        os.rmdir(setting.metadatadir + "/" + path)
        #Rmdir in chunkdir and mirrordir:
        os.rmdir("." + path)

    def symlink(self, path, path1):
        os.symlink(path, "." + path1)

    def rename(self, path, path1):
        os.rename("." + path, "." + path1)

    def link(self, path, path1):
        os.link("." + path, "." + path1)

    def chmod(self, path, mode):
        os.chmod("." + path, mode)

    def chown(self, path, user, group):
        os.chown("." + path, user, group)

    def truncate(self, path, len):
        f = open("." + path, "a")
        f.truncate(len)
        f.close()

    def mknod(self, path, mode, dev):
        os.mknod("." + path, mode, dev)

    def mkdir(self, path, mode):
        '''Make directory.'''
        #Mkdir in chunkdir and metadatadir:
        os.mkdir(setting.chunkdir + "/" + path, mode)
        os.mkdir(setting.metadatadir + "/" + path, mode)
        #Mkdir in mirrordir:
        os.mkdir("." + path, mode)

    def utime(self, path, times):
        os.utime("." + path, times)

    def access(self, path, mode):
        if not os.access("." + path, mode):
            return -EACCES

    def statfs(self):
        """
        Should return an object with statvfs attributes (f_bsize, f_frsize...).
        Eg., the return value of os.statvfs() is such a thing (since py 2.2).
        If you are not reusing an existing statvfs object, start with
        fuse.StatVFS(), and define the attributes.

        To provide usable information (ie., you want sensible df(1)
        output, you are suggested to specify the following attributes:

            - f_bsize - preferred size of file blocks, in bytes
            - f_frsize - fundamental size of file blcoks, in bytes
                [if you have no idea, use the same as blocksize]
            - f_blocks - total number of blocks in the filesystem
            - f_bfree - number of free blocks
            - f_files - total number of file inodes
            - f_ffree - nunber of free file inodes
        """

        return os.statvfs(".")

    def fsinit(self):
        '''File system initialisation.'''
        os.chdir(self.root)
        #Initial storage threads again to support non-debug mode:
        storage.initStorages(setting)
        #Synchronize cloud to mirror directory:
        storage.syncMirror(setting, self.root)


    class CloudNcfsFile(object):
        '''Class of stateful file IO.'''

        def __init__(self, path, flags, *mode):
            '''File open. Download file from cloud for read mode.'''
            self.file = os.fdopen(os.open("." + path, flags, *mode),
                                  flag2mode(flags))
            self.fd = self.file.fileno()
            self.path = path
            print "open file ",path
            #Set direct_io and keep_cache options as required by fuse:
            self.direct_io = True
            self.keep_cache = False
            #Construct file metadata:
            filename = path[path.index("/")+1:]
            filesize = os.path.getsize("." + path)
            self.metadata = common.FileMetadata(filename,filesize,setting.totalnode,setting.coding)

            if ("r" in self.file.mode) or ("+" in self.file.mode):
                #Download file from clouds to mirror dir:
                workflow.downloadFile(setting, self.metadata)

        def read(self, length, offset):
            print "test: read file ", self.path
            self.file.seek(offset)
            return self.file.read(length)

        def write(self, buf, offset):
            print "test: write file ", self.path
            self.file.seek(offset)
            self.file.write(buf)
            return len(buf)

        def release(self, flags):
            '''File close. Upload file to cloud for write mode.'''
            global settingpath
            self.file.close()
            setting = common.Setting()
            setting.read(settingpath)#cq update setting before unlink. repair operation may change setting.cfg
            if setting.testmode == False:
                if ("a" in self.file.mode) or ("w" in self.file.mode) or ("+" in self.file.mode):
                    self.metadata.totalnode = setting.totalnode
                    #Encode and upload file to clouds:
                    print "upload file in cloudncfs"
                    workflow.uploadFile(setting, self.metadata)

        def _fflush(self):
            if 'w' in self.file.mode or 'a' in self.file.mode:
                self.file.flush()

        def fsync(self, isfsyncfile):
            self._fflush()
            if isfsyncfile and hasattr(os, 'fdatasync'):
                os.fdatasync(self.fd)
            else:
                os.fsync(self.fd)

        def flush(self):
            global settingpath
            self._fflush()
            os.close(os.dup(self.fd))
            setting = common.Setting()
            setting.read(settingpath)#cq update setting before unlink. repair operation may change setting.cfg
            if setting.testmode == True:
                if ("a" in self.file.mode) or ("w" in self.file.mode) or ("+" in self.file.mode):
                    self.metadata.totalnode = setting.totalnode
                    #Encode and upload file to clouds:
                    workflow.uploadFile(setting, self.metadata)

        def fgetattr(self):
            return os.fstat(self.fd)

        def ftruncate(self, len):
            self.file.truncate(len)

    def main(self, *a, **kw):
        self.file_class = self.CloudNcfsFile
        return Fuse.main(self, *a, **kw)


def main():
    usage = """
Userspace CloudNCFS: mount local directory to multiple Cloud storages with fault tolerance by network coding.
""" + Fuse.fusage

    server = CloudNCFS(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')
    #use dash_s_do='setsingle' to set fuse to single threaded:
    server.parse(values=server, errex=1)
    #disable fuse multithreaded option to avoid racing problem:
    server.multithreaded = False
    server.main()

if __name__ == '__main__':
    main()
