import sys
import threading
import zookeeper

from datetime import datetime

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"};

class ZooKeeperLock:
    '''Class of shared lock with ZooKeeper

    Assumes one single lock acquisition request at any time
    '''

    def __init__(self, setting):
        #Connect to ZK servers
        print "Connecting to ZooKeeper ... "
        self.connected = False
        self.log = open("zookeeper.log", 'a')
        self.log.write("\n\n=============\nZOOKEEPER LOG\n=============\n")
        self.log.write(datetime.now().__str__())
        zookeeper.set_log_stream(self.log)
        self.cv = threading.Condition()
        self.cv2 = threading.Condition()
        def watcher (handle, type, state, path):
            self.cv.acquire()
            if state == zookeeper.CONNECTED_STATE:
                print "Connected!"
                self.connected = True
            else:
                print "Disconnected from ZooKeeper: ",
                print zookeeper.zerror(state)
            self.cv.notify()
            self.cv.release()
        
        self.cv.acquire()
        self.zh = zookeeper.init(setting.zookeeperloc, watcher, 10000)
        self.cv.wait(10.0)
        if not self.connected:
            print "Cannot connect to ZooKeeper. ",
            print "Check that server(s) are on " + setting.zookeeperloc
            sys.exit()
        self.cv.release()

        self.root = setting.zookeeperroot
        self.createRootNode(self.root)
        self.zpath = '/'


    def createRootNode(self, root):
        #Create root node recursively from back to front to back
        if root == '/': return
        while True:
            try:
                root = root[:-1]
                zookeeper.create(self.zh, root, '', [ZOO_OPEN_ACL_UNSAFE], 0)
                return
            except zookeeper.NodeExistsException:
                return
            except zookeeper.NoNodeException:
                self.createRootNode(root.rsplit('/', 1)[0] + '/')
                

    def createLockNode(self, filename, lock):
        #Create a lock node ('r'/'w') on ZooKeeper and return its no.
        while True:
            try:
                zpath = zookeeper.create(self.zh, self.root+filename+'/lock', lock,
                    [ZOO_OPEN_ACL_UNSAFE], zookeeper.SEQUENCE | zookeeper.EPHEMERAL)
                return zpath
            except zookeeper.NoNodeException:
                self.createRootNode(self.root + filename + '/')

    
    def deleteNode(self, zpath):
        #Delete a node recursively until root node
        while zpath != self.root[:-1]:
            try:
                zookeeper.delete(self.zh, zpath)
            except zookeeper.NoNodeException:
                pass
            except zookeeper.NotEmptyException:
                break
            finally:
                zpath = zpath[:-1].rsplit('/', 1)[0]


    def getReadLock(self, metadata):
        #Acquire read lock on file through ZooKeeper
        fn = metadata.filename
        print "Trying to acquire read lock for " + self.root + fn
        self.zpath = self.createLockNode(fn, 'r')
        myLock = self.zpath.rsplit('/',1)[1]
        while True:
            children = sorted(zookeeper.get_children(self.zh, self.root + fn))
            lastLock = ''
            for child in children:
                try:
                    if child == myLock:
                        break
                    elif zookeeper.get(self.zh, self.root+fn+'/'+child)[0] == 'w':
                        lastLock = child
                except:
                    pass
            if lastLock != '':
                def watcher (handle, type, state, path):
                    self.cv2.acquire()
                    self.cv2.notify()
                    self.cv2.release()

                self.cv2.acquire()
                if zookeeper.exists(self.zh, self.root+fn+'/'+lastLock, watcher):
                    self.cv2.wait()
                self.cv2.release()
            else:
                break
        print "Acquired read lock for " + self.root + fn


    def releaseReadLock(self, metadata):
        #Release read lock on file through ZooKeeper
        #CloudNCFS currently works on a single file at any time
        self.deleteNode(self.zpath)
        print "Released read lock for " + self.zpath

    def getWriteLock(self, metadata):
        #Acquire write lock on file through ZooKeeper
        fn = metadata.filename
        print "Trying to acquire write lock for " + self.root + fn
        self.zpath = self.createLockNode(fn, 'w')
        myLock = self.zpath.rsplit('/',1)[1]
        while True:
            children = sorted(zookeeper.get_children(self.zh, self.root + fn))
            lastLock = ''
            for child in children:
                if child == myLock: break
                else: lastLock = child
            if lastLock != '':
                def watcher (handle, type, state, path):
                    self.cv2.acquire()
                    self.cv2.notify()
                    self.cv2.release()

                self.cv2.acquire()
                if zookeeper.exists(self.zh, self.root+fn+'/'+lastLock, watcher):
                    self.cv2.wait()
                self.cv2.release()
            else:
                break
        print "Acquired write lock for " + self.root + fn

    def releaseWriteLock(self, metadata):
        #Release write lock on file through ZooKeeper
        #CloudNCFS currently works on a single file at any time
        self.deleteNode(self.zpath)
        print "Released write lock for " + self.zpath

    def updateSetting(self, setting):
        #Update settings after node repair
        #Not used yet
        return
