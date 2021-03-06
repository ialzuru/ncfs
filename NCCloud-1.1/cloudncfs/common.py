#!/usr/bin/python
#
# @name = 'common.py'
# 
# @description = "Common classes for settings and metadata."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#
 

import ConfigParser

settingpath = "setting.cfg"

class NodeData:
    '''Class of Data of a storage node.'''
    def __init__(self,nodeid,nodekey,nodetype,nodeloc,accesskey,secretkey,bucketname):
        self.nodeid = nodeid
        self.nodekey = nodekey
        self.nodetype = nodetype
        self.nodeloc = nodeloc
        self.accesskey = accesskey
        self.secretkey = secretkey
        self.bucketname = bucketname
        self.healthy = True


class Setting:
    '''Class of File system setting.'''

    def __init__(self):
        self.totalnode = int(0)
        self.datanode = int(0)
        self.healthynode = int(0)
        self.storagethreads = int(0)
        self.rebuildthreads = int(0)
        self.autorepair = bool(False)
        self.writerlock = bool(False)
        self.zookeeper = bool(False)    #Using Zookeeper for locking?
        self.zookeeperloc = ''  #,-delimited list of server ip:port
        self.zookeeperroot = ''  #Root node on ZK, allows multi-set of CloudNCFS
        self.smartthreads = bool(False)  #Use new storage threading algorithm?
        self.smartthreadslim = 0  #Lower limit of subdivided chunk size
        self.smartthreadsmethod = 1  #New storage threading algorithm to use
        self.testmode = bool(False)
        self.coding = ''
        self.mirrordir = ''
        self.chunkdir = ''
        self.metadatadir = ''
        self.coeffdir = ''
        self.uuid = ''
        self.nodeInfo = []   #list of NodeData
        self.totalsparenode = int(0)   #number of spare nodes
        self.spareNodeInfo = []   #list of spare node info
        self.deduplication = bool(False)  #cq enable deduplication function
        self.pointerdir = ''#cq initialize pointerdir path


    def read(self, path):
        #Read configuration file.
        config = ConfigParser.ConfigParser()
        config.read(path)
        self.totalnode = int(config.get("global","totalnode"))
        self.datanode = int(config.get("global","datanode"))
        self.healthynode = int(config.get("global","totalnode"))
        self.coding = config.get("global","coding")
        self.mirrordir = config.get("global","mirrordir")
        self.chunkdir = config.get("global","chunkdir")
        self.metadatadir = config.get("global","metadatadir")
        self.coeffdir = config.get("global","coeffdir")
        self.uuid = config.get("global","uuid")
        self.storagethreads = int(config.get("global","storagethreads"))
        self.smartthreads = config.getboolean("global","smartthreads")
        self.smartthreadslim = int(config.get("global","smartthreadslim"))
        self.smartthreadsmethod = int(config.get("global","smartthreadsmethod"))
        self.rebuildthreads = int(config.get("global","rebuildthreads"))
        self.autorepair = config.getboolean("global","autorepair")
        self.writerlock = config.getboolean("global","writerlock")
        self.zookeeper = config.getboolean("global","zookeeper")
        self.zookeeperloc = config.get("global","zookeeperloc")
        self.zookeeperroot = config.get("global","zookeeperroot")
        self.testmode = config.getboolean("global","testmode")
        self.deduplication = config.getboolean("global","deduplication") #cq config deduplication mode
        self.pointerdir = config.get("global","pointerdir") #cq config pointerdir path
        
        for i in range(int(self.totalnode)):
            #Now use config file section node number as node id.
            nodeid = i
            nodekey = config.get("node"+str(i),"nodekey")
            nodetype = config.get("node"+str(i),"nodetype")
            nodeloc = config.get("node"+str(i),"nodeloc")
            accesskey = config.get("node"+str(i),"accesskey")
            secretkey = config.get("node"+str(i),"secretkey")
            bucketname = config.get("node"+str(i),"bucketname")
            node = NodeData(nodeid,nodekey,nodetype,nodeloc,accesskey,secretkey,bucketname)
            self.nodeInfo.append(node)
        try:
            self.totalsparenode = int(config.get("global","totalsparenode"))
            for i in range(int(self.totalsparenode)):
                nodeid = i
                nodekey = config.get("sparenode"+str(i),"nodekey")
                nodetype = config.get("sparenode"+str(i),"nodetype")
                nodeloc = config.get("sparenode"+str(i),"nodeloc")
                accesskey = config.get("sparenode"+str(i),"accesskey")
                secretkey = config.get("sparenode"+str(i),"secretkey")
                bucketname = config.get("sparenode"+str(i),"bucketname")
                node = NodeData(nodeid,nodekey,nodetype,nodeloc,accesskey,secretkey,bucketname)
                self.spareNodeInfo.append(node)
        except:
            print "No spare node info."


    def write(self, path):
        #Write configuration file.
        config = ConfigParser.ConfigParser()
        config.add_section("global")
        config.set("global","totalnode",self.totalnode)
        config.set("global","datanode",self.datanode)
        config.set("global","coding",self.coding)
        config.set("global","mirrordir",self.mirrordir)
        config.set("global","chunkdir",self.chunkdir)
        config.set("global","metadatadir",self.metadatadir)
        config.set("global","coeffdir",self.coeffdir)
        config.set("global","uuid",self.uuid)
        config.set("global","storagethreads",self.storagethreads)
        config.set("global","smartthreads",self.smartthreads)
        config.set("global","smartthreadslim",self.smartthreadslim)
        config.set("global","smartthreadsmethod",self.smartthreadsmethod)
        config.set("global","rebuildthreads",self.rebuildthreads)
        config.set("global","autorepair",self.autorepair)
        config.set("global","writerlock",self.writerlock)
        config.set("global","zookeeper",self.zookeeper)
        config.set("global","zookeeperloc",self.zookeeperloc)
        config.set("global","zookeeperroot",self.zookeeperroot)
        config.set("global","testmode",self.testmode)
        config.set("global","deduplication",self.deduplication) #cq set deduplication
        config.set("global","pointerdir",self.pointerdir) #cq set pointerdir
        for i in range(int(self.totalnode)):
            nodeSection = "node" + str(i)
            config.add_section(nodeSection)
            config.set(nodeSection, "nodeid", i)
            config.set(nodeSection, "nodekey", self.nodeInfo[i].nodekey)
            config.set(nodeSection, "nodetype", self.nodeInfo[i].nodetype)
            config.set(nodeSection, "nodeloc", self.nodeInfo[i].nodeloc)
            config.set(nodeSection, "accesskey", self.nodeInfo[i].accesskey)
            config.set(nodeSection, "secretkey", self.nodeInfo[i].secretkey)
            config.set(nodeSection, "bucketname", self.nodeInfo[i].bucketname)
        try:
            config.set("global","totalsparenode",self.totalsparenode)
            for i in range(int(self.totalsparenode)):
                spareSection = "sparenode" + str(i)
                config.add_section(spareSection)
                config.set(spareSection, "nodeid", i)
                config.set(spareSection, "nodekey", self.spareNodeInfo[i].nodekey)
                config.set(spareSection, "nodetype", self.spareNodeInfo[i].nodetype)
                config.set(spareSection, "nodeloc", self.spareNodeInfo[i].nodeloc)
                config.set(spareSection, "accesskey", self.spareNodeInfo[i].accesskey)
                config.set(spareSection, "secretkey", self.spareNodeInfo[i].secretkey)
                config.set(spareSection, "bucketname", self.spareNodeInfo[i].bucketname)                
        except:
            print "No spare node info"
        #Write config to file.
        configfile = open(path,"w")
        config.write(configfile)
        configfile.close()


class ChunkMetadata:
    '''Class of Metadata of a file chunk.'''

    def __init__(self, chunkid):
        self.chunkid = chunkid
        self.chunkname = ''
        self.chunksize = int(0)
        self.chunktype = ''
        self.chunkpath = ''
        self.nodeid = int(0)
        self.nodekey = ''
        self.nodetype = ''
        self.bucketname = ''
        self.healthy = True
        self.action = ''
        self.position = int(0)    #Added for big-chunk

class FileNodeMetadata:
    '''Class of Metadata of node info for a file.'''

    def __init__(self, nodeid):
        self.nodeid = nodeid
        self.nodekey = ''
        self.nodetype = ''
        self.bucketname = ''
        self.bigchunkname = ''
        self.bigchunksize = int(0)
        self.bigchunkpath = ''
        self.healthy = True
        self.action = ''
        self.chunknum = int(0)

class FileMetadata:
    '''Class of Metadata of a file.'''

    def __init__(self, filename, filesize, totalnode, coding):
        self.filename = filename
        self.filesize = filesize
        self.filekey = ''
        self.totalnode = totalnode
        self.datanode = int(0)
        self.healthynode = totalnode
        self.coding = coding
        self.totalchunk = int(0)
        self.chunkInfo = []   #list of ChunkMetadata
        self.fileNodeInfo = []    #list of node info for big-chunk
        self.parityCoeff = []   #parity coefficients for fmsr
        self.uuid = ''

    def codingToByte(self, coding):
        #Convert a coding type to a character.
        character = chr(0)
        if coding == 'replication':
            character = 'a'
        elif coding == 'striping':
            character = 'b'
        elif coding == 'parityrs':
            character = 'c'
        elif coding == 'embr':
            character = 'd'
        elif coding == 'fmsrtwo':
            character = 'e'
        return character
    
    def codingFromByte(self, character):
        #Convert a character to a coding type.
        coding = ''
        if character == 'a':
            coding = "replication"
        elif character == 'b':
            coding = "striping"
        elif character == 'c':
            coding = "parityrs"
        elif character == 'd':
            coding = "embr"
        elif character == 'e':
            coding = "fmsrtwo"
        return coding
    
    def chunkToByte(self, type):
        #Convert a chunk type to a character.
        character = 'b'
        if type == "native":
            character = 'a'
        elif type == "parity":
            character = 'b'
        elif type == "replica":
            character = 'c'
        return character
    
    def chunkFromByte(self, character):
        #Convert a character to a chunk type.
        type = ''
        if character == 'a':
            type = 'native'
        elif character == 'b':
            type = 'parity'
        elif character == 'c':
            type = 'replica'
        return type
    
    def read(self, path, setting):
        #Read file metadata from binary metadata file in path.

        infile = open(path, 'rb')
        mstring = infile.read()
        infile.close()

        #Read metadata:
        self.totalnode = setting.totalnode
        self.datanode = setting.datanode
        #Read coding type from Byte[0]
        self.coding = self.codingFromByte(mstring[0])
        #Read totalchunk from Byte[2-3]:
        self.totalchunk = ord(mstring[2]) + (ord(mstring[3]) << 8)
        #Read totalparitycoeff from Byte[4-5]:
        totalparitycoeff = ord(mstring[4]) + (ord(mstring[5]) << 8)
        #Read file size from Byte[6-11]:
        self.filesize = 0
        for i in range(0,6):
            self.filesize += ord(mstring[6+i]) << (8*i)        

        #Read chunk info:
        for i in range(self.totalchunk):
            chunk = ChunkMetadata(i)
            cstring = mstring[(12+10*i):(12+10*(i+1))]
            #Read block id from ChunkByte[0]:
            blockid = ord(cstring[0])
            #Read chunk type from ChunkByte[1]:
            chunk.chunktype = self.chunkFromByte(cstring[1])
            if self.coding == 'embr' or self.coding == 'fmsrtwo':
                chunk.chunkpath = self.filename + '.chunk' + str(blockid)
            elif self.coding == 'replication':    #cq
                chunk.chunkpath = self.filename + '.node0'
            else:
                chunk.chunkpath = self.filename + '.node' + str(blockid)
            #Read node id from ChunkByte[2]:
            chunk.nodeid = ord(cstring[2])
            chunk.nodetype = setting.nodeInfo[chunk.nodeid].nodetype
            chunk.bucketname = setting.nodeInfo[chunk.nodeid].bucketname
            #Read position from ChunkByte[3]:
            chunk.position = ord(cstring[3])
            #Read chunk size from ChunkByte[4-9]:
            chunk.chunksize = 0
            for j in range(0,6):
                chunk.chunksize += ord(cstring[4+j]) << (8*j)
            self.chunkInfo.append(chunk)

        #Read node info:
        for i in range(self.totalnode):
            fileNode = FileNodeMetadata(i)
            nstring = mstring[(12+10*self.totalchunk+8*i):(12+10*self.totalchunk+8*(i+1))]
            #Read node id from NodeByte[0]:
            fileNode.nodeid = ord(nstring[0])
            fileNode.nodekey = setting.nodeInfo[fileNode.nodeid].nodekey
            fileNode.nodetype = setting.nodeInfo[fileNode.nodeid].nodetype
            fileNode.bucketname = setting.nodeInfo[fileNode.nodeid].bucketname
            #Read chunk number from NodeByte[1]:
            fileNode.chunknum = ord(nstring[1])
            #Read big-chunk size from NodeByte[2-7]:
            fileNode.bigchunksize = 0
            for j in range(0,6):
                if j < len(nstring) - 2:
                    fileNode.bigchunksize += ord(nstring[2+j]) << (8*j)
            if setting.coding == 'replication':
                fileNode.bigchunkpath = self.filename + '.node0'
            else:
                fileNode.bigchunkpath = self.filename + '.node' + str(fileNode.nodeid)
            self.fileNodeInfo.append(fileNode)

        #Read coefficients for fmsrtwo and settings.cfg UUID:
        if 'fmsr' in self.coding:
            coeffstart = (12+10*self.totalchunk+8*self.totalnode)
            coeffend = coeffstart + totalparitycoeff
            pstring = mstring[coeffstart:coeffend]
            for i in range(totalparitycoeff):
                #Read coefficients:
                coeff = ord(pstring[i])
                self.parityCoeff.append(coeff)
            self.uuid = mstring[coeffend:]

    def write(self, path):
        #Write file metadata to a binary metadata file in path.

        #Build the binary metadata string:
        #Generate basic info:
        mstring = ''
        #Byte[0-1] for coding type:
        codingbyte = self.codingToByte(self.coding)
        mstring += codingbyte
        mstring += codingbyte
        #Byte[2-3] for totalchunks:
        mstring += chr(self.totalchunk & 0b11111111)
        mstring += chr((self.totalchunk >> 8) & 0b11111111)
        #Byte[4-5] for totalparitycoeff:
        totalparitycoeff = len(self.parityCoeff)
        mstring += chr(totalparitycoeff & 0b11111111)
        mstring += chr((totalparitycoeff >> 8) & 0b11111111)
        #Byte[6-11] for file size:
        tempsize = self.filesize
        for i in range(0,6):
            mstring += chr(tempsize & 0b11111111)
            tempsize = tempsize >> 8
            
        #Generate chunk info:
        for chunk in self.chunkInfo:
            #ChunkByte[0] for chunkid:
            #mstring += chr(chunk.chunkid & 0b11111111)
            chunkpath = chunk.chunkpath
            if ".chunk" in chunkpath:
                blockid = int(chunkpath[chunkpath.rindex('.chunk')+6:])
            else:
                blockid = chunk.chunkid
            mstring += chr(blockid & 0b11111111)
            #ChunkByte[1] for chunktype:
            mstring += self.chunkToByte(chunk.chunktype)
            #ChunkByte[2] for nodeid:
            mstring += chr(chunk.nodeid & 0b11111111)
            #ChunkByte[3] for position:
            mstring += chr(chunk.position & 0b11111111)
            #ChunkByte[4-9] for chunk size:
            tempsize = chunk.chunksize
            for i in range(0,6):
                mstring += chr(tempsize & 0b11111111)
                tempsize = tempsize >> 8
            
        #Generate file node info:
        for fileNode in self.fileNodeInfo:
            #NodeByte[0] for nodeid:
            mstring += chr(fileNode.nodeid & 0b11111111)
            #NodeByte[1] for chunknum:
            mstring += chr(fileNode.chunknum & 0b11111111)
            #NodeByte[2-7] for big-chunk size:
            tempsize = fileNode.bigchunksize
            for i in range(0,6):
                mstring += chr(tempsize & 0b11111111)
                tempsize = tempsize >> 8
            
        #Generate parity coefficient info:
        for i in range(totalparitycoeff):
            mstring += chr(self.parityCoeff[i] & 0b11111111)
        if (totalparitycoeff): #fmsr
            mstring += self.uuid
        #Open file:
        outfile = open(path, 'wb')
        #Write metadata:
        outfile.write(mstring)
        #Close file:
        outfile.close()

