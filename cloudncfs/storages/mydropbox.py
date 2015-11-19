#!/usr/bin/python
#
# @name = 'dropbox.py'
#
# @description = 'dropbox storage module'
#
# @author = ['CAI Qi']
#
# limitation currently this storage interface only supports one layer. It means the bucket should be# created directly under the root directory. In addition, chunks and metadata should be placed 
# directly under the bucket directory /bucketname/ChunkorMeta

import dropbox 
import common
import storageutil
import os

def connectCloud(setting, nodeid):
    '''Establish connection.'''
    #print "try to establish connection"
    #dropbox connection: secretkey is the dropbox access token
    
    access_token = setting.nodeInfo[nodeid].secretkey
    #print access_token
    #print len(access_token)
    client =  dropbox.client.DropboxClient(access_token)
    if client == False:
        print "Fail to connect dropbox"
        return False
    else:
        #print "connect successfully"
        return client
"""
    except:
        print "exception appears when connecting to dropbox"
        return False
"""

def checkHealth(setting, nodeid):
    '''Check if successful connection and existing bucket. '''
    conn = connectCloud(setting, nodeid)
    if conn == False:
        return False
    else:
        bucketname = setting.nodeInfo[nodeid].bucketname
        try:
            split_path = bucketname.split('/')
            parent_split_path = split_path[0:-1]
            folder_name = split_path[-1]
            if folder_name == '':
                print "Error: please input a bucketname under the dropbox app directory"
                return False
            parent_path = '/'.join(parent_split_path)
            if parent_path == '':
                parent_path = '/'
            results = conn.search(parent_path,folder_name)
            for result in results: 
                if result['path'] == bucketname:
                    #print "successfully connect to ", bucketname
                    return True
            #print "Fail to connect to ", bucketname
            return False
        except:
            return False
       
def syncMirror(setting, nodeid, path):
    '''Synchronize file entries on cloud to local directory.'''
    #Dropbox connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Sync cloud to mirror dir:
    folder_metadata = conn.metadata(bucketname)
    folder_contents = folder_metadata["contents"]
    for content in folder_contents:
        #Touch files in local directory:
        filename = os.path.relpath(content["path"],bucketname)
        #only sync certain file object
        if setting.coding == 'replication':
            if filename.endswith('.node0') or filename.endswith('.pt'):
                storageutil.syncFile(setting, filename, path)                
        else:
            if filename.endswith('.node%d' % nodeid):
                storageutil.syncFile(setting, filename, path)
    return True
    

def uploadFile(setting, nodeid, path, dest):
    '''Upload a file to cloud.'''
    #Dropbox connection
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Upload file to bucket
    try:
        absdest = os.path.join(bucketname,dest)
        results = conn.search(bucketname,dest)
        for result in results:
            if result['path'] == absdest:
                conn.file_delete(absdest)
                break

        f = open(path,'rb')
        response = conn.put_file(absdest,f)
        f.close()
        #print "upload file", response
        return True
    except IOError:
        print >> sys.stderr, "Error: The destination location is not writable."
        return False

def uploadMetadata(setting, nodeid, metadatapath, dest):
    '''Upload metadata to cloud'''
    #Dropbox connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Upload metadata to bucket:
    absdest = os.path.join(bucketname, dest+'.metadata')
    try:
        results = conn.search(bucketname,dest+'.metadata')
        for result in results:
            if result['path'] == absdest:
                conn.file_delete(absdest)
                break
        f = open(metadatapath,'rb')
        response = conn.put_file(absdest,f)
        f.close()
        #print "upload metadata", response
        return True
    except IOError:
        print >> sys.stderr, "Error: The destination location is not writable."   
        return False

def uploadFileAndMetadata(setting, nodeid, path, metadatapath, dest):
    '''Upload file and metadata to cloud'''
    #Dropbox connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Upload file and metadata to bucket:
    try:
        results = conn.search(bucketname, dest)        
        absdest_file = os.path.join(bucketname,dest)
        if len(results) != 0:
            for result in results:
                if result['path'] == absdest_file:
                    conn.file_delete(absdest_file)
                    break
        file = open(path,'rb')
        response = conn.put_file(absdest_file,file)
        file.close()
        #print "upload file", response
        
        results = conn.search(bucketname, dest+'.metadata')            
        absdest_meta = os.path.join(bucketname,dest+'.metadata')
        if len(results) != 0:
            for result in results:
                if result['path'] == absdest_meta:
                   conn.file_delete(absdest_meta)
                   break
        meta = open(metadatapath,'rb')
        response = conn.put_file(absdest_meta, meta)
        meta.close()
        #print "upload metadata", response
        return True
    except IOError:
        print >> sys.stderr, "Error: The destination location is not writable."
        return False

def downloadFile(setting, nodeid, src, path):
    '''Download an object from cloud. '''
    #Dropbox connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Download object from bucket:
    abssrc = os.path.join(bucketname,src)
    out_file = open(path,'wb')
    with conn.get_file(abssrc) as f:
        out_file.write(f.read())
    return True

def partialDownloadFile(setting, nodeid, src, path, offset, size):
    '''Partial download an object from cloud.'''
    #Dropbox connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Partial download object from bucket:
    abssrc = os.path.join(bucketname,src)
    out_file = open(path,'wb')
    with conn.get_file(abssrc,start=offset,length=size) as f:
        out_file.write(f.read())
    return True

def downloadMetadata(setting, nodeid, src, metadatapath):
    '''Download metadata from cloud.'''
    #Drobox connection:
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Download metadata from bucket:
    abssrc = os.path.join(bucketname,src+'.metadata')
    out_file = open(metadatapath,'wb')
    with conn.get_file(abssrc) as f:
        out_file.write(f.read())
    return True

def existsFile(setting, nodeid, name):
    '''Check existence of an object on cloud.'''
    '''Return True for exist, False for not exist.'''
    #Dropbox connection:
    conn = connectCloud(setting,nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Check existence of an object in bucket:
    absdest = os.path.join(bucketname,name)
    folder_metadata = conn.metadata(bucketname)
    folder_contents = folder_metadata["contents"]
    for content in folder_contents:
        if content["path"] == absdest:
            return True
    return False
            
def deleteFile(setting, nodeid, name):
    '''Delete an object from cloud.'''
    #Dropbox connection
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Delete an object from bucket:
    absdest = os.path.join(bucketname,name)
    conn.file_delete(absdest)
    #Delete the associated metadata
    if setting.coding != 'replication':
        conn.file_delete(absdest.rsplit('.',1)[0]+'.metadata')
    else:
        conn.file_delete(absdest + '.metadata')
    #conn.file_delete(absdest+'.metadata')
    return True

def downloadPointers(setting, nodeid):
    '''download all pointer files in a given cloud'''
    conn = connectCloud(setting, nodeid)
    if conn == False:
        print "Fail to connect to dropbox in downloadPointers"
        return False
    else:
        if setting.coding != 'replication':
            print "don't support download pointers for coding modes other than replication"
            return False
        pointerdir = setting.pointerdir
        bucketname = setting.nodeInfo[nodeid].bucketname
        folder_metadata = conn.metadata(bucketname)
        folder_contents = folder_metadata["contents"]

        for content in folder_contents:
            filename = os.path.relpath(content["path"],bucketname)
            if filename.endswith('.pt'):
                destFilePath = os.path.join(pointerdir,filename)
                f, metadata = conn.get_file_and_metadata(content["path"])
                out = open(destFilePath,'wb')
                out.write(f.read())
                out.close()
        return True

def detectFile(setting, filename, nodeid):
    '''detect the existence of a given file named filename on a given node with ID nodeid'''
    conn = connectCloud(setting, nodeid)
    if conn == False:
        return False
    else:
        parent_path = setting.nodeInfo[nodeid].bucketname
        results = conn.search(parent_path, filename)
        file_path = os.path.join(parent_path,filename)
        for result in results:     
            if result['path'] == file_path:
                return True
        return False

def deletePointer(setting, nodeid, name):
    '''Delete an object from cloud.'''
    #Dropbox connection
    conn = connectCloud(setting, nodeid)
    bucketname = setting.nodeInfo[nodeid].bucketname
    #Delete an object from bucket:
    absdest = os.path.join(bucketname,name+'.pt')
    conn.file_delete(absdest)
    return True

def downloadPointer (setting, nodeid, filename):
    '''download the pointer related to filename in a given cloud specified by nodeid'''
    conn = connectCloud(setting, nodeid)
    if conn == False:
        print "Fail to connect to dropbox in downloadPointer"
        return False
    else:
        if setting.coding != 'replication':
            print "Error in downloadPointer. Only support replication coding mode"
            return False
        else:
            pointerdir = setting.pointerdir
            bucketname = setting.nodeInfo[nodeid].bucketname
            destPath = os.path.join(pointerdir,filename + '.pt')
            srcPath = os.path.join(bucketname,filename + '.pt')
            f, metadata = conn.get_file_and_metadata(srcPath)
            out = open(destPath,'wb')
            out.write(f.read())
            out.close()
        return True
                    
