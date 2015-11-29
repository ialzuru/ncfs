#!/usr/bin/python
#
# @name = 'googledrive.py'
#
# @description = 'Google Drive storage module'
#
# @author = ['Icaro Alzuru']
#

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pydrive.files import ApiRequestError, FileNotUploadedError, FileNotDownloadableError
import common
import storage
import os

############################################################################################
def connectCloud(setting, nodeid):
    '''Establish connection.'''    
    print "... connecting to Google Drive"

    # Read the accesskey and secretkey from the setting.cfg file    
    accesskey = setting.nodeInfo[nodeid].accesskey
    secretkey = setting.nodeInfo[nodeid].secretkey

    # Create a client_secrets.json with the accesskey and secretkey just read
    file = open("/home/ialzuru/NCCloud-1.1/GoogleDrive/client_secrets.json", "w")
    file.write('{"installed":{"client_id":"')
    file.write(accesskey)
    file.write('","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://accounts.google.com/o/oauth2/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_secret":"')
    file.write(secretkey)
    file.write('","redirect_uris":["urn:ietf:wg:oauth:2.0:oob","http://localhost"]}}')
    file.close()

    # Connects to the Google Drive
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile("/home/ialzuru/NCCloud-1.1/GoogleDrive/mycreds.txt")
    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
    gauth.SaveCredentialsFile("/home/ialzuru/NCCloud-1.1/GoogleDrive/mycreds.txt")
    drive = GoogleDrive(gauth)
    if drive is None:
        print "Error (connectCloud): Connection could not be established. Parameters seem to be wrong.\n"
        return False
        
    print " connection successfully established ..."
    return drive

############################################################################################
def checkHealth(setting, nodeid):
    '''Check if successful connection and existing bucket. '''
    # Connects to the Google Drive
    drive = connectCloud(setting, nodeid)
    if drive == False:
        return False

    basedir = setting.nodeInfo[nodeid].bucketname
    #########
    if (basedir == "") or (not basedir):
        file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
        if file_list is None:
            print "Error: Unhealthy connection.\n"
            return False
        else:
            return True 
    else:
        try:
            dir_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
            for d in dir_list:
                # Find the folder
                if (d['mimeType'] == 'application/vnd.google-apps.folder') and (d['title'] == basedir):
                    return True
        except (ApiRequestError, FileNotUploadedError, FileNotDownloadableError) as e:
            print "Error: Folder could not be checked." + e
            return False

    print "Error (checkHealth): bucket (folder) not found."
    return False

############################################################################################
def syncMirror(setting, nodeid, path):
    #print "syncMirror(setting, nodeid, path): ", setting, nodeid, path
    '''Synchronize file entries on cloud to local directory.'''
    # Connects to the Google Drive
    drive = connectCloud(setting, nodeid)
    if drive == False:
        return False
        
    basedir = setting.nodeInfo[nodeid].bucketname
    try:
        #########
        if (basedir == "") or (not basedir): 
            file_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            #find in base folder
            for f in file_list:
                srcFile = drive.CreateFile({'id': f['id']})
                dstName = os.path.join(path, f['title'])
                if setting.coding == 'replication':
                    if dstName.endswith('.node0') or dstName.endswith('.pt'):
                        dstName = os.path.splitext( dstName )[0]
                        if os.path.exists(dstName):
                            os.remove(dstName)
                        srcFile.GetContentFile(dstName)    
                else:
                    if dstName.endswith('.node%d' % nodeid): 
                        dstName = os.path.splitext( dstName )[0]
                        if os.path.exists(dstName):
                            os.remove(dstName)
                        srcFile.GetContentFile(dstName)
        
        else:
            dir_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            for d in dir_list:
                # Find the folder
                if (d['mimeType'] == 'application/vnd.google-apps.folder') and (d['title'] == basedir):
                    file_list = drive.ListFile({'q': "'%s' in parents" % d['id']}).GetList()
                    #find in bucket
                    for f in file_list:
                        srcFile = drive.CreateFile({'id': f['id']})
                        dstName = os.path.join(path, f['title'])
                        if setting.coding == 'replication':
                            if dstName.endswith('.node0') or dstName.endswith('.pt'):
                                dstName = os.path.splitext( dstName )[0]
                                if os.path.exists(dstName):
                                    os.remove(dstName)
                                srcFile.GetContentFile(dstName)    
                        else:
                            if dstName.endswith('.node%d' % nodeid): 
                                dstName = os.path.splitext( dstName )[0]
                                if os.path.exists(dstName):
                                    os.remove(dstName)
                                srcFile.GetContentFile(dstName)
                                   
    except (ApiRequestError, FileNotUploadedError, FileNotDownloadableError) as e:
        print "Error (syncMirror): Folder could not be synchronized." + e
        return False
    
    print "Synchronization completed.\n"    
    return True

############################################################################################
def uploadFile(setting, nodeid, path, dest):  # From path(+file) to Dest (folder)
    #print "uploadFile(setting, nodeid, path, dest): ", setting, nodeid, path, dest
    '''Upload a file to cloud.'''
    # Connects to the Google Drive
    drive = connectCloud(setting, nodeid)
    if drive == False:
        return False

    # Verify is file exists
    if ( not os.path.isfile(path) ):
        print "Error (uploadFile): File to be uploaded does not exist.\n"
        return False

    basedir = setting.nodeInfo[nodeid].bucketname
    try:
        #########
        if (basedir == "") or (not basedir): 
            f = drive.CreateFile({ 'title': dest })
            f.SetContentFile(path)
            f.Upload()
            return True
        else:
            dir_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            for d in dir_list:
                # Find the folder
                if (d['mimeType'] == 'application/vnd.google-apps.folder') and (d['title'] == basedir):
                    # Search if there is a previous version of the same file and delete it first
                    file_list = drive.ListFile({'q': "'%s' in parents" % d['id']}).GetList()
                    for f in file_list:
                        if (f['title'] == dest):
                            f.DeleteFile(f['id'])
                    # After deleting the file (if existed), we creare a new file
                    f = drive.CreateFile({'title': dest, 'parents': [{'kind': 'drive#fileLink','id': d['id'] }]})
                    f.SetContentFile(path)
                    f.Upload()
                    return True
            print "Error (uploadFile): Bucket not found."
            return False;
    except ApiRequestError:
        print "Error (uploadFile): File could not be upload it."
        return False

    return False;

############################################################################################
def uploadMetadata(setting, nodeid, metadatapath, dest): # metadata (srcPath+filename), dest (filename)
    #print "uploadMetadata(setting, nodeid, metadatapath, dest): ",setting, nodeid, metadatapath, dest
    '''Upload metadata to cloud'''
    # Verify the existence of the source metadata file.
    if ( not os.path.isfile(metadatapath) ):
        print "Error (uploadMetadata): Metadata file to upload does not exist.\n"
        return False

    dstName = dest + ".metadata"
    return uploadFile(setting, nodeid, metadatapath, dstName)

############################################################################################
def uploadFileAndMetadata(setting, nodeid, path, metadatapath, dest):
    #print "uploadFileAndMetadata(setting, nodeid, path, metadatapath, dest):",setting,nodeid,metadatapath,dest
    '''Upload file and metadata to cloud'''
    if ( uploadFile(setting, nodeid, path, dest) ):
        return uploadMetadata(setting, nodeid, metadatapath, dest)
    return False

############################################################################################
def downloadFile(setting, nodeid, src, path): # src (remote), path (local path+filename)
    '''Download an object from cloud. '''
    # Connects to the Google Drive
    drive = connectCloud(setting, nodeid)
    if drive == False:
        return False

    basedir = setting.nodeInfo[nodeid].bucketname
    try:
        if (basedir == "") or (not basedir):
            files_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            for f in files_list:
                if f['title'] == src:
                    nf = drive.CreateFile({'id': f['id']})
                    if os.path.exists(path):
                        os.remove(path)
                    nf.GetContentFile(path)
                    return True
            print "Error (downloadFile): file not found."
            return False  
        else:
            dir_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            for d in dir_list:
                # Find the folder
                if (d['mimeType'] == 'application/vnd.google-apps.folder') and (d['title'] == basedir):
                    file_list = drive.ListFile({'q': "'%s' in parents" % d['id']}).GetList()
                    #find in bucket
                    for f in file_list:
                        if f['title'] == src:
                            nf = drive.CreateFile({'id': f['id']})
                            if os.path.exists(path):
                                os.remove(path)
                            nf.GetContentFile(path)
                            return True
                    print "Error (downloadFile): file not found."
                    return False   
            print "Error (downloadFile): bucket not found."
            return False 
    except (ApiRequestError, FileNotUploadedError, FileNotDownloadableError):
        print "Error (downloadFile): File could not be downloaded."
        return False

    return False

############################################################################################
def partialDownloadFile(setting, nodeid, src, path, offset, size):
    '''Partial download an object from cloud.'''

    return True;

############################################################################################
def downloadMetadata(setting, nodeid, src, metadatapath): # src (remote filename), metadatapath (local path+filename)
    '''Download metadata from cloud.'''
    # Verifies that the metadata file has extension .metadata
    filename, file_extension = os.path.splitext(metadatapath)
    if ( file_extension != '.metadata'):
        print "Error (downloadMetadata): Invalid metadata file name.\n"
        return False

    srcName = src + ".metadata"
    return downloadFile(setting, nodeid, srcName, metadatapath)

############################################################################################
def existsFile(setting, nodeid, name):
    '''Check existence of an object on cloud.'''
    '''Return True for exist, False for not exist.'''
    # Connects to the Google Drive
    drive = connectCloud(setting, nodeid)
    if drive == False:
        return False

    basedir = setting.nodeInfo[nodeid].bucketname
    try:
        if (basedir == "") or (not basedir):
            files_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            for f in files_list:
                if f['title'] == name:
                    return True
            return False  
        else:
            dir_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            for d in dir_list:
                # Find the folder
                if (d['mimeType'] == 'application/vnd.google-apps.folder') and (d['title'] == basedir):
                    file_list = drive.ListFile({'q': "'%s' in parents" % d['id']}).GetList()
                    #find in bucket
                    for f in file_list:
                        if f['title'] == name:
                            return True
                    return False   
            print "Error (existsFile): bucket not found."
            return False 
    except ApiRequestError:
        print "Error (existsFile): Google Drive could not be read."
        return False
      
    return False

############################################################################################
def deleteFile(setting, nodeid, name):
    '''Delete an object from cloud.'''
    # Connects to the Google Drive
    drive = connectCloud(setting, nodeid)
    if drive == False:
        return False
    
    basedir = setting.nodeInfo[nodeid].bucketname
    try:
        if (basedir == "") or (not basedir):
            files_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            for f in files_list:
                if (f['title'] == name) or ( f['title'] == (name + '.metadata') ):
                    f.DeleteFile(f['id'])
        else:
            dir_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            for d in dir_list:
                # Find the folder
                if (d['mimeType'] == 'application/vnd.google-apps.folder') and (d['title'] == basedir):
                    file_list = drive.ListFile({'q': "'%s' in parents" % d['id']}).GetList()
                    #find in bucket
                    for f in file_list:
                        if (f['title'] == name) or ( f['title'] == (name + '.metadata') ):
                            f.DeleteFile(f['id'])
    except ApiRequestError:
        print "Error (deleteFile): File could not be deleted."
        return False
    
    return True

############################################################################################
def downloadPointers(setting, nodeid):
    '''download all pointer files from Google Drive to the mirrordir'''
    if setting.coding != 'replication':
        print "Error: downloadPointers is only supported in replication mode."
        return False
    
    # Connects to the Google Drive
    drive = connectCloud(setting, nodeid)
    if drive == False:
        return False

    basedir = setting.nodeInfo[nodeid].bucketname
    try:
        #########
        if (basedir == "") or (not basedir): 
            file_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            #find in base folder
            for f in file_list:
                if f['title'].endswith('.pt'):
                    nf = drive.CreateFile({'id': f['id']})
                    if os.path.exists(path):
                        os.remove(path)
                    nf.GetContentFile(path)
        
        else:
            dir_list = drive.ListFile({'q': "'root' in parents and trashed=False"}).GetList()
            for d in dir_list:
                # Find the folder
                if (d['mimeType'] == 'application/vnd.google-apps.folder') and (d['title'] == basedir):
                    file_list = drive.ListFile({'q': "'%s' in parents" % d['id']}).GetList()
                    #find in bucket
                    for f in file_list:
                        if f['title'].endswith('.pt'):
                            nf = drive.CreateFile({'id': f['id']})
                            if os.path.exists(path):
                                os.remove(path)
                            nf.GetContentFile(path) 

    except (ApiRequestError, FileNotUploadedError, FileNotDownloadableError):
        print "Error (downloadPointers): Pointer file could not be downloaded."
        return False
    
    return True

############################################################################################
def detectFile(setting, filename, nodeid):
    '''detect the existence of a given file named filename on a given node with ID nodeid'''
    if setting.coding != 'replication':
        print "Error (detectFile): detectFile is supported only in replication mode"
        return False
    
    return existsFile(setting, nodeid, filename)

############################################################################################
def deletePointer(setting, nodeid, name):
    '''Delete a pointer object from Google Drive.'''
    if setting.coding != 'replication':
        print "Error (deletePointer): detectFile is supported only in replication mode"
        return False
    
    ptFilename = name + '.pt'
    return deleteFile(setting, nodeid, ptFilename)

############################################################################################
def downloadPointer(setting, nodeid, filename):
    '''download the pointer related to filename in a given cloud specified by nodeid'''
    if setting.coding != 'replication':
       print "Error (downloadPointer): downloadPointer is supported only in replication mode"
       return False

    pointerdir = setting.pointerdir
    destPath = os.path.join(pointerdir, filename + '.pt')
    ptFilename = filename + '.pt'
    return downloadFile(setting, nodeid, ptFilename, destPath)
    
############################################################################################