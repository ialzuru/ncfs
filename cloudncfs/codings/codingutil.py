#!/usr/bin/python
#
# @name = 'codingutil.py'
# 
# @description = "Coding utilities module."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import os
import zfec

def split(src, dest):
    '''Split a file into a list of chunks.'''
    infile = open(src, 'rb')
    data = infile.read()
    infile.close()
    totalchunk = len(dest)
    filesize = len(data)
    if filesize == 0:
        for i in range(totalchunk):
            outfile = open(dest[i], 'wb')
            outfile.close()
    else:
        chunksize = filesize/totalchunk + 1
        #Split file:
        chunkid = 0
        for i in range(0, filesize+1, chunksize):
            outfile = open(dest[chunkid], 'wb')
            startaddr = i
            endaddr = i + chunksize
            outfile.write(data[startaddr:endaddr])
            if endaddr > filesize:
                paddingsize = endaddr - (filesize)
                for j in range(0, paddingsize):
                    outfile.write('\0')
            outfile.close()
            chunkid += 1


def join(src, dest, filesize):
    '''Join a list of chunks into a file.'''
    datalist = []
    for filename in src:
        infile = open(filename, 'rb')
        datalist.append(infile.read())
        infile.close()
    outfile = open(dest,'wb')
    writesize = 0
    for data in datalist:
        writesize += len(data)
        if writesize > filesize:
            chunksize = len(data) - (writesize - filesize)
            outfile.write(data[0:chunksize])
        else:
            outfile.write(data)
    outfile.close()


def ecEncode(src, dataChunks, parityChunks, setting):
    '''Encode a file into data and parity chunks by erasure coding.'''
    '''Use zfec library.'''
    k = len(dataChunks)
    n = k + len(parityChunks)

    #Split src file into data chunks:
    infile = open(src, 'rb')
    data = infile.read()
    infile.close()
    totalchunk = len(dataChunks)
    filesize = len(data)
    if filesize == 0:
        for i in range(totalchunk):
            outfile = open(dataChunks[i], 'wb')
            outfile.close()
    else:
        chunksize = filesize/totalchunk + 1
        setting.chunksize = chunksize
        chunkid = 0
        for i in range(0, filesize+1, chunksize):
            outfile = open(dataChunks[chunkid], 'wb')
            startaddr = i
            endaddr = i + chunksize
            outfile.write(data[startaddr:endaddr])
            if endaddr > filesize:
                paddingsize = endaddr - (filesize)
                for j in range(0, paddingsize):
                    outfile.write('\0')
            outfile.close()
            chunkid += 1

    #Encode to generate parity chunks:
    encoder = zfec.Encoder(k, n)
    indatalist = []
    for filename in dataChunks:
        infile = open(filename, 'rb')
        indatalist.append(infile.read())
        infile.close()
    outdatalist = encoder.encode(indatalist)
    for i in range(n-k):
        outfile = open(parityChunks[i],'wb')
        outfile.write(outdatalist[k+i])
        outfile.close()

def ecDecodeFile(n, k, src, blocknums, dest, filesize, setting):
    '''Decode a list of chunks to original file by erasure coding.'''
    '''Use zfec library.'''
    decoder = zfec.Decoder(k, n)
    indatalist = []
    for filename in src:
        infile = open(filename, 'rb')
        indatalist.append(infile.read())
        infile.close()
    #Get native data chunks:
    outdatalist = decoder.decode(indatalist, blocknums)
    outfile = open(dest,'wb')
    writesize = 0
    #Join chunks into file:
    for data in outdatalist:
        writesize += len(data)
        if writesize > filesize:
            chunksize = len(data) - (writesize - filesize)
            outfile.write(data[0:chunksize])
        else:
            outfile.write(data)
    outfile.close()


def ecDecodeChunks(n, k, src, blocknums, dest, setting, failed=[], nativeFailed=True):
    '''Generate a full list of chunks.'''
    '''Use zfec library.'''
    if nativeFailed:
        decoder = zfec.Decoder(k, n)
        indatalist = []
        for filename in src:
            infile = open(filename, 'rb')
            indatalist.append(infile.read())
            infile.close()
        #Get native data chunks:
        outdatalist = decoder.decode(indatalist, blocknums)
    else:
        outdatalist = []
        for filename in src:
            infile = open(filename, 'rb')
            outdatalist.append(infile.read())
            infile.close()

        #Generate full list of chunks:
        encoder = zfec.Encoder(k, n)
        outdatalist = encoder.encode(outdatalist)

    for i in range(len(outdatalist)):
        if i in failed:
            filename = dest[i]
            outfile = open(filename, 'wb')
            outfile.write(outdatalist[i])
            outfile.close()

