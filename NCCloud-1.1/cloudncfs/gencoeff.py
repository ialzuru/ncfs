'''
Coeff file format for coeffdir/uuid:
[total parity coeff (2B LE)][encoding coeff (1B each)]

Coeff file format for coeffdir/uuid.i (used in repair):
[total parity coeff (2B LE)][new encoding coeff (1B each)]
[no. of chunks used in repair (1B)][chunk #'s to download (1B each)]
[no. of chunks in new node (1B)][[# coeff (1B)][repair coeff for chunks ...]]

usage: python getcoeff.py (setting.cfg) [repaired node #]
'''

import os
import sys

import common

from codings import fmsrtwo
from codings import fmsrutil

def gencoeff(path, parity_coeff, failed_node, setting):
  chunks_vectors = []
  chunks = []

  #Setup parameters before using function from codings/fmsrtwo.py
  n, k = setting.totalnode, setting.datanode
  metadata = common.FileMetadata('', 0, n, 'fmsrtwo')
  nativeBlockNum = fmsrutil.getNativeBlockNum(n, k)
  parityBlockNum = fmsrutil.getParityBlockNum(n, k)
  metadata.totalchunk = parityBlockNum
  for i in xrange(metadata.totalchunk):
    metadata.chunkInfo.append(common.ChunkMetadata(i))
  for i in xrange(metadata.totalnode):
    metadata.fileNodeInfo.append(common.FileNodeMetadata(i))
  setting.healthynode = n - 1
  metadata.totalnode = n
  metadata.parityCoeff = parity_coeff
  for node in setting.nodeInfo:
    node.healthy = True
  setting.nodeInfo[failed_node].healthy = False

  #Get coeff
  ret = fmsrtwo.updateMetadataForDecode(setting, metadata, 'repair')
  if ret == False:
    print >> sys.stderr, "gencoeff failed for node " + str(failed_node)
  new_parity_coeff = [metadata.enc_matrix[i][j] for i in xrange(parityBlockNum) for j in xrange(nativeBlockNum)]
  for i in xrange(metadata.totalchunk):
    if metadata.chunkInfo[i].action == 'download':
      chunks.append(i)
  chunks_vectors = metadata.repairCodingCoeff

  #Write to file
  with open(path, 'wb') as f:
    f.write(chr(len(new_parity_coeff) & 0b11111111))
    f.write(chr((len(new_parity_coeff) >> 8) & 0b11111111))
    for i in new_parity_coeff:
      f.write(chr(i & 0b11111111))
    f.write(chr(len(chunks) & 0b11111111))
    for i in chunks:
      f.write(chr(i & 0b11111111))
    f.write(chr(len(chunks_vectors) & 0b11111111))
    for chunk_vectors in chunks_vectors:
      f.write(chr(len(chunk_vectors) & 0b11111111))
      for i in chunk_vectors:
        f.write(chr(i & 0b11111111))

  setting.nodeInfo[failed_node].healthy = True

def init_from_old(path, totalnode):
  content = ''
  with open(path + '.' + sys.argv[2], 'rb') as f:
    content = f.read()
  content_len = 2 + ord(content[0]) + (ord(content[1]) << 8)
  with open(path, 'wb') as f:
    f.write(content[:content_len])
  for i in xrange(totalnode):
    os.remove(path + '.' + str(i))

def init(path, totalnode, datanode):
  parity_coeff = fmsrutil.getParityCoeff(totalnode, datanode)
  with open(path, 'wb') as f:
    f.write(chr(len(parity_coeff) & 0b11111111))
    f.write(chr((len(parity_coeff) >> 8) & 0b11111111))
    for i in parity_coeff:
      f.write(chr(i & 0b11111111))

def load_coeff(path):
  parity_coeff = []
  with open(path, 'rb') as f:
    for i in f.read()[2:]:
      parity_coeff.append(ord(i))
  return parity_coeff

if __name__ == '__main__':
  '''Usage: python cloudncfs/gencoeff.py (setting.cfg) [repaired node #]'''
  setting = common.Setting()
  setting_path = common.settingpath
  if len(sys.argv) > 1:
    setting_path = sys.argv[1]
  setting.read(setting_path)
  uuid = setting.uuid
  path = setting.coeffdir + '/' + setting.uuid
  totalnode, datanode = setting.totalnode, setting.datanode

  #Initialize base encoding coefficients, save to base coeff file, then load
  if len(sys.argv) > 2:
    init_from_old(path, totalnode)
  elif not os.path.exists(path):
    init(path, totalnode, datanode)
  parity_coeff = load_coeff(path)

  #Generate coeff for each possible node failure and write to coeff files
  for failed_node in xrange(setting.totalnode):
    gencoeff(path + '.' + str(failed_node), parity_coeff, failed_node, setting)

