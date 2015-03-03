import hashlib
import os
import zlib
import time
import sys
from ddm.DropboxSEPlugin import DropboxSEPlugin
from ddm.GridSEPlugin import GridSEPlugin
from ddm.LocalSEPlugin import LocalSEPlugin

BIN_HOME='/srv/lsm/rrcki-sendjob'
SITE_PREFIX = 'srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN='
SITE_DATA_HOME = '/t1.grid.kiae.ru/data/atlas/atlasscratchdisk/rucio'
LOGFILE='/srv/lsm/log/ddm.log'

def log(msg):
    try:
        f=open(LOGFILE, 'a')
        f.write("%s %s %s\n" % (time.ctime(), os.getpid(), msg))
        f.close()
        os.chmod(LOGFILE, 0666)
    except:
        pass

def fail(errorcode=200,msg=None):
    if msg:
        msg='%s %s'%(errorcode, msg)
    else:
        msgs=str(errorcode)
    print msg
    log(msg)
    sys.exit(errorcode)

def adler32(fname):
    """Compute the adler32 checksum of the file, with seed value 1"""
    BLOCKSIZE = 4096 * 1024
    f = open(fname,'r')
    checksum = 1 #Important - dCache uses seed of 1 not 0
    while True:
        data = f.read(BLOCKSIZE)
        if not data:
            break
        checksum = zlib.adler32(data, checksum)
    f.close()
    # Work around problem with negative checksum
    if checksum < 0:
        checksum += 2**32
    return hex(checksum)[2:10].zfill(8).lower()

def md5sum(fname):
    BLOCKSIZE = 4096 * 1024
    f = open(fname,'r')
    checksum = hashlib.md5()
    while True:
        data = f.read(BLOCKSIZE)
        if not data:
            break
        checksum.update(data)
    f.close()
    return checksum.hexdigest().lower()

class SEFactory:
    def __init__(self):
        print 'SEFactory initialization'

    def getSE(self, label, params=None):
        try:
            if label not in ['dropbox', 'grid', 'local']:
                raise AttributeError("Attribute 'label' error: Not found in list")

            if label == 'dropbox':
                se = DropboxSEPlugin(params)

            elif label == 'grid':
                se = GridSEPlugin(params)

            elif label == 'local':
                se = LocalSEPlugin(params)

            else:
                se = SEPlugin()
        except Exception:
            print Exception.message

        return se

class SEPlugin(object):
    def __init__(self, params=None):
        print 'SEPlugin initialization'

    def get(self, src, dest, fsize, fsum):
        raise NotImplementedError("SEPlugin.get not implemented")

    def put(self, src, dest):
        raise NotImplementedError("SEPlugin.put not implemented")


