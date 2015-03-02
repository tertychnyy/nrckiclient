import os
import time
import sys
import shutil
from utils.get import adler32

LOGFILE='/srv/lsm/log/get.log'

def log(msg):
    try:
        f=open(LOGFILE, 'a')
        f.write("%s %s\n" % (time.ctime(), msg))
        f.close()
        os.chmod(LOGFILE, 0666)
    except:
        pass

def fail(errorcode=200,msg=None):
    if msg:
        msg='%s %s'%(errorcode, msg)
    else:
        msg=str(errorcode)
    print msg
    log(msg)
    sys.exit(errorcode)

class LocalSEPlugin():
    def __init__(self, params=None):
        print 'Local SEPlugin initialization'

    def get(self, src, dest):
        if not os.path.isfile(src):
            fail(210, "%s: File not found" %src)

        fname = src.split('/')[-1]
        fsize = int(os.path.getsize(src))
        fsum = adler32(src)

        ## Check for 'file exists'
        if os.path.isfile(dest):
            ### * 211 - File already exist and is different (size/checksum).
            ### * 212 - File already exist and is the same as the source (same size/checksum)

            size = None
            if fsize:
                try:
                    size = int(os.path.getsize(dest))
                except:
                    size = "UNKNOWN"
            sum = None
            if fsum:
                try:
                    fchecksumval = adler32(dest)
                except Exception,e:
                    fchecksumval = "UNKNOWN"
                    log("%s failed with: %s" % ('adler32', e) )
            if fchecksumval != fsum or fsize != size:
                fail(211, "%s size:%s %s, checksum: %s %s"%
                     (dest, fsize, size, fchecksumval, fsum))
            else:
                fail(212, "%s: File exists" % dest)

        if dest.endswith('/'):
            destdir = dest
        else:
            destdir = os.path.join(dest.split('/')[:-1])

        try:
            shutil.copy2(src, destdir)
        except:
            fail(201, 'Unable to move:%s %s' % (src, destdir))


    def put(self, src, dest):
        if not os.path.isfile(src):
            fail(210, "%s: File not found" %src)

        self.get(src, dest)
        shutil.rmtree(os.path.join(src.split('/')[:-1]))