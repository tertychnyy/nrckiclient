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
        pass

    def get(self, src, dest):
        try:
            if not os.path.isfile(src):
                fail(210, "%s: File not found" %src)

            shutil.move(src, dest)
        except:
            fail(201, 'Unable to move:%s %s' % (src, dest))


    def put(self, src, dest):
        if not os.path.isfile(src):
            fail(210, "%s: File not found" %src)

        self.get(src, dest)
        shutil.rmtree(os.path.join(src.split('/')[:-1]))