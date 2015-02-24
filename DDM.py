import os
import time
import sys
from plugins.DropboxSEPlugin import DropboxSEPlugin
from plugins.GridSEPlugin import GridSEPlugin

BIN_HOME='/srv/lsm/rrcki-sendjob'
LOGFILE='/srv/lsm/log/ddm.log'
SITE_PREFIX = 'srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN='
SITE_DATA_HOME = '/t1.grid.kiae.ru/data/atlas/atlasscratchdisk/rucio'


def log(self, msg):
        try:
            f = open(LOGFILE, 'a')
            f.write("%s %s\n" % (time.ctime(), msg))
            f.close()
            os.chmod(LOGFILE, 0666)
        except:
            pass

def fail(self, errorcode=200, msg=None):
    if msg:
        msg='%s %s'%(errorcode, msg)
    else:
        msg=str(errorcode)
    print msg
    log(msg)
    sys.exit(errorcode)

def getSURL(self, scope, lfn):
        #get full surl
        # /<site_prefix>/<space_token>/rucio/<scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>
        try:
            # for python 2.6
            import hashlib
            hash = hashlib.md5()
        except:
            # for python 2.4
            import md5
            hash = md5.new()

        correctedscope = "/".join(scope.split('.'))
        hash.update('%s:%s' % (scope, lfn))
        hash_hex = hash.hexdigest()[:6]
        return '%s%s/%s/%s/%s/%s' % (SITE_PREFIX, SITE_DATA_HOME, correctedscope, hash_hex[0:2], hash_hex[2:4], lfn)

class SEFactory:
    def __init__(self):
        self.se = SEPlugin()

    def getSE(self, label):
        se = self.se
        try:
            if label not in ['dropbox', 'grid']:
                raise AttributeError("Attribute 'label' error: Not found in list")

            if label == 'dropbox':
                se = DropboxSEPlugin()

            if label == 'grid':
                se = GridSEPlugin()
        except Exception:
            log(Exception.message)

        return se

class SEPlugin:
    def __init__(self):
        print 'SEPlugin initialization'

    def get(self, src, dest, fsize, fsum):
        print 'SEPlugin.get: Not implemented'
        raise NotImplementedError("SEPlugin.get not implemented")

    def put(self, src, dest):
        print 'SEPlugin.put: Not implemented'
        raise NotImplementedError("SEPlugin.put not implemented")

