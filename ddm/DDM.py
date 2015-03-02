import hashlib
from ddm.DropboxSEPlugin import DropboxSEPlugin
from ddm.GridSEPlugin import GridSEPlugin
from ddm.LocalSEPlugin import LocalSEPlugin

from rucio.client import Client as RucioClient
from rucio.common.exception import UnsupportedOperation,DataIdentifierNotFound,\
    FileAlreadyExists,Duplicate,DataIdentifierAlreadyExists

BIN_HOME='/srv/lsm/rrcki-sendjob'
SITE_PREFIX = 'srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN='
SITE_DATA_HOME = '/t1.grid.kiae.ru/data/atlas/atlasscratchdisk/rucio'

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

class DDM:
    def __init__(self):
        pass

# instantiate
ddm = DDM()
del DDM
