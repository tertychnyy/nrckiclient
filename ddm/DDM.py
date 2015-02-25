from ddm.DropboxSEPlugin import DropboxSEPlugin
from ddm.GridSEPlugin import GridSEPlugin

BIN_HOME='/srv/lsm/rrcki-sendjob'
SITE_PREFIX = 'srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN='
SITE_DATA_HOME = '/t1.grid.kiae.ru/data/atlas/atlasscratchdisk/rucio'

class SEFactory:
    def __init__(self):
        print 'SEFactory initialization'

    def getSE(self, label, params=None):
        try:
            if label not in ['dropbox', 'grid']:
                raise AttributeError("Attribute 'label' error: Not found in list")

            if label == 'dropbox':
                se = DropboxSEPlugin(params)

            elif label == 'grid':
                se = GridSEPlugin(params)

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
