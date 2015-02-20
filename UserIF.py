import os
import subprocess
from DDM import UserSE, GridSE


VENV_HOME = '/srv/lsm/.venv/rrcki-sendjob'
BIN_HOME = '/srv/lsm/rrcki-sendjob'
DATA_HOME = '/srv/lsm/data'

class UserIF:
    def __init__(self):
        self.buffer = None

    #get dataset from grid
    def getDataset(self, dataset):
        fromSE = GridSE()
        toSE = UserSE()

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        files = proc.communicate("dq2-ls -f -p -L RRC-KI-T1_SCRATCHDISK %s | grep 'srm://'" % dataset, env=fromSE.myenv)
        print files

        tmphome = "%s/%s" % (DATA_HOME, dataset)
        os.mkdir(tmphome)

        for src in files:
            fname = file.split('/')[-1]
            tmpfile = os.path.join(tmphome, fname)
            fromSE.get(src, tmpfile)
            toSE.put(tmpfile, os.path.join('/', dataset, fname))

        os.rmdir(tmphome)
        return

# Singleton
userIF = UserIF()
del UserIF

#Web interface methods
def getDataset(req, dataset):
    userIF.getDataset(dataset)