import os
import shutil
import subprocess
from DDM import SEFactory


VENV_HOME = '/srv/lsm/.venv/rrcki-sendjob'
BIN_HOME = '/srv/lsm/rrcki-sendjob'
DATA_HOME = '/srv/lsm/data'

class UserIF:
    def __init__(self):
        self.sefactory = SEFactory()

    #get dataset from grid
    def getDataset(self, dataset):
        #get dataset by name

        #se initialization
        fromSE = self.sefactory.getSE('grid')
        toSE = self.sefactory.getSE('dropbox')

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fromSE.myenv)
        files = proc.communicate("dq2-ls -f -p -L RRC-KI-T1_SCRATCHDISK %s | grep 'srm://'" % dataset)[0].split('\n')[:-1]

        tmphome = "%s/%s" % (DATA_HOME, dataset)
        os.mkdir(tmphome)

        for src in files:
            fname = src.split('/')[-1]

            proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fromSE.myenv)
            fprops = proc.communicate("dq2-ls -f %s | grep %s" % (dataset, fname))[0].split('\t')
            fsize = int(fprops[-1])
            fsum = fprops[-2]

            tmpfile = os.path.join(tmphome, fname)
            fromSE.get(src, tmpfile, fsize, fsum)
            toSE.put(tmpfile, os.path.join('/', dataset, fname))

        #os.rmdir(tmphome)
        shutil.rmtree(tmphome)
        return

# Singleton
userIF = UserIF()
del UserIF

#Web interface methods
def getDataset(req, dataset):
    userIF.getDataset(dataset)