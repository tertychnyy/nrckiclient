import os
import subprocess
import shutil
from ddm.DDM import SEFactory
from ui.UserIF import DATA_HOME

def getSEFactory():
    factory = SEFactory()
    return factory

def getDataset(dataset, auth_key):
    #get dataset by name
    sefactory = getSEFactory()

    #se initialization
    fromSE = sefactory.getSE('grid', params=None)
    toSE = sefactory.getSE('dropbox', params={'auth_key': auth_key})

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

def putDataset(file, dataset, auth_key):
    #put file into dataset
    sefactory = getSEFactory()

    #se initialization
    fromSE = sefactory.getSE('dropbox', params={'auth_key': auth_key})
    toSE = sefactory.getSE('grid', params=None)

    tmphome = "%s/%s" % (DATA_HOME, dataset)
    os.mkdir(tmphome)

    fname = file.split('/')[-1]
    tmpfile = os.path.join(tmphome, fname)

    fromSE.get(file, tmpfile)
    toSE.put(tmpfile, dataset)

    shutil.rmtree(tmphome)
    return