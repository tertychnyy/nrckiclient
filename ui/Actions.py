import commands
import os
import subprocess
import shutil
import sys
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
"""
def putDataset(file, dataset, auth_key):
    #put file into dataset
    sefactory = getSEFactory()

    #se initialization
    #fromSE = sefactory.getSE('dropbox', params={'auth_key': auth_key})
    fromSE = sefactory.getSE('local', params=None)
    toSE = sefactory.getSE('grid', params=None)

    tmphome = "%s/%s" % (DATA_HOME, dataset)
    #os.mkdir(tmphome)
    if not os.path.isdir(tmphome):
        os.makedirs(tmphome)


    fname = file.split('/')[-1]
    tmpfile = os.path.join(tmphome, fname)

    fromSE.get(file, tmpfile)

    tmpTgzName = commands.getoutput('uuidgen')
    tmpTgz = os.path.join(tmphome, tmpTgzName + '.job.input.tgz')
    os.chdir(tmphome)
    proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    out = proc.communicate("tar -cvzf %s *" % tmpTgz)

    toSE.put(tmpTgz, dataset)

    shutil.rmtree(tmphome)
    return {'fname': tmpTgzName,
            'dataset': dataset}
"""

def moveData(params, fromSEparams, toSEparams):
    if 'tmpdir' not in params.keys():
        print 'Attribute error: tmpdir'
        sys.exit(300)
    tmpdir = params['tmpdir']

    if 'compress' in params.keys() and 'tgzname' in params.keys():
        compress = params['compress']
        tmpTgzName = params['tgzname']
    else:
        compress = False

    if 'src' not in fromSEparams.keys():
        print 'Attribute error: src'
        sys.exit(300)
    src = fromSEparams['src']

    if 'dest' not in toSEparams.keys():
        print 'Attribute error: dest'
        sys.exit(300)
    dest = toSEparams['dest']

    sefactory = getSEFactory()
    fromSE = sefactory.getSE(fromSEparams['label'], fromSEparams)
    toSE = sefactory.getSE(toSEparams['label'], toSEparams)

    tmphome = "%s/%s" % (DATA_HOME, tmpdir)
    if not os.path.isdir(tmphome):
        os.makedirs(tmphome)

    fname = src.split('/')[-1]
    tmpfile = os.path.join(tmphome, fname)
    #get file from SE
    fromSE.get(src, tmpfile)

    if compress:
        tmpTgz = os.path.join(tmphome, tmpTgzName)
        wd = os.getcwd()
        os.chdir(tmphome)
        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        proc.communicate("tar -cvzf %s *" % tmpTgz)
        tmpout = tmpTgz
        os.chdir(wd)
    else:
        tmpout = tmpfile
    #put file to SE
    toSE.put(tmpout, dest)
    shutil.rmtree(tmphome)
    return 0