import commands
import os
import subprocess
import shutil
from common.KILogger import KILogger
from ddm.DDM import SEFactory
from ui.UserIF import DATA_HOME

_logger = KILogger().getLogger("Actions")

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

def moveData(params, fileList, params1, params2):
    if len(fileList) == 0:
        _logger.debug('No files to move')
        return (0, 'No files to move')

    tmpdir = commands.getoutput("uuidgen")

    if 'compress' in params.keys() and 'tgzname' in params.keys():
        compress = params['compress']
        tmpTgzName = params['tgzname']
    else:
        compress = False

    if 'dest' not in params2.keys():
        _logger.error('Attribute error: dest')
        return (1, 'Attribute error: dest')
    dest = params2['dest']

    sefactory = getSEFactory()
    fromSE = sefactory.getSE(params1['label'], params1)
    toSE = sefactory.getSE(params2['label'], params2)

    tmphome = "%s/%s" % (DATA_HOME, tmpdir)
    if not os.path.isdir(tmphome):
        os.makedirs(tmphome)

    tmpout = []
    for f in fileList:
        if ':' in f:
            fname = f.split(':')[1]
        elif '/' in f:
            fname = f.split('/')[-1]
        else:
            fname = f

        tmpfile = os.path.join(tmphome, fname)
        fromSE.get(f, tmphome)
        tmpout.append(tmpfile)


    _logger.debug('Need compress? ' + str(compress))
    if compress:
        _logger.debug('Compress start: ')
        tmpTgz = os.path.join(tmphome, tmpTgzName)
        _logger.debug('TGZ file = ' + tmpTgz)
        wd = os.getcwd()
        os.chdir(tmphome)
        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        proc.communicate("tar -cvzf %s *" % tmpTgz)
        tmpout = [tmpTgz]
        os.chdir(wd)
        _logger.debug('Compress finish:')

    for f in tmpout:
        #put file to SE
        toSE.put(f, dest)

    shutil.rmtree(tmphome)
    return (0, 'moveData success')