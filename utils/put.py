#!/usr/bin/env python
# lsm-put: copy files from local disk to mass storage
#
# $Id: lsm-put,v 1.13 2010/03/04 14:54:07 cgw Exp $
#
# lsm-put [-t token] [--size N] [--checksum csum]

#lcg-cp --verbose --vo atlas -b --srm-timeout=3600 --connect-timeout=300 --sendreceive-timeout=3600 -U srmv2 -S ATLASSCRATCHDISK fil
#e:///s/ls2/home/users/poyda/testpilot/Panda_Pilot_32495_1423052812/PandaJob_66_1423052815/stageout.job.output.lib.txt srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN=/t1.grid.kiae.ru/data/atlas/atlasscratchdisk/rucio/NULL/8c/73/stageout.j
#ob.output.lib.txt
import commands
import datetime
import exceptions
import sys, os, stat, time

from dq2.clientapi import DQ2
from dq2.filecatalog.FileCatalogUnknownFactory import FileCatalogUnknownFactory
from dq2.filecatalog.FileCatalogException import FileCatalogException
from rucio.common.exception import FileConsistencyMismatch,DataIdentifierNotFound,UnsupportedOperation
from ddm.DDM import rucioAPI

try:
    from dq2.clientapi.cli import Register2
except:
    pass
try:
    from dq2.filecatalog.rucio.RucioFileCatalogException import RucioFileCatalogException
except:
    # dummy class
    class RucioFileCatalogException:
        pass

#Tunable parameters
from utils.get import adler32

COPY_TIMEOUT=3600
COPY_RETRIES=5
COPY_COMMAND='lcg-cp'
COPY_ARGS='--verbose --vo atlas -b --srm-timeout=3600 --connect-timeout=300 --sendreceive-timeout=3600 -U srmv2'
SITE_PREFIX = 'srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN='
SITE_DATA_HOME = '/t1.grid.kiae.ru/data/atlas/atlasscratchdisk/rucio'
PERM_DIR=0775
PERM_FILE=0664

LOGFILE='/srv/lsm/log/put.log'

def log(msg):
    try:
        f=open(LOGFILE, 'a')
        f.write("%s %s %s\n" % (time.ctime(), os.getpid(), msg))
        f.close()
        os.chmod(LOGFILE, 0666)
    except:
        pass

def fail(errorcode=200,msg=None):
    if msg:
        msg='%s %s'%(errorcode, msg)
    else:
        msgs=str(errorcode)
    print msg
    log(msg)
    sys.exit(errorcode)

def getChecksum(surl):
    cmd = 'lcg-get-checksum -b -T srmv2 --connect-timeout=300 --sendreceive-timeout=3600'
    cmd += ' %s' % surl
    p = os.popen(cmd)
    output = p.read()
    return output.split('\t')[0]

def getSURL(scope, lfn):
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

def extract_scope(self, dsn):
        if ':' in dsn:
            return dsn.split(':')[:2]
        scope = dsn.split('.')[0]
        if dsn.startswith('user') or dsn.startswith('group'):
            scope = ".".join(dsn.split('.')[0:2])
        return scope,dsn

def register(fname, scope, dataset, surl, fsize, fsum):

    # extract scope from dataset
    guid = commands.getoutput('uuidgen')
    dataset = 'user.ruslan.data.%s' % commands.getoutput('uuidgen')
    dsn = dataset

    attachmentList = []
    files = []

    lfn = fname
    # set metadata
    meta = {'guid': guid}
    # set mandatory fields
    file = {'scope': scope,
            'name' : lfn,
            'bytes': fsize,
            'meta' : meta}
    file['adler32'] = fsum
    file['pfn'] = surl
    # append files
    files.append(file)
    # add attachment
    attachment = {'scope':scope,
                  'name':dsn,
                  'dids':files}

    attachmentList.append(attachment)

    try:
        log("DQ2 registraion for file: %s" % fname)

        client = RucioClient()
        client.add_files_to_datasets(attachmentList)
    except:
        # unknown errors
        errType,errValue = sys.exc_info()[:2]
        out = '%s : %s' % (errType,errValue)
        log(out)

def register2(lfn, dataset, surl, fsize, fsum):
    guid = commands.getoutput('uuidgen')

    #describe files
    attachmentList = []
    files = []

    scope,dsn = extract_scope(dataset)
    meta = {'guid': guid}
    if ':' in lfn:
        s, lfn = lfn.split(':')
    else:
        s = scope
    file = {'scope': scope,
            'name' : lfn,
            'bytes': fsize,
            'meta' : meta,
            'adler32': fsum,
            'pfn': surl}
    files.append(file)

    attachment = {'scope':scope,
                    'name':dsn,
                    'dids':files}

    attachmentList.append(attachment)

    # add files
    nTry = 3
    for iTry in range(nTry):
        isFatal  = False
        isFailed = False
        regStart = datetime.datetime.utcnow()
        regMsgStr = ''
        try:
            regMsgStr = "LFC+DQ2 registraion with for {1} files ".format(1)
            log('%s %s' % ('registerFilesInDatasets',str(attachmentList)))
            out = rucioAPI.registerFilesInDataset(attachmentList)
        except (DQ2.DQClosedDatasetException,
                DQ2.DQFrozenDatasetException,
                DQ2.DQUnknownDatasetException,
                DQ2.DQDatasetExistsException,
                DQ2.DQFileMetaDataMismatchException,
                FileCatalogUnknownFactory,
                FileCatalogException,
                DataIdentifierNotFound,
                RucioFileCatalogException,
                FileConsistencyMismatch,
                UnsupportedOperation,
                exceptions.KeyError):
            # fatal errors
            errType,errValue = sys.exc_info()[:2]
            out = '%s : %s' % (errType,errValue)
            isFatal = True
            isFailed = True
        except:
            # unknown errors
            errType,errValue = sys.exc_info()[:2]
            out = '%s : %s' % (errType,errValue)
            isFatal = False
            isFailed = True
        regTime = datetime.datetime.utcnow() - regStart
        log(regMsgStr + 'took %s.%03d sec' % (regTime.seconds,regTime.microseconds/1000))
        # failed
        if isFailed or isFatal:
            log('Error: %s' % out)
            if (iTry+1) == nTry or isFatal:
                errMsg = "Could not add files to DDM: "
                log('%s %s' % (errMsg, out))
                return 1
            log("Try:%s" % iTry)
            # sleep
            time.sleep(10)
        else:
            log('%s' % str(out))
            break

if __name__ == '__main__':
    token=None
    size=None
    checksumtype=None
    checksumval=None

    log(' '.join(sys.argv))

    args = sys.argv[1:]
    while args and args[0].startswith('-'):
        arg = args.pop(0)
        val = args.pop(0)
        if arg=='-t':
            token = val
        elif arg=='--size' or arg=='-s':
            size = int(val)
        elif arg=='--checksum' or arg=='-c':
            if ':' in val:
                checksumtype, checksumval = val.split(':')
            else:
                checksumtype = "md5"
            # Only Adler32's supported in dCache
            if not checksumtype.startswith("ad"):
                fail(202, "Unsupported checksum type %s" % checksumtype)

    if len(args) != 2:
        fail(202, "Invalid command")

    src, dataset = args
    fname = src.split('/')[-1]
    fsize = int(os.path.getsize(src))
    fsum = adler32(src)
    scope = 'user.ruslan'
    dest = getSURL(scope, fname)


    """
    if os.path.isfile(src):
        ### * 211 - File already exist and is different (size/checksum).
        ### * 212 - File already exist and is the same as the source (same size/checksum)
        fchecksumval = None
        if checksumval:
            try:
                fchecksumval = getChecksum(dest)
            except:
                fchecksumval = "UNKNOWN"
        if fchecksumval != checksumval:
            fail(211, "%s checksum: %s %s"% (src, fchecksumval,checksumval))
        else:
            fail(212, "%s: File exists" % dest)
    """

    if token:
        token_arg = '-S %s' % token
    else:
        token_arg= ''

    cmd = ''
    cmd += "%s %s %s file://%s %s 2>&1" % (
        COPY_COMMAND, COPY_ARGS, token_arg, src, dest)

    log("Executing command: %s" % (cmd))
    t0 = os.times()

    try:
        s, o = commands.getstatusoutput(cmd)
    except Exception, e:
        log("!!WARNING!!2999!! lcg-cp threw an exception: %s" % (o))

    t1 = os.times()
    t = t1[4] - t0[4]
    log("Command finished after %f s" % (t))

    if checksumval:
        try:
            fchecksumval = getChecksum(dest)
        except:
            fchecksumval = "UNKNOWN"
        if fchecksumval != checksumval:
            fail(205, "Checksum mismatch %s!=%s"%(fchecksumval,checksumval))

    register2(fname, dataset, dest, fsize, fsum)

    log("%s OK" % dest)

    print dataset

    if size:
        print "size", size
    if checksumval:
        print "adler32", checksumval