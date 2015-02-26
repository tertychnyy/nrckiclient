#!/usr/bin/env python
# lsm-put: copy files from local disk to mass storage
#
# $Id: lsm-put,v 1.13 2010/03/04 14:54:07 cgw Exp $
#
# lsm-put [-t token] [--size N] [--checksum csum]

#lcg-cp --verbose --vo atlas -b --srm-timeout=3600 --connect-timeout=300 --sendreceive-timeout=3600 -U srmv2 -S ATLASSCRATCHDISK fil
#e:///s/ls2/home/users/poyda/testpilot/Panda_Pilot_32495_1423052812/PandaJob_66_1423052815/stageout.job.output.lib.txt srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN=/t1.grid.kiae.ru/data/atlas/atlasscratchdisk/rucio/NULL/8c/73/stageout.j
#ob.output.lib.txt


import sys, os, stat, time

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

def register():
    print 'registration'

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
    dest = getSURL('user.ruslan', fname)


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
    """
    for retry in xrange(COPY_RETRIES+1):
        log("executing %s retry %s" % (cmd, retry))
        exit_status, time_used, cmd_out, cmd_err = timed_command(cmd, COPY_TIMEOUT)
        # lcg-cp does not return error codes, and on a successful transfer it
        # prints a stray newline to stdout - anything other than whitespace
        # indicates an error
        if exit_status or cmd_err or cmd_out.strip():
            log("command failed %s %s %s" % (exit_status, cmd_err, cmd_out.strip()))
            if os.path.exists(dest):
                try:
                    os.unlink(dest)
                except:
                    pass
            time.sleep(15)
        else:
            break

    if checksumval:
        try:
            fchecksumval = getChecksum(dest)
        except:
            fchecksumval = "UNKNOWN"
        if fchecksumval != checksumval:
            fail(205, "Checksum mismatch %s!=%s"%(fchecksumval,checksumval))
    """
    print cmd
    print fname
    print dest
    print fsize
    print fsum
    #register(fname, dest, fsize, fsum)

    log("%s OK" % dest)

    print dataset

    if size:
        print "size", size
    if checksumval:
        print "adler32", checksumval