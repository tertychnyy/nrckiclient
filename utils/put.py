#!/usr/bin/env python
# lsm-put: copy files from local disk to mass storage
#
# $Id: lsm-put,v 1.13 2010/03/04 14:54:07 cgw Exp $
#
# lsm-put [-t token] [--size N] [--checksum csum]

import sys, os, stat, time
from timed_command import timed_command

#Tunable parameters
COPY_TIMEOUT=3600
COPY_RETRIES=5
COPY_COMMAND='lcg-cp'
COPY_ARGS='-b -U srmv2'
COPY_SETUP='setup.sh'
COPY_PREFIX='srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN='
PNFSROOT='/pnfs/uchicago.edu'
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

def get_dcache_size(fname):
    data=get_level2(fname,'l')
    if data:
        return int(data)
    else:
        return None

def get_dcache_checksum(fname):
    data=get_level2(fname,'c')
    if not data:
        return None
    if ':' in data:
        data = data.split(':')[1]
    return data.lower()

def get_level2(fname, attr):
    level2 = "%s/.(use)(2)(%s)" % os.path.split(fname)
    if not os.path.exists(level2):
        return None
    try:
        f= open(level2)
        lines = f.readlines()
        f.close()
    except:
        return None
    tok = lines[1].split(';')
    if tok[0].startswith(':'):
        tok[0]=tok[0][1:]
        for t in tok:
            a,v=t.split("=")
            if a==attr:
                return v
    return None

def getSURL(src, dataset):
    return

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

dest = getSURL(src, dataset)



if os.path.isfile(dest):
    ### * 211 - File already exist and is different (size/checksum).
    ### * 212 - File already exist and is the same as the source (same size/checksum)
    fsize = None
    if size:
        try:
            fsize = get_dcache_size(dest)
        except:
            fsize = "UNKNOWN"
    fchecksumval = None
    if checksumval:
        try:
            fchecksumval = get_dcache_checksum(dest)
        except:
            fchecksumval = "UNKNOWN"
    if fchecksumval != checksumval or fsize != size:
        fail(211, "%s size:%s %s, checksum: %s %s"% (src, fsize,size,
                                                 fchecksumval,checksumval))
    else:
        fail(212, "%s: File exists" % dest)

if token:
    token_arg = '-S %s' % token
else:
    token_arg= ''

cmd = ''
if COPY_SETUP:
    cmd = ". %s;" % COPY_SETUP

cmd += "%s %s %s %s %s" % (
    COPY_COMMAND, COPY_ARGS, token_arg, src, dest)

for retry in xrange(COPY_RETRIES+1):
    log("executing %s retry %s" % (cmd, retry))
    exit_status, time_used, cmd_out, cmd_err = timed_command(cmd,COPY_TIMEOUT)
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

if size:
    try:
        fsize = get_dcache_size(dest)
    except:
        fsize = "UNKNOWN"
    if size != fsize:
        try:
            os.unlink(dest)
        except:
            pass
        fail(204, "Size mismatch %s!=%s"%(fsize,size))

if checksumval:
    try:
        fchecksumval = get_dcache_checksum(dest)
    except:
        fchecksumval = "UNKNOWN"
    if fchecksumval != checksumval:
        try:
            os.unlink(dest)
        except:
            pass
        fail(205, "Checksum mismatch %s!=%s"%(fchecksumval,checksumval))

log("%s OK" % dest)

print dest

if size:
    print "size", size
if checksumval:
    print "adler32", checksumval