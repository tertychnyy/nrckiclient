#!/usr/bin/env python


# lsm-get: retrieve files from storage element to local disk
#
# $Id: lsm-get,v 1.19 2010/07/09 19:06:48 sarah Exp $
#
# lsm-get [-t token] [--size N] [--checksum csum] [---guid]

import sys, os, stat, time, zlib, hashlib
#md5->hashlib

#Tunable parameters
COPY_TIMEOUT=3600
COPY_RETRIES=5
COPY_COMMAND='lcg-cp'
COPY_ARGS='-b -D srmv2 -S ATLASSCRATCHDISK'
COPY_PREFIX='srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN='
PNFSROOT='srm://'
PERM_DIR=0775
PERM_FILE=0664

LOGFILE='/srv/lsm/log/get.log'

class Timer:
     def __init__(self):
         self.t0 = time.time()
     def __str__(self):
         return "%0.2f" % (time.time() - self.t0)
     def __float__(self):
         return time.time() - self.t0


def adler32(fname):
    """Compute the adler32 checksum of the file, with seed value 1"""
    BLOCKSIZE = 4096 * 1024
    f = open(fname,'r')
    checksum = 1 #Important - dCache uses seed of 1 not 0
    while True:
        data = f.read(BLOCKSIZE)
        if not data:
            break
        checksum = zlib.adler32(data, checksum)
    f.close()
    # Work around problem with negative checksum
    if checksum < 0:
        checksum += 2**32
    return hex(checksum)[2:10].zfill(8).lower()

def md5sum(fname):
    BLOCKSIZE = 4096 * 1024
    f = open(fname,'r')
    checksum = hashlib.md5()
    while True:
        data = f.read(BLOCKSIZE)
        if not data:
            break
        checksum.update(data)
    f.close()
    return checksum.hexdigest().lower()

def log(msg):
    try:
        f=open(LOGFILE, 'a')
        f.write("%s %s %s\n" % (time.ctime(), sessid, msg))
        f.close()
        os.chmod(LOGFILE, 0666)
    except:
        pass

def fail(errorcode=200,msg=None):
    if msg:
        msg='%s %s'%(errorcode, msg)
    else:
        msg=str(errorcode)
    print msg
    log(msg)
    sys.exit(errorcode)

token=None
size=None
checksumtype=None
checksumval=None
guid=None
sessid="%s.%s" % ( int(time.time()), os.getpid() )

log(' '.join(sys.argv))

args = sys.argv[1:]
while args and args[0].startswith('-'):
    arg = args.pop(0)
    val = args.pop(0)
    if arg=='-t':
        token = val
    elif arg=='--size' or arg=='-s':
        size = int(val)
    elif arg=='--guid' or arg=='-g':
        guid = val
    elif arg=='--checksum' or arg=='-c':
        if ':' in val:
            checksumtype, checksumval = val.split(':')
        else:
            checksumtype = "md5"
            checksumval = val
        checksumval = checksumval.lower()
        if checksumtype.startswith('md5'):
            checksumfunc = md5sum
        elif checksumtype.startswith('ad'):
            checksumfunc = adler32
        else:
            fail(202, "Unsupported checksum type %s" % checksumtype)

if len(args) != 2:
    fail(202, "Invalid command")
    sys.exit(1)

src_url, dest = args

index = src_url.find(PNFSROOT)
if index >= 0:
    src = src_url[index:]
else:
    fail(202, "Invalid command")

sfn = src_url.split('srm://sdrm.t1.grid.kiae.ru')[1]
src = COPY_PREFIX + sfn

## Check for 'file exists'
if os.path.isfile(dest):
    ### * 211 - File already exist and is different (size/checksum).
    ### * 212 - File already exist and is the same as the source (same size/checksum)
    fsize = None
    if size:
        try:
            fsize = os.stat(dest)[stat.ST_SIZE]
        except:
            fsize = "UNKNOWN"
    fchecksumval = None
    if checksumval:
        t = Timer()
        try:
            fchecksumval = checksumfunc(dest)
            log("local checksum took %s seconds for %s byte file,  %.2f b/s"
                % (t,fsize, fsize/float(t)) )
        except Exception,e:
            fchecksumval = "UNKNOWN"
            log("%s failed with: %s" % (checksumfunc, e) )
    if fchecksumval != checksumval or fsize != size:
        fail(211, "%s size:%s %s, checksum: %s %s"%
             (dest, fsize, size, fchecksumval, checksumval))
    else:
        fail(212, "%s: File exists" % dest)

if os.path.isdir(dest) and not dest.endswith('/'):
    dest += '/'
if dest.endswith('/'):
    basename = src.split('/')[-1]
    dest += basename

dirname, filename = os.path.split(dest)
if not os.path.exists(dirname):
    try:
        os.makedirs(dirname,PERM_DIR)
    except:
        ##Might already exist, created by another process
        pass

if not os.path.exists(dirname):
    fail(206, "Cannot create %s" % dirname)

cmd = "%s %s" % (COPY_COMMAND, COPY_ARGS)
cmd += " '%s' file://%s 2>&1" % (src, dest)

t = Timer()
p = os.popen(cmd)
output = p.read()
if output:
    log(output)
status = p.close()
log("transfer took %s seconds for a %s byte file,  %.2f b/s"
    % (t,size, size/float(t)) )

if status:
    ##Transfer failed.  Could retry, but that's already
    ## done inside of pcache
    if os.path.exists(dest):
        try:
            os.unlink(dest)
        except:
            pass
    fail(201, "Copy command failed")

if not os.path.exists(dest):
    fail(201, "Copy command failed")

try:
    os.chmod(dest, PERM_FILE)
except:
    pass

## Verify size/checksum if asked for
### XXXXX TODO move this to pcache
if size:
    try:
        fsize = os.stat(dest)[stat.ST_SIZE]
    except:
        fsize = "UNKNOWN"
    if size != fsize:
        fail(204, "Size mismatch %s!=%s"%(fsize,size))

if checksumval:
    t = Timer()
    try:
        fchecksumval = checksumfunc(dest)
        log("local checksum took %s seconds for %s byte file,  %.2f b/s" % (t,fsize, fsize/float(t)) )
    except Exception,e:
        fchecksumval = "UNKNOWN"
        log("%s failed with: %s" % (checksumfunc, e) )
    if fchecksumval != checksumval:
        fail(205, "Checksum mismatch %s!=%s"%(fchecksumval, checksumval))

log("0 OK")

