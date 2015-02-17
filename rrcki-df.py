#!/usr/bin/env python
# lsm-df: return free space in megabytes
#
# $Id: lsm-df,v 1.5 2009/06/24 17:38:07 cgw Exp $
#
# For now, instead of getting correct dCache space information,
# we lie and always say 10TB is free

import sys, os, time

DEFAULT_SPACE=10*1024*1024
LOGFILE='/var/log/lsm/lsm-df.log'

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
        msg=str(errorcode)
    print msg
    log(msg)
    sys.exit(errorcode)

log(' '.join(sys.argv))
args=sys.argv[1:]
while args and args[0].startswith('-'): #Skip all cmd-line flags
    args.pop(0)
arg = args and args[0]

if not arg or '/pnfs/uchicago.edu' in arg:
    log("return %s"%DEFAULT_SPACE)
    print DEFAULT_SPACE
    sys.exit(0)

p=os.popen('df -P -B1M %s'%arg)
lines=p.readlines()
status=p.close()

if status:
    fail(200, "FAILED")

try:
    result = lines[1].split()[3]
except:
    result = None

if result:
    log("0 OK %s" % result)
    print result
    sys.exit(0)
else:
    fail(200, "FAILED")