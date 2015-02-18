#!/usr/bin/env python
# lsm-rm:  delete files and directories from /scratch or dcache
#
# $Id: lsm-rm,v 1.6 2009/06/24 17:38:07 cgw Exp $

import sys, os, time

LOGFILE='/var/log/lsm/lsm-rm.log'
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

def try_delete(name):
    try:
        if os.path.isdir(name):
            os.rmdir(name)
        else:
            os.unlink(name)
    except:
        exc,msg,tb=sys.exc_info()
        fail(200, msg)

log(' '.join(sys.argv))
args=sys.argv[1:]
while args and args[0].startswith('-'): #Skip all cmd-line flags
    args.pop(0)

if len(args) != 1:
    fail(202, "Invalid command")

arg=args[0]
if arg.startswith('/scratch'):
    try_delete(arg)
else:
    index = arg.find('/pnfs/uchicago.edu')
    if index>0:
        try_delete(arg[index:])
    else:
        fail(200, "cannot delete %s"%arg)

log("0 OK")