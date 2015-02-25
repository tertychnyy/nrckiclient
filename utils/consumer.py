import os
import sys
import time
from mq.MQ import MQ
LOGFILE='/var/log/lsm/lsm-consumer.log'

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

if __name__ == '__main__':

    log(' '.join(sys.argv))

    keys = sys.argv[1:]

    if len(keys) == 1 and keys[0] == 'method.getdataset':
        mq = MQ(host='localhost', exchange='lsm')
        mq.startGetDatasetConsumer()

    if len(keys) == 1 and keys[0] == 'method.putdataset':
        mq = MQ(host='localhost', exchange='lsm')
        mq.startPutDatasetConsumer()

