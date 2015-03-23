import os
import sys
import time
from common.KILogger import KILogger
from mq.MQ import MQ
_logger = KILogger().getLogger("comsumer")

if __name__ == '__main__':

    _logger.debug(' '.join(sys.argv))

    keys = sys.argv[1:]

    if len(keys) == 1 and keys[0] == 'method.getdataset':
        mq = MQ(host='localhost', exchange='lsm')
        mq.startGetDatasetConsumer()

    if len(keys) == 1 and keys[0] == 'method.putdataset':
        mq = MQ(host='localhost', exchange='lsm')
        mq.startPutDatasetConsumer()

    if len(keys) == 1 and keys[0] == 'method.sendjob':
        mq = MQ(host='localhost', exchange='lsm')
        mq.startSendJobConsumer()
