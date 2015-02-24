import os
import sys
import mq.MQ.MQ
from mq.MQ import MQ

BIN_HOME='/srv/lsm/rrcki-sendjob'

if __name__ == '__main__':
    keys = sys.argv[1:]

    os.chdir(BIN_HOME)

    mq = MQ()
    mq.startConsumer(keys)