import sys
from ui.UserIF import userIF
from common.KILogger import KILogger

_logger = KILogger().getLogger("comsumer")

if __name__=='__main__':
    _logger.debug('cmd: ' + str(sys.argv[1:]))
    if False:
        _logger.error('Invalid arguments')

    args = sys.argv[1:]
    userIF.sendJob(args)

