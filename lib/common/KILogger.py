import logging

# setup logger
_formatter = logging.Formatter('%(asctime)s %(name)-12s: %(levelname)-8s %(message)s')

logdir = "/srv/lsm/log"

class KILogger:
    def __init__(self):
        pass

    def getLogger(self, lognm):
        logh = logging.getLogger("lsm.log.%s" % lognm)
        txth = logging.FileHandler('%s/lsm-%s.log' % (logdir, lognm))
        txth.setLevel(logging.DEBUG)
        txth.setFormatter(_formatter)
        logh.addHandler(txth)
        return logh

