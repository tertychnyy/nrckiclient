import logging

# setup logger
_formatter = logging.Formatter('%(asctime)s %(name)-12s: %(levelname)-8s %(message)s')

logdir = "/srv/nrckiclient/log"

class KILogger:
    def __init__(self):
        pass

    def getLogger(self, lognm):
        logh = logging.getLogger("nrckiclient.log.%s" % lognm)
        txth = logging.FileHandler('%s/nrckiclient-%s.log' % (logdir, lognm))
        txth.setLevel(logging.DEBUG)
        txth.setFormatter(_formatter)
        logh.addHandler(txth)
        return logh
