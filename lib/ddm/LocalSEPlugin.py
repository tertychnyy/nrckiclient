import os
import time
import sys
import shutil
from common.KILogger import KILogger

_logger = KILogger().getLogger("LocalSEPlugin")

class LocalSEPlugin():
    def __init__(self, params={}):
        pass

    def get(self, src, dest):
        try:
            if not os.path.isfile(src):
                _logger.error("%s: File not found" %src)

            shutil.copy2(src, dest)
        except:
            _logger.error('Unable to move:%s %s' % (src, dest))


    def put(self, src, dest):
        if not os.path.isfile(src):
            _logger.error("%s: File not found" %src)

        self.get(src, dest)
        shutil.rmtree(os.path.join(src.split('/')[:-1]))