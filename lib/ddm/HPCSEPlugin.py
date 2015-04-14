import subprocess
import ftplib
from common.NrckiLogger import NrckiLogger

_logger = NrckiLogger().getLogger("DDM")

class HPCSEPlugin():
    def __init__(self, params=None):
        self.key = ''
        self.host = ''
        self.user = ''
        self.datadir = ''

    def get(self, src, dest):
        _logger.debug('HPC: Try to get file from %s to %s' % (src, dest))
        try:
            if not src.startwith('/'):
                src = '/' + src
            proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            #out = proc.communicate('scp -i %s %s@%s:%s%s %s' % (self.key, self.user, self.host, self.datadir, src, dest))
            out = proc.communicate("rsync -av -e 'ssh -i %s' %s@%s:%s%s %s/" % (self.key, self.user, self.host, self.datadir, src, dest))

        except:
            _logger.error('Unable to download:%s to %s' % (src, dest))


    def put(self, src, dest):
        _logger.debug('HPC: Try to put file from %s to %s' % (src, dest))
        try:
            if not dest.startwith('/'):
                dest = '/' + dest
            proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            #out = proc.communicate('scp -i %s %s %s@%s:%s%s' % (self.key, self.user, self.host, self.datadir, src, dest))
            out = proc.communicate("rsync -av -e 'ssh -i %s' %s %s@%s:%s%s/" % (self.key, src, self.user, self.host, self.datadir, dest))

        except:
            _logger.error('Unable to download:%s to %s' % (src, dest))
