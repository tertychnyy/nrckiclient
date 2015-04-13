import subprocess
import ftplib
from common.KILogger import KILogger

_logger = KILogger().getLogger("DDM")

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
            out = proc.communicate('scp -i %s %s@%s:%s%s %s' % (self.key, self.user, self.host, self.datadir, src, dest))

        except:
            _logger.error('Unable to download:%s to %s' % (src, dest))


    def put(self, src, dest):
        _logger.debug('HPC: Try to put file from %s to %s' % (src, dest))
        try:
            if not dest.startwith('/'):
                dest = '/' + dest
            proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            out = proc.communicate('scp -i %s %s %s@%s:%s%s' % (self.key, self.user, self.host, self.datadir, src, dest))

        except:
            _logger.error('Unable to download:%s to %s' % (src, dest))

    def connect(self, host, login, password):
        ftp = ftplib.FTP(host)
        if self.anonymode:
            ftp.login()
        else:
            ftp.login(login, password)
        return ftp