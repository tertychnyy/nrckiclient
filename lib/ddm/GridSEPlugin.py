import os
import subprocess
import errno
from common.NrckiLogger import NrckiLogger
from common.utils import adler32

_logger = NrckiLogger().getLogger("GridSEPlugin")
BIN_HOME = '/srv/nrckiclient'

class GridSEPlugin():
    def __init__(self, params={}):
        pass
        
        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        env = proc.communicate(". %s/setup.sh; python -c 'import os; print os.environ'" % BIN_HOME)[0][:-1]
        env = env.split('\n')[-1]
        import ast
        self.myenv = ast.literal_eval(env)
            
    def get(self, src, dest):
        _logger.debug('RUCIO: Try to get file from %s to %s' % (src, dest))
        try:
            scope, fname = src.split(':')
                
            #make dest dir
            try:
                os.makedirs(dest)
            except OSError as exc: # Python >2.5
                if exc.errno == errno.EEXIST and os.path.isdir(dest):
                    pass
                else: raise
            
            _logger.debug('RUCIO: download %s' % src)
            proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
            out = proc.communicate('rucio download --dir %s %s' % (dest, src))
            filetmp = "%s/%s/%s" % (dest, scope, fname)
            fileout = "%s/%s:%s" % (dest, scope, fname)
            if os.path.isfile(filetmp):
                os.rename(filetmp, fileout)
                return (0, fileout)
            else:
                raise Exception()

        except:
            return (1, 'Error')

    def put(self, src, dest):
        _logger.debug('RUCIO: Try to put file from %s to %s' % (src, dest))
        try:
            if os.path.isfile(src):
                dataset = dest
                fname = src.split('/')[-1]
                fsize = int(os.path.getsize(src))
                fsum = adler32(src)
                scope = 'user.ruslan'
                rse = 'RRC-KI-T1_SCRATCHDISK'
            else:
                self.ddm.fail(212, "%s: File doesn't exist" % src)
            _logger.debug('RUCIO: add-dataset %s' % dataset)
            proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
            out = proc.communicate('rucio add-dataset %s' % dataset)

            _logger.debug('RUCIO: upload %s' % src)
            proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
            out = proc.communicate('rucio upload --rse %s --scope %s --files %s' % (rse, scope, src))
            #self.ddm.log('upload out: ' + out)

            _logger.debug('RUCIO: get metadata %s:%s' % (scope, fname))
            proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
            out = proc.communicate('rucio get-metadata %s:%s' % (scope, fname))
            #self.ddm.log('metadata out: ' + out)

            _logger.debug('RUCIO: add-files-to-dataset %s' % dataset)
            proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
            out = proc.communicate('rucio add-files-to-dataset --to %s %s:%s' % (dataset, scope, fname))
            #self.ddm.log('add-to-dataset out: ' + out)
            return (0, '%s:%s' % (scope, fname))
        except:
            return (1, 'Error')