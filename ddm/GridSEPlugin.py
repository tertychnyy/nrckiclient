import os
import subprocess
from ddm.DDM import adler32, log
from ddm.DDM import fail

BIN_HOME = '/srv/lsm/rrcki-sendjob'

class GridSEPlugin():
    def __init__(self, params=None):
        print 'GridSEPlugin initialization'

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        env = proc.communicate(". %s/setup.sh; python -c 'import os; print os.environ'" % BIN_HOME)[0][:-1]
        env = env.split('\n')[-1]
        import ast
        self.myenv = ast.literal_eval(env)

    def get(self, src, dest, fsize, fsum):
        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('python %s/utils/get.py --size %s --checksum %s %s %s' % (BIN_HOME, fsize, fsum, src, dest))
        #print out

    def put(self, src, dest):
        #proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        #out = proc.communicate('python %s/utils/put.py -t %s %s %s' % (BIN_HOME, 'ATLASSCRATCHDISK', src, dest))
        #print out

        if os.path.isfile(src):
            dataset = dest
            fname = src.split('/')[-1]
            fsize = int(os.path.getsize(src))
            fsum = adler32(src)
            scope = 'user.ruslan'
            rse = 'RRC-KI-T1_SCRATCHDISK'
        else:
            fail(212, "%s: File doesn't exist" % src)

        #TODO
        #dest existing

        #TODO
        #create dataset if not exist

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('rucio upload --rse %s --scope %s --files %s' % (rse, scope, src))
        log('upload out: ' + out)

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('rucio get-metadata %s:%s' % (scope, fname))
        log('metadata out: ' + out)

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('rucio add-files-to-dataset --to %s:%s %s:%s' % (scope, dataset, scope, fname))
        log('add-to-dataset out: ' + out)