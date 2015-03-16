import os
import subprocess
from ddm.DDM import DDM

BIN_HOME = '/srv/lsm/rrcki-sendjob'

class GridSEPlugin():
    def __init__(self, params=None):
        print 'GridSEPlugin initialization'

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        env = proc.communicate(". %s/setup.sh; python -c 'import os; print os.environ'" % BIN_HOME)[0][:-1]
        env = env.split('\n')[-1]
        import ast
        self.myenv = ast.literal_eval(env)
        self.ddm = DDM()

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
            fsum = self.ddm.adler32(src)
            scope = 'user.ruslan'
            rse = 'RRC-KI-T1_SCRATCHDISK'
        else:
            self.ddm.fail(212, "%s: File doesn't exist" % src)

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('rucio add-dataset %s:%s' % (scope, src))

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('rucio upload --rse %s --scope %s --files %s' % (rse, scope, src))
        #self.ddm.log('upload out: ' + out)

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('rucio get-metadata %s:%s' % (scope, fname))
        #self.ddm.log('metadata out: ' + out)

        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('rucio add-files-to-dataset --to %s:%s %s:%s' % (scope, dataset, scope, fname))
        #self.ddm.log('add-to-dataset out: ' + out)