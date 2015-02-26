import subprocess
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
        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('python %s/utils/put.py -t %s %s %s' % (BIN_HOME, 'ATLASSCRATCHDISK', src, dest))
        #print out