import os
import time
import sys
import subprocess

import dropbox


BIN_HOME='/srv/lsm/rrcki-sendjob'
LOGFILE='/srv/lsm/log/ddm.log'
SITE_PREFIX = 'srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN='
SITE_DATA_HOME = '/t1.grid.kiae.ru/data/atlas/atlasscratchdisk/rucio'


def log(self, msg):
        try:
            f = open(LOGFILE, 'a')
            f.write("%s %s\n" % (time.ctime(), msg))
            f.close()
            os.chmod(LOGFILE, 0666)
        except:
            pass

def fail(self, errorcode=200, msg=None):
    if msg:
        msg='%s %s'%(errorcode, msg)
    else:
        msg=str(errorcode)
    print msg
    log(msg)
    sys.exit(errorcode)

def getSURL(self, scope, lfn):
        #get full surl
        # /<site_prefix>/<space_token>/rucio/<scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>
        try:
            # for python 2.6
            import hashlib
            hash = hashlib.md5()
        except:
            # for python 2.4
            import md5
            hash = md5.new()

        correctedscope = "/".join(scope.split('.'))
        hash.update('%s:%s' % (scope, lfn))
        hash_hex = hash.hexdigest()[:6]
        return '%s%s/%s/%s/%s/%s' % (SITE_PREFIX, SITE_DATA_HOME, correctedscope, hash_hex[0:2], hash_hex[2:4], lfn)

class UserSE:
    def __init__(self):
        print 'UserSE initialization'
        self.client = self.getClient()

    def getClient(self):
        # Get your app key and secret from the Dropbox developer website
        app_key = 'APP_KEY'
        app_secret = 'APP_SECRET'

        flow = dropbox.client.DropboxOAuth2FlowNoRedirect(app_key, app_secret)

        # Have the user sign in and authorize this token
        authorize_url = flow.start()
        print '1. Go to: ' + authorize_url
        print '2. Click "Allow" (you might have to log in first)'
        print '3. Copy the authorization code.'
        code = raw_input("Enter the authorization code here: ").strip()

        # This will fail if the user enters an invalid authorization code
        access_token, user_id = flow.finish(code)
        return dropbox.client.DropboxClient(access_token)

    def get(self, src, dest):
        #get file from dropbox to local se
        out = open(dest, 'wb')
        f, metadata = self.client.get_file_and_metadata(src)
        with f:
            out.write(f.read())
        print metadata

    def put(self, src, dest):
        #put file from local se to dropbox
        f = open(src, 'rb')
        response = self.client.put_file(dest, f)
        print 'uploaded: ', response


class GridSE:
    def __init__(self):
        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        env = proc.communicate(". %s/setup.sh; python -c 'import os; print os.environ'" % BIN_HOME)[0][:-1]
        env = env.split('\n')[-1]
        import ast
        self.myenv = ast.literal_eval(env)

    def get(self, src, dest):
        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('python %s/utils/get.py %s %s' % (BIN_HOME, src, dest))
        #print out

    def put(self, src, dest):
        #os.system('utils/put.py %s %s' % (src, dest))
        proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=self.myenv)
        out = proc.communicate('python %s/utils/put.py %s %s' % (BIN_HOME, src, dest))
        #print out