import os
import dropbox
import time
import sys

LOGFILE='log/ddm-get.log'

def log(msg):
    try:
        f = open(LOGFILE, 'a')
        f.write("%s %s\n" % (time.ctime(), msg))
        f.close()
        os.chmod(LOGFILE, 0666)
    except:
        pass

def fail(errorcode=200,msg=None):
    if msg:
        msg='%s %s'%(errorcode, msg)
    else:
        msg=str(errorcode)
    print msg
    log(msg)
    sys.exit(errorcode)

def get(type, src, dest):
    if type == 'db':
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

        client = dropbox.client.DropboxClient(access_token)
        print 'linked account: ', client.account_info()

        out = open(dest, 'wb')
        f, metadata = client.get_file_and_metadata(src)
        with f:
            out.write(f.read())
        print metadata

log(' '.join(sys.argv))

args = sys.argv[1:]

if len(args) != 3:
    fail(202, "Invalid command")
    sys.exit(1)

type, src, dest = args

get(type, src, dest)