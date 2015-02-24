import dropbox
from plugins.DDM import SEPlugin

class DropboxSEPlugin(SEPlugin):
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