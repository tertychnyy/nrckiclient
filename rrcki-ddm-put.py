import dropbox

def send(type, src, dest):
    if type=='db':
        # Get your app key and secret from the Dropbox developer website
        app_key = 'INSERT_APP_KEY'
        app_secret = 'INSERT_APP_SECRET'

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

        f = open(src, 'rb')
        response = client.put_file(dest, f)
        print 'uploaded: ', response

        folder_metadata = client.metadata('/')
        print 'metadata: ', folder_metadata

        f, metadata = client.get_file_and_metadata(dest)
        #out = open('magnum-opus.txt', 'wb')
        #out.write(f.read())
        #out.close()
        print metadata