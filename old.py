#!/usr/bin/env python

"""katapult.py - a command-line tool to post archival material to google drive

simple usage:
    $ python katapult.py

to see all command-line flags:
    $ python katapult.py --help

"""

from __future__ import print_function
import httplib2
import os
import json

from apiclient import discovery
from apiclient import errors
from apiclient.http import MediaFileUpload
import oauth2client
from oauth2client import client
from oauth2client import tools

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/katapult.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'secret.json'
APPLICATION_NAME = 'Katapult Archival Uploader'

# Populate the CLIENT_SECRET_FILE using non-sensitive data from auth.json
# and with sensitive data taken from environment variables
with open('auth.json', 'r') as auth:
    json_string = auth.read()
parsed_json = json.loads(json_string)
parsed_json['installed']['client_id'] = os.environ.get('KATAPULT_CLIENT_ID')
parsed_json['installed']['client_secret'] = os.environ.get('KATAPULT_CLIENT_SECRET')
secret_json_string = json.dumps(parsed_json, sort_keys=True, indent=4, separators=(',',':'))
with open('secret.json', 'w') as secret:
    secret.write(secret_json_string)

# Helpful message to display in the browser if the CLIENT_SECRET_FILE
# is missing.
MISSING_CLIENT_SECRETS_MESSAGE = """
WARNING: Please configure OAuth 2.0

To run katapult you will need to configure environment variables:
    $KATAPULT_CLIENT_ID
    $KATAPULT_CLIENT_SECRET

Please insert the corresponding information from the
Google APIs Console <https://code.google.com/apis/console>.

"""

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'katapult.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES, MISSING_CLIENT_SECRETS_MESSAGE)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

# def open_logfile():
#     if not re.match( '^/', FLAGS.logfile ):
#         FLAGS.logfile = FLAGS.destination + FLAGS.logfile
#     global LOG_FILE
#     LOG_FILE = open( FLAGS.logfile, 'w+' )
#
# def log(str):
#     stamp = str(datetime.datetime().now())
#     LOG_FILE.write( (stamp + ': ' + str + '\n').encode('utf8') )

def download_file( service, drive_file, dest_path ):
    """Download a file's content.

    Args:
      service: Drive API service instance.
      drive_file: Drive File instance.

    Returns:
      File's content if successful, None otherwise.
    """
    file_location = dest_path + drive_file['title'].replace( '/', '_' )

    if is_google_doc(drive_file):
        try:
            download_url = drive_file['exportLinks']['application/pdf']
        except KeyError:
            download_url = None
    else:
        download_url = drive_file['downloadUrl']
    if download_url:
        try:
            resp, content = service._http.request(download_url)
        except httplib2.IncompleteRead:
            # log( 'Error while reading file %s. Retrying...' % drive_file['title'].replace( '/', '_' ) )
            download_file( service, drive_file, dest_path )
            return False
        if resp.status == 200:
            try:
                target = open( file_location, 'w+' )
            except:
                # log( "Could not open file %s for writing. Please check permissions." % file_location )
                return False
            target.write( content )
            return True
        else:
            # log( 'An error occurred: %s' % resp )
            return False
    else:
        # The file doesn't have any content stored on Drive.
        return False

def insert_file(service, title, description, parent_id, mime_type, filename):
  """Insert new file.

  Args:
    service: Drive API service instance.
    title: Title of the file to insert, including the extension.
    description: Description of the file to insert.
    parent_id: Parent folder's ID.
    mime_type: MIME type of the file to insert.
    filename: Filename of the file to insert.
  Returns:
    Inserted file metadata if successful, None otherwise.
  """
  media_body = MediaFileUpload(filename, mimetype=mime_type, resumable=True)
  body = {
    'title': title,
    'description': description,
    'mimeType': mime_type
  }
  # Set the parent folder.
  if parent_id:
    body['parents'] = [{'id': parent_id}]

  try:
    file = service.files().insert(
        body=body,
        media_body=media_body).execute()

    # Uncomment the following line to print the File ID
    # print 'File ID: %s' % file['id']

    return file
  except errors.HttpError, error:
    print('An error occured: %s' % error)
    return None


def main():
    """Shows basic usage of the Google Drive API.

    Creates a Google Drive API service object and outputs the names and IDs
    for up to 10 files.
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v2', http=http)

    # open_logfile()
    # log( 'Authentication success, awaiting commands' )

    results = service.files().list(pageSize=10,fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print('{0} ({1})'.format(item['name'], item['id']))

if __name__ == '__main__':
    main()
