from __future__ import print_function
import httplib2
import os
import json
import datetime
import sys

from apiclient import discovery
from apiclient.http import MediaFileUpload
from apiclient import errors
import oauth2client
from oauth2client import client
from oauth2client import tools

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# Populate the CLIENT_SECRET_FILE using non-sensitive data from auth.json
# and with sensitive data taken from environment variables
with open('auth.json', 'r') as auth:
    json_string = auth.read()
parsed_json = json.loads(json_string)
parsed_json['installed']['client_id'] = os.environ.get('KATAPULT_CLIENT_ID')
parsed_json['installed']['client_secret'] = os.environ.get('KATAPULT_CLIENT_SECRET')
secret_json_string = json.dumps(parsed_json, sort_keys=True, separators=(',',':'))
with open('secret.json', 'w') as secret:
    secret.write(secret_json_string)

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'

# Dictionary for folder names and ids
DIR = {"files":"0Byn7eiAVCHMNaGZaaGcybGYwdDA"}


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
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def open_logfile():
    global LOG_FILE
    LOG_FILE = open('upload_logs.dat', 'a')

def log(msg):
    stamp = datetime.datetime.now()
    LOG_FILE.write((str(stamp) + ': ' + msg + '\n').encode('utf8'))

def uploadFile(service,file,parent_id):
    """Uploads a file.

    """
    media = MediaFileUpload(file,resumable=True)
    head, tail = os.path.split(file)
    file_metadata =  {'title': tail }
    if parent_id:
        file_metadata['parents'] = [{'id':parent_id}]
    try:
        file_uploaded = service.files().insert(body=file_metadata,media_body=media).execute()
        log('Success: uploaded file %s' % file_uploaded.get('title'))
    except errors.HttpError, error:
        log('Upload failed: %s' % error)
        sys.exit('Error: %s' % error)

def createDir(service, dirName):
    """Creates a directory on google drive and returns its id

    """
    head, tail = os.path.split(dirName)
    print("Head: %s" % head)
    print("Tail: %s" % tail)
    parent_id = DIR[head]
    file_metadata = {
        'title' : tail,
        'mimeType' : 'application/vnd.google-apps.folder'
    }
    if parent_id:
        file_metadata['parents'] = [{'id':parent_id}]
    try:
        folder = service.files().insert(body=file_metadata,fields='id').execute()
        log('Success: created a directory %s' % folder.get('title'))
        return folder.get('id')
    except errors.HttpError, error:
        log('Directory Creation failed: %s' % error)
        sys.exit('Error: %s' % error)


def getDirID(service, dirName):
    """Checks if a directory id exists, if not creates a directory and returns its id

    """
    if dirName in DIR:
        return DIR[dirName]
    else:
        id = createDir(service, dirName)
        DIR[dirName] = id
        return id

def uploadDir(service,root_dir):
    """Traverse through a given root_directory

    """
    for dirName, subdirList, fileList in os.walk(root_dir):
        id = getDirID(service, dirName)
        for fname in fileList:
            print("\t%s" % fname)
            file_path = dirName+"/"+fname
            uploadFile(service, file_path, id)

def main():
    """Shows basic usage of the Google Drive API.

    Creates a Google Drive API service object and outputs the names and IDs
    for up to 10 files.
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v2', http=http)

    open_logfile()

    uploadDir(service, "files")

if __name__ == '__main__':
    main()
