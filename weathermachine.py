""" katapult.py - an application to 'throw' archival material into the cloud """

from __future__ import print_function
import os
import json
import datetime
import time
import sys
import csv
import argparse
import socket
from struct import unpack
from functools import wraps
import httplib2
from apiclient import discovery
from apiclient.http import MediaFileUpload
from apiclient import errors
import oauth2client
from oauth2client import client
from oauth2client import tools

# Load the logfile
LOG_FILE = open('upload_logs.dat', 'a')

# Setup the command-line options
FLAGS = argparse.ArgumentParser(
    parents=[tools.argparser],
    description='Upload files to Google Drive archive.')
FLAGS.add_argument(
    '-r',
    '--root_dir',
    type=str,
    nargs=1,
    help='Path to root directory containing all files to be uploaded.')
FLAGS.add_argument(
    '-v',
    '--check_validity',
    type=str,
    nargs=1,
    help='Google Drive folder ID to check against root directory for upload validity.')
FLAGS.add_argument(
    '-t',
    '--metadata_from_title',
    type=str,
    nargs=1,
    help='Add metadata to google file from its file name'
)
ARGS = FLAGS.parse_args()

# Populate the CLIENT_SECRET_FILE using non-sensitive data from auth.json
# and with sensitive data taken from environment variables
with open('auth.json', 'r') as auth:
    JSON_STRING = auth.read()
PARSED_JSON = json.loads(JSON_STRING)
PARSED_JSON['installed']['client_id'] = os.environ.get('KATAPULT_CLIENT_ID')
PARSED_JSON['installed']['client_secret'] = os.environ.get('KATAPULT_CLIENT_SECRET')
SECRET_JSON_STRING = json.dumps(PARSED_JSON, sort_keys=True, separators=(',', ':'))
with open('secret.json', 'w') as secret:
    secret.write(SECRET_JSON_STRING)

# If modifying these scopes, delete your previously saved credentials
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'secret.json'
APPLICATION_NAME = 'Katapult'

def log(msg):
    """Logs a timestamp and a message to the logfile."""
    stamp = datetime.datetime.now()
    LOG_FILE.write((str(stamp) + ': ' + msg + '\n').encode('utf8'))

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
        if FLAGS:
            credentials = tools.run_flow(flow, store, ARGS)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

# Retry decorator with exponential backoff
def retry(exception_to_check, tries=4, delay=3, backoff=2):
    '''Retries a function or method until it returns True.'''

    def deco_retry(func):
        '''decorator'''
        @wraps(func)
        def f_retry(*args, **kwargs):
            '''main function'''
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return func(*args, **kwargs)
                except exception_to_check, text:
                    msg = "%s, Retrying in %d seconds..." % (str(text), mdelay)
                    log(msg)
                    print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return func(*args, **kwargs)
        return f_retry  # true decorator
    return deco_retry

@retry((errors.HttpError, socket.error), tries=10)
def edit_gfile(service, file_id, data):
    '''Changes metadata of remote file.'''
    try:
        file = {'description':data}
        # Add metadata as description to the file.
        updated_file = service.files().patch(
            fileId=file_id,
            body=file,
            fields='description').execute()
        return updated_file
    except errors.HttpError, error:
        log('An error occurred while adding metada to file: %s' % error)
        return None

@retry((errors.HttpError, socket.error), tries=10)
def filename_to_metadata(service, folder_id):
    page_token = None
    while True:
        param = {'maxResults': 1000}
        if page_token:
            param['pageToken'] = page_token
        children = service.files().list(q="'%s' in parents" % folder_id, **param).execute()
        for child in children.get('items', []):
            filename = child['title']
            if filename[0].isdigit() and filename[1].isdigit() and filename[2] == ".":
                data = "Date: 19"+filename[0]+filename[1]
                log('Adding metadata to file: %s' % filename)
                edit_gfile(service, child['id'], data)
            filename_to_metadata(service, child['id'])
        page_token = children.get('nextPageToken')
        if not page_token:
            return None

@retry((errors.HttpError, socket.error), tries=10)
def check_filename_metadata(service, folder_id):
    page_token = None
    while True:
        param = {'maxResults': 1000}
        if page_token:
            param['pageToken'] = page_token
        children = service.files().list(q="'%s' in parents" % folder_id, **param).execute()
        for child in children.get('items', []):
            if not child['mimeType'] == "application/vnd.google-apps.folder":
                filename = child['title']
                metadata = child['description']
                if filename[0].isdigit() and filename[1].isdigit() and filename[2] == ".":
                    if not filename[:2] == metadata[-2:]:
                        log('File has wrong/missing metadata: %s' % filename)
                    else:
                        log('File double checked for metadata: %s' % filename)
            check_filename_metadata(service, child['id'])
        page_token = children.get('nextPageToken')
        if not page_token:
            return None

def loop_local(root_dir_path, total=0):
    """ Generate a map of the local directory structure """
    result = []
    for root, dirs, files in os.walk(root_dir_path):
        for name in files:
            if not name.startswith('.'):
                result.append(name)
                total += 1
        for name in dirs:
            if not name.startswith('.'):
                result.append(name)
                total += 1
    return result, total

@retry((errors.HttpError, socket.error), tries=10)
def loop_drive(service, folder_id, result, total=0):
    """ Generates a map of the google drive folder structure """
    page_token = None
    while True:
        param = {'maxResults': 1000}
        if page_token:
            param['pageToken'] = page_token
        children = service.files().list(q="'%s' in parents" % folder_id, **param).execute()
        total += len(children.get('items', []))
        for child in children.get('items', []):
            result.append(child['title'])
            result, total = loop_drive(service, child['id'], result, total)
        page_token = children.get('nextPageToken')
        if not page_token:
            return result, total

def main():
    """Main Function

    """
    global FOLDER_COLORS
    global COLOR_MAP
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v2', http=http)
    log("Authentication success.")

    # if no arguments, show the help and exit
    if len(sys.argv) == 1:
        FLAGS.print_help()
        sys.exit(1)

    if ARGS.root_dir and ARGS.check_validity:
        diff_items = []
        root_dir_name = os.path.split(ARGS.root_dir[0])[1]
        remote_dir = ARGS.check_validity[0]
        remote_result, remote_total = loop_drive(service, remote_dir, [], 0)
        local_result, local_total = loop_local(ARGS.root_dir[0], 0)
        if remote_total == local_total:
            print("Total of %d files match" % remote_total)
        elif local_total > remote_total:
            diff = local_total - remote_total
            print("Missing files: %d files found locally missing from remote" % diff)
            for item in local_result:
                if item not in remote_result:
                    diff_items.append(item)
            for item in diff_items:
                print(item)
        elif remote_total > local_total:
            diff = remote_total - local_total
            print("Missing files: %d files found remotely missing from local machine" % diff)
            for item in remote_result:
                if item not in remote_result:
                    diff_items.append(item)
            for item in diff_items:
                print(item)
        sys.exit(0)

    if ARGS.metadata_from_title:
        filename_to_metadata(service, ARGS.metadata_from_title[0])
        check_filename_metadata(service, ARGS.metadata_from_title[0])

if __name__ == '__main__':
    main()
