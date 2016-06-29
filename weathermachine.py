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
    print("Authentication success.")

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

if __name__ == '__main__':
    main()
