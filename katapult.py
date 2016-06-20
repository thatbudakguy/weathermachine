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
    required=True,
    help='Root directory containing all files to be uploaded.')
FLAGS.add_argument(
    '-m',
    '--metadata',
    type=str,
    nargs=1,
    required=False,
    help='Comma-separated file of metadata to apply to uploaded files.')
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
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'secret.json'
APPLICATION_NAME = 'Katapult'

# Dictionaries for folder names and ids, metadata, uploaded files
DIR = {}
METADATA = {}
TOTALFILES = 0.0
UPLOADEDFILES = 0.0

# Load the logfile
LOG_FILE = open('upload_logs.dat', 'a')

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

def log(msg):
    """Logs a timestamp and a message to the logfile."""
    stamp = datetime.datetime.now()
    LOG_FILE.write((str(stamp) + ': ' + msg + '\n').encode('utf8'))

def log_dir(dir_name, dir_id):
    """Logs a created directory and its id to the DIR registry file."""
    DIR[dir_name] = dir_id

def read_csv(input_file):
    """Reads a .csv format file.
    Returns:
        input_data, the csv data as a list of lines.
    """
    input_data = []
    if input_file[-4:] != '.csv':
        sys.exit("Input file must be in .csv format.")
    with open(input_file, 'rU') as file_contents:
        reader = csv.reader(file_contents, delimiter=',')
        input_data = [r for r in reader]
    return input_data

def clean_csv(rows):
    """Takes a list of lines from read_csv() and removes lines that:
        - have less than two elements
        - begin with an empty element
    Returns:
        rows, a list of lines in the same format as the input
    """
    for row in rows:
        if row[0] == '' or len(row) <= 1:
            rows.remove(row)
    return rows

def create_meta_dict(metadata):
    """Takes a list of lines from read_csv() or clean_csv() and creates a dict,
    using the first element of a line as a key and all following elements of
    the line as the corresponding value.
    """
    for line in metadata:
        METADATA[line[0]] = line[1:]

# Retry decorator with exponential backoff
def retry(ExceptionToCheck, tries=4, delay=3, backoff=2):
    '''Retries a function or method until it returns True.'''

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck, e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    log(msg)
                    print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry  # true decorator
    return deco_retry

@retry((errors.HttpError, socket.error), tries=5)
def get_file_id(service, file_name, parent_id=None):
    """Checks if a file exists in a given parent directory.
    Returns:
        the id of the file or None
    """
    page_token = False
    while True:
        param = {}
        if page_token:
            param['pageToken'] = page_token
        children = service.files().list(q="'%s' in parents" % parent_id, **param).execute()
        for child in children.get('items', []):
            if child['title'] == file_name:
                log('Found existing file: %s' % child['title'])
                return child['id']
        page_token = children.get('nextPageToken')
        if not page_token:
            return False

def count_files(service, parent_id):
    """Counts number of files in a folder on google drive
    """
    counter = 0
    result = []
    page_token = None
    while True:
        param = {'maxResults': 1000}
        if page_token:
            param['pageToken'] = page_token
        children = service.files().list(q="'%s' in parents" % parent_id, **param).execute()
        counter += len(children.get('items', []))
        page_token = children.get('nextPageToken')
        if not page_token:
            print('Number of files in folder: %d' % counter)
            break

@retry((errors.HttpError, socket.error), tries=5)
def do_file_upload(service, file_metadata, media):
    """Uses the API to do the file upload, handling errors."""
    global UPLOADEDFILES
    file_uploaded = service.files().insert(body=file_metadata, media_body=media).execute()
    UPLOADEDFILES += 1.0
    progress = str(round(((UPLOADEDFILES/TOTALFILES)*100), 4)) + " % processed"
    sys.stdout.write("\r")
    sys.stdout.write(progress)
    sys.stdout.write("\t")
    sys.stdout.write('Uploaded file: %s' % file_uploaded.get('title'))
    sys.stdout.flush()
    log('Success: uploaded file %s' % file_uploaded.get('title'))

def upload_file(service, input_file, parent_id):
    """Uploads a file if it does not yet exist.

    """
    file_name = os.path.split(input_file)[1]
    # fix an issue with "0X" for months prior to October
    split = file_name.split("_")
    if len(split[1]) == 2 and split[1][0] == "0":
        split[1] = split[1][1:]
    month_name = "_".join(split)
    if not get_file_id(service, file_name, parent_id):
        media = MediaFileUpload(input_file, resumable=True)
        file_metadata = {'title': file_name}
        if METADATA:
            try:
                csv_metadata = METADATA[os.path.splitext(file_name)[0]]
                file_metadata['description'] = "Date: " + csv_metadata[0] + "\n\nTitle: " + csv_metadata[1] + "\n\nDescription: " + csv_metadata[2]
            except KeyError:
                try:
                    csv_metadata = METADATA[os.path.splitext(month_name)[0]]
                    file_metadata['description'] = "Date: " + csv_metadata[0] + "\n\nTitle: " + csv_metadata[1] + "\n\nDescription: " + csv_metadata[2]
                except KeyError:
                    log("Didn't find metadata for file %s, uploading anyway" % file_name)
                    print("Didn't find metadata for file %s, uploading anyway" % file_name)
        if parent_id:
            file_metadata['parents'] = [{'id':parent_id}]
        # do the upload
        do_file_upload(service, file_metadata, media)

@retry((errors.HttpError, socket.error), tries=5)
def create_dir(service, dir_name, parent_id=None):
    """Creates a directory on google drive and returns its id

    """
    file_metadata = {
        'title' : dir_name,
        'mimeType' : 'application/vnd.google-apps.folder'
    }
    if parent_id:
        file_metadata['parents'] = [{'id':parent_id}]
    folder = service.files().insert(body=file_metadata, fields='id').execute()
    log('Success: created a directory %s' % dir_name)
    print('created directory: %s' % dir_name)
    return folder.get('id')

def get_dir_id(service, dir_name):
    """Checks if a directory id exists, if not creates a directory and returns its id

    """
    if dir_name in DIR:
        return DIR[dir_name]
    else:
        head, tail = os.path.split(dir_name)
        if head:
            parent_id = DIR[head]
            dir_id = create_dir(service, tail, parent_id)
        else:
            dir_id = create_dir(service, tail)
        log_dir(dir_name, dir_id)
        return dir_id

def upload_dir(service, root_dir_name, root_dir_path):
    """Traverse through a given root_directory"""
    for dir_path, sub_dir_list, file_list in os.walk(root_dir_path):
        dir_name = os.path.split(dir_path[:-1])[1]
        dir_id = get_dir_id(service, dir_name)
        for fname in file_list:
            if not fname.startswith('.'):
                file_path = dir_path+"/"+fname
                upload_file(service, file_path, dir_id)

def upload_progress(root_dir_path):
    """Traverse through a given root_directory and count the number of files
    to upload
    """
    global TOTALFILES
    for dir_path, sub_dir_list, file_list in os.walk(root_dir_path):
        for fname in file_list:
            if not fname.startswith('.'):
                TOTALFILES += 1.0


def export_dir():
    """Exports the DIR dictionary to a csv file

    """
    dir_file = open('dir_ids.csv', 'w')
    for dir_name, dir_id in DIR.iteritems():
        dir_file.write(dir_name + "," + dir_id + "\n")
    dir_file.close()

def import_dir():
    """Imports the DIR dictionary from a csv file, if any

    """
    if os.path.isfile('dir_ids.csv'):
        dir_csv = read_csv('dir_ids.csv')
        for line in dir_csv:
            DIR[line[0]] = line[1]

def main():
    """Main Function

    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v2', http=http)

    #count number of files in a particular folder
    # count_files(service, "0Byn7eiAVCHMNcE5Gbl82YjVoUFU")

    import_dir()

    log("Authentication success, beginning upload using root %s" % ARGS.root_dir[0])

    if ARGS.metadata[0]:
        create_meta_dict(clean_csv(read_csv(ARGS.metadata[0])))
        for key, value in METADATA.items():
            newkey = key.replace('.', '_')
            METADATA[newkey] = METADATA[key]
            del METADATA[key]

    root_dir = os.path.split(ARGS.root_dir[0][:-1])[1]
    root_id = get_dir_id(service, root_dir)
    log_dir(root_dir, root_id)
    upload_progress(ARGS.root_dir[0])
    upload_dir(service, root_dir, ARGS.root_dir[0])

    export_dir()

if __name__ == '__main__':
    main()
