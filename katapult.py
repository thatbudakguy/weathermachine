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
    '-m',
    '--metadata',
    type=str,
    nargs=1,
    help='Path to comma-separated file of metadata to apply to uploaded files.')
FLAGS.add_argument(
    '-f',
    '--count_files',
    type=str,
    nargs=1,
    help='ID of a Google Drive folder to count total containing files.')
FLAGS.add_argument(
    '-c',
    '--color_map',
    type=str,
    nargs=1,
    help='Path to a JSON file mapping OSX label colors to Drive folder colors.')
FLAGS.add_argument(
    '-d',
    '--date_file',
    action='store_true',
    help='Flag to parse date and other metadata values from file name, if exists,')
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
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'secret.json'
APPLICATION_NAME = 'Katapult'

# Global variables
DIR = {}
METADATA = {}
TOTALFILES = 0.0
UPLOADEDFILES = 0.0
COLORNAMES = {
    1: 'gray',
    2: 'green',
    3: 'purple',
    4: 'blue',
    5: 'yellow',
    6: 'red',
    7: 'orange'
}
COLOR_MAP = {}
FOLDER_COLORS = False

# Load the logfile
LOG_FILE = open('upload_logs.dat', 'a')

# Determine the OS
if sys.platform == "darwin":
    from xattr import xattr
    FOLDER_COLORS = True

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
def get_file_id(service, file_name, parent_id=None):
    """Checks if a file exists in a given parent directory.
    Returns:
        the id of the file or None
    """
    page_token = False
    while True:
        param = {'maxResults': 1000}
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
    page_token = None
    while True:
        param = {'maxResults': 1000}
        if page_token:
            param['pageToken'] = page_token
        children = service.files().list(q="'%s' in parents" % parent_id, **param).execute()
        counter += len(children.get('items', []))
        page_token = children.get('nextPageToken')
        sys.stdout.write("\r")
        sys.stdout.write('%d files counted' % counter)
        sys.stdout.flush()
        if not page_token:
            sys.stdout.write("\r")
            sys.stdout.write('Number of files in folder: %d \n' % counter)
            sys.stdout.flush()
            break

def file_name_to_date(file_name):
    """Gets metadata for 2-digit dates from filenames """
    parts = file_name.split(".")
    file_metadata = "Date: 19"+parts[0]
    return file_metadata

@retry((errors.HttpError, socket.error), tries=10)
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
    if not get_file_id(service, file_name, parent_id):
        media = MediaFileUpload(input_file, resumable=True)
        file_metadata = {'title': file_name}
        if METADATA:
            # fix an issue with "0X" for months prior to October
            split = file_name.split("_")
            if len(split[1]) == 2 and split[1][0] == "0":
                split[1] = split[1][1:]
            month_name = "_".join(split)
            try:
                csv_metadata = METADATA[os.path.splitext(file_name)[0]]
                file_metadata['description'] = "Date: " + csv_metadata[0] + \
                "\n\nTitle: " + csv_metadata[1] + "\n\nDescription: " + \
                csv_metadata[2]
            except KeyError:
                try:
                    csv_metadata = METADATA[os.path.splitext(month_name)[0]]
                    file_metadata['description'] = "Date: " + csv_metadata[0] + \
                    "\n\nTitle: " + csv_metadata[1] + "\n\nDescription: " + \
                    csv_metadata[2]
                except KeyError:
                    log("Didn't find metadata for file %s, uploading anyway" \
                    % file_name)
                    print("Didn't find metadata for file %s, uploading anyway" \
                    % file_name)
        if ARGS.date_file:
            file_metadata['description'] = file_name_to_date(file_name)
        if parent_id:
            file_metadata['parents'] = [{'id':parent_id}]
        # do the upload
        do_file_upload(service, file_metadata, media)

@retry((errors.HttpError, socket.error), tries=10)
def create_dir(service, dir_name, parent_id=None, color=None):
    """Creates a directory on google drive and returns its id

    """
    file_metadata = {
        'title' : dir_name,
        'mimeType' : 'application/vnd.google-apps.folder'
    }
    if parent_id:
        file_metadata['parents'] = [{'id':parent_id}]
    if color:
        file_metadata['folderColorRgb'] = COLOR_MAP[color]
    folder = service.files().insert(body=file_metadata, fields='id').execute()
    log('Success: created a directory %s' % dir_name)
    print('created directory: %s' % dir_name)
    return folder.get('id')

def get_dir_color(dir_path):
    """Checks the label color of a local folder on an OSX machine."""
    if not FOLDER_COLORS:
        return None
    attrs = xattr(dir_path)
    try:
        finder_attrs = attrs[u'com.apple.FinderInfo']
        flags = unpack(32*'B', finder_attrs)
        color = flags[9] >> 1 & 7
        return COLORNAMES[color]
    except KeyError:
        return None

def get_dir_id(service, dir_name, color):
    """Checks if a directory id exists, if not creates a directory and returns its id

    """
    if dir_name in DIR:
        return DIR[dir_name]
    else:
        head, tail = os.path.split(dir_name)
        if head:
            parent_id = DIR[head]
            dir_id = create_dir(service, tail, parent_id, color)
        else:
            dir_id = create_dir(service, tail, None, color)
        log_dir(dir_name, dir_id)
        return dir_id

def upload_dir(service, root_dir_path):
    """Traverse through a given root_directory"""
    for dir_path, sub_dir_list, file_list in os.walk(root_dir_path):
        dir_name = os.path.split(dir_path)[1]
        dir_id = get_dir_id(service, dir_name, get_dir_color(dir_path))
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
    log("Authentication success.")

    # if no arguments, show the help and exit
    if len(sys.argv) == 1:
        FLAGS.print_help()
        sys.exit(1)

    if ARGS.color_map:
        if FOLDER_COLORS:
            color_map_json = open(ARGS.color_map[0]).read()
            COLOR_MAP = json.loads(color_map_json)
        else:
            print("Error: host OS is not OSX; aborting colormap")
            sys.exit(1)

    if ARGS.count_files:
        count_files(service, ARGS.count_files[0])
        sys.exit(0)

    if ARGS.metadata:
        create_meta_dict(clean_csv(read_csv(ARGS.metadata[0])))
        for key, value in METADATA.items():
            newkey = key.replace('.', '_')
            METADATA[newkey] = METADATA[key]
            del METADATA[key]

    if ARGS.root_dir and not ARGS.check_validity:
        import_dir()
        log("beginning upload using root %s" % ARGS.root_dir[0])
        root_dir = os.path.split(ARGS.root_dir[0])[1]
        root_id = get_dir_id(service, root_dir, get_dir_color(ARGS.root_dir[0]))
        log_dir(root_dir, root_id)
        upload_progress(ARGS.root_dir[0])
        upload_dir(service, ARGS.root_dir[0])
        export_dir()

    elif ARGS.root_dir and ARGS.check_validity:
        diff_items = []
        root_dir_name = os.path.split(ARGS.root_dir[0])[1]
        remote_dir = ARGS.check_validity[0]
        log("comparing local directory %s to remote folder id %s" % (root_dir_name, remote_dir))
        remote_result, remote_total = loop_drive(service, remote_dir, [], 0)
        local_result, local_total = loop_local(ARGS.root_dir[0], 0)
        if remote_total == local_total:
            print("Total number of files match.")
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
