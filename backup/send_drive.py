from __future__ import print_function
import pickle
import os.path
import pkg_resources.py2_warn
from googleapiclient import errors
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive'] #.metadata.readonly']

def getCredentials():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credenciales/credentials_faqture.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)
    return service

def list_items():
    services = getCredentials()
    results = services.files().list(pageSize=5, 
                                    fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(u'{0} ({1})'.format(item['name'], item['id']))

def uploadFile(filename,filepath,mimetype):
    services = getCredentials()
    print('Backup Uploading...')
    file_metadata = {'name': filename}
    media = MediaFileUpload(filepath, 
                            mimetype = mimetype, 
                            resumable = True) #resumable=True para enviar achivos mayores a 5MB
    file = services.files().create(body = file_metadata,
                                    media_body = media,
                                    fields='id').execute()
    return print('Uploaded: %s' % file.get('id'))

def searchFile(size, query, filename):
    services = getCredentials()
    print('Serching File...')
    f_mT = 'application/x-rar-compressed'
    results = services.files().list(pageSize = size,
                                    fields = "nextPageToken, files(id)",
                                    q=query).execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')        
        uploadFile(filename, filename, f_mT)
    else:
        for item in items:            
            fileId = item['id']
        updateFile(fileId, filename, f_mT)
        return item
    
def updateFile(file_id, new_filename, new_mime_type):
    services = getCredentials()
    print('Backup Updating...')
    try:
        file = {}
        media_body = MediaFileUpload(new_filename, 
                                    mimetype=new_mime_type, 
                                    resumable=True)
        updated_file = services.files().update(fileId=file_id,                                           
                                            body=file,
                                            media_body=media_body).execute()
        return print('Updated: %s' % updated_file['id'])
    except errors.HttpError as error:
        print ('An error occurred: %s' % error) 
        return None