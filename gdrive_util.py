import io
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# Define the Google Drive API scopes and service account file path
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = "./key/key.json"

# Create credentials using the service account file
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Build the Google Drive service
service = build('drive', 'v3', credentials=credentials)

def upload_file(folder_id, source_file_path, gdrive_new_file_name, mimetype="image/png", overwrite=False):
    file_metadata = {
        'name': gdrive_new_file_name,
        'parents': [folder_id]  # ID of the folder you want to upload to
    }
    media = MediaFileUpload(source_file_path, mimetype=mimetype)
    ret = {'deleted-existing-file': None}

    if overwrite:
        query = f"name = '{gdrive_new_file_name}' and '{folder_id}' in parents and trashed = false"
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])

        # If the file exists, delete it
        if files:
            file_id = files[0].get('id')
            service.files().delete(fileId=file_id).execute()
            ret['deleted-existing-file'] = file_id

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id, webContentLink'
    ).execute()
    return ret | file


def is_folder_accessible(folder_id):
    try:
        # Retrieve the permissions for the folder
        permissions = service.permissions().list(
            fileId=folder_id,
            fields='permissions(id, type, role)'
        ).execute()

        read_access = False
        write_access = False

        # Check for public read or write access
        for permission in permissions.get('permissions', []):
            if permission['type'] == 'anyone':
                if permission['role'] in ['reader', 'commenter']:
                    read_access = True
                if permission['role'] == 'writer':
                    write_access = True

        return read_access, write_access
    except Exception as e:
        print(f"An error occurred: {e}")
        return False, False



#
# # print(upload_file("1iVu_IHsyUJuAxu69djB4BdRg2marP0T8", "./graphics/temp_glmark2_1920x1080.png", "myimage.png",
# #                   overwrite=True))
#
#
# def create_folder(folder_name, parent_folder_id=None):
#     """Create a folder in Google Drive and return its ID."""
#     folder_metadata = {
#         'name': folder_name,
#         "mimeType": "application/vnd.google-apps.folder",
#         'parents': [parent_folder_id] if parent_folder_id else []
#     }
#
#     created_folder = service.files().create(
#         body=folder_metadata,
#         fields='id'
#     ).execute()
#
#     print(f'Created Folder ID: {created_folder["id"]}')
#     return created_folder["id"]
#
# def list_folder(parent_folder_id=None, delete=False):
#     """List folders and files in Google Drive."""
#     results = service.files().list(
#         q=f"'{parent_folder_id}' in parents and trashed=false" if parent_folder_id else None,
#         pageSize=1000,
#         fields="nextPageToken, files(id, name, mimeType)"
#     ).execute()
#     items = results.get('files', [])
#
#     if not items:
#         print("No folders or files found in Google Drive.")
#     else:
#         print("Folders and files in Google Drive:")
#         for item in items:
#             print(f"Name: {item['name']}, ID: {item['id']}, Type: {item['mimeType']}")
#             if delete:
#                 delete_files(item['id'])
#
#
# def delete_files(file_or_folder_id):
#     """Delete a file or folder in Google Drive by ID."""
#     try:
#         service.files().delete(fileId=file_or_folder_id).execute()
#         print(f"Successfully deleted file/folder with ID: {file_or_folder_id}")
#     except Exception as e:
#         print(f"Error deleting file/folder with ID: {file_or_folder_id}")
#         print(f"Error details: {str(e)}")
#
# def download_file(file_id, destination_path):
#     """Download a file from Google Drive by its ID."""
#     request = service.files().get_media(fileId=file_id)
#     fh = io.FileIO(destination_path, mode='wb')
#
#     downloader = MediaIoBaseDownload(fh, request)
#
#     done = False
#     while not done:
#         status, done = downloader.next_chunk()
#         print(f"Download {int(status.progress() * 100)}%.")