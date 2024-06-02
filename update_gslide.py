import glob
import os.path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from gdrive_util import SCOPES, upload_file, is_folder_accessible
from thread_pool_worker import WorkerPool
from update_gslide_util import GslideUtil

credentials = service_account.Credentials.from_service_account_file("./key/key.json")
creds = credentials.with_scopes(SCOPES)
slides_service = build('slides', 'v1', credentials=creds)

class UpdateGslide():
    def __init__(self):
        self.images_gdrive_folder_id = "1iVu_IHsyUJuAxu69djB4BdRg2marP0T8"
        self.presentation_id = "1T_1giXV5MO6COGHmdVdubExm3PDA6wq4TVe0U_qUYpE"
        self.presentation_url = f"https://docs.google.com/presentation/d/{self.presentation_id}/edit#slide=id.g2e205ea646f_0_38"
        print(self.presentation_url)
        self.presentation = self.slides = None
        self.update_presentation_information()

        # for upserting image
        assert is_folder_accessible(self.images_gdrive_folder_id)[0], f"Public should be able to read GDrive folder {self.images_gdrive_folder_id}"


    def update_presentation_information(self):
        self.presentation = slides_service.presentations().get(presentationId=self.presentation_id).execute()
        self.slides = self.presentation.get('slides')

    def upsert_all_in_folder(self, folder_path):
        workers = WorkerPool()
        workers.start_workers(1)

        for file in glob.glob(f'{folder_path}/*.*'):
            basename = os.path.basename(file)
            def task(file, basename):
                print(f"Upserting image {file}")
                self.upsert_image_from_local(file, gdrive_new_file_name=basename, metadata_id_to_replace=basename)
            workers.add_task((lambda file, basename: lambda: task(file, basename))(file, basename))
        workers.stop_workers()


    def upsert_image_from_local(self, local_file_name, slide_object_id=None, gdrive_new_file_name=None, object_id_to_replace=None, metadata_id_to_replace=None):
        if gdrive_new_file_name is None:
            gdrive_new_file_name = local_file_name

        response = upload_file(self.images_gdrive_folder_id, local_file_name, gdrive_new_file_name, overwrite=True)
        download_link = response.get('webContentLink')
        ret = self.upsert_image(download_link, slide_object_id=slide_object_id, object_id_to_replace=object_id_to_replace, metadata_id_to_replace=metadata_id_to_replace)
        return ret


    def upsert_image(self, image_url, slide_object_id=None, object_id_to_replace=None, metadata_id_to_replace=None):
        if slide_object_id is None:
            slide_object_id = self.slides[-1].get('objectId')
        requests = []
        ret = None
        if object_id_to_replace is not None and GslideUtil.get_object(self.slides, object_id_to_replace) is not None:
            requests.append(GslideUtil.replace_image(object_id_to_replace, image_url))
        elif metadata_id_to_replace is not None and GslideUtil.get_object(self.slides, metadata_id_to_replace, 'title') is not None:
            object_id_to_replace = GslideUtil.get_object(self.slides, metadata_id_to_replace, 'title')['objectId']
            requests.append(GslideUtil.replace_image(object_id_to_replace, image_url))
        else:
            requests.append(GslideUtil.create_image(slide_object_id, image_url))
        response = slides_service.presentations().batchUpdate(
            presentationId=self.presentation_id,
            body={'requests': requests}
        ).execute()
        try:
            object_id_to_replace = next(iter(response['replies'][0].values()))['objectId']
        except:
            pass
        if metadata_id_to_replace is not None:
            slides_service.presentations().batchUpdate(presentationId=self.presentation_id,
               body={'requests': [GslideUtil.updatePageElementAltText(object_id_to_replace, metadata=metadata_id_to_replace)]}
               ).execute()
        return object_id_to_replace


