from google.cloud import storage
from google.oauth2 import service_account
import traceback

class CloudStorageHandler:
    def __init__(self, credentials):
        self.credentials = credentials
        self.project_id = self.credentials['project_id']
        self.storage_credentials = service_account.Credentials.from_service_account_info(self.credentials)
        self.storage_client = storage.Client(project= self.project_id, credentials=self.storage_credentials)
       
    def get_stored_file(self,bucket,file_name):
        try:
            client = self.storage_client
            try:
                bucket = client.get_bucket(bucket)
                try:
                    blob = bucket.blob(file_name)
                    try:
                        with blob.open("r") as f:
                            return f.read()
                    except Exception:
                        print("get_stored_param : couldn't open blob")
                except Exception:
                    print("get_stored_param : couldn't get blob from bucket / file not found in bucket")
            except Exception:
                print("get_stored_param : couldn't find bucket")
                traceback.print_exc()
        except Exception:
            print("get_stored_param : couldn't start GCS client")

    def create_stored_file(self,bucket,file_name,data):
        try:
            client = self.storage_client
            try:
                bucket = client.get_bucket(bucket)
                try:
                    with bucket.blob(file_name).open('w') as f:
                        f.write(str(data))
                except Exception:
                    print("create_stored_param : couldn't write file")
            except Exception:
                print("create_stored_param : couldn't find bucket")
        except Exception:
            print("create_stored_param : couldn't start GCS client")

    def update_stored_file(self,bucket,file_name,data):
        try:
            client = self.storage_client
            try:
                bucket = client.get_bucket(bucket)
                try:
                    with bucket.blob(file_name).open('w') as f:
                        f.write(str(data))
                except Exception:
                    print("update_stored_param : couldn't write file")
                    print("update_stored_param : trying to create file")
                    self.create_stored_file(bucket,file_name,data)
            except Exception:
                print("update_stored_param : couldn't find bucket")
        except Exception:
            print("update_stored_param : couldn't start GCS client")