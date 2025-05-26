import os 
import json
from google.oauth2 import service_account
import google.auth.transport.requests
import googleapiclient.discovery 


class ServiceAccountHandler():
    def __init__(self,admin_service_account):
        self.admin_service_account = admin_service_account
        self.project_id = admin_service_account['project_id']
        self.credentials = self.getCredentials()
        self.service = googleapiclient.discovery.build('iam', 'v1', credentials=self.credentials)

    def getCredentials(self):
        credentials = service_account.Credentials.from_service_account_info(self.admin_service_account,
                                                                            scopes=['https://www.googleapis.com/auth/cloud-platform'])
        request = google.auth.transport.requests.Request()
        credentials.refresh(request)
        return credentials
    
    def createServiceAccount(self,service_account_name):
        new_service_account = (self.service.projects().serviceAccounts().create(name=f"projects/{self.project_id}",
                                                                           body={"accountId": service_account_name,
                                                                                "serviceAccount": {"displayName": "Datagem Service Account - "+service_account_name}}).execute())
        return new_service_account
    
    def deleteServiceAccount(self,service_account_name):
        service_account_email = f"{service_account_name}@{self.project_id}.iam.gserviceaccount.com"
        name = f"projects/-/serviceAccounts/{service_account_email}"
        service_account = self.service.projects().serviceAccounts().delete(name=name).execute()
        return service_account

    def getServiceAccounts(self):
        service_accounts = self.service.projects().serviceAccounts().list(name=f"projects/{self.project_id}").execute()
        return service_accounts

    def createKeyServiceAccount(self,service_account_name):
        service_account_email = f"{service_account_name}@{self.project_id}.iam.gserviceaccount.com"
        name = f"projects/-/serviceAccounts/{service_account_email}"
        key = self.service.projects().serviceAccounts().keys().create(name=name,body={}).execute()
        return key

    def deleteKeyServiceAccount(self,service_account_name,key_id):
        service_account_email = f"{service_account_name}@{self.project_id}.iam.gserviceaccount.com"
        name = f"projects/-/serviceAccounts/{service_account_email}/keys/{key_id}"
        key = self.service.projects().serviceAccounts().keys().delete(name=name,keyId=key_id).execute()
        return key

    def getServiceAccountKey(self,service_account_name,key_id):
        service_account_email = f"{service_account_name}@{self.project_id}.iam.gserviceaccount.com"
        name = f"projects/-/serviceAccounts/{service_account_email}/keys/{key_id}"
        key = self.service.projects().serviceAccounts().keys().get(name=name,keyId=key_id).execute()
        return key
    
    def getServiceAccountKeys(self,service_account_name):
        service_account_email = f"{service_account_name}@{self.project_id}.iam.gserviceaccount.com"
        name = f"projects/-/serviceAccounts/{service_account_email}"
        keys = self.service.projects().serviceAccounts().keys().list(name=name).execute()
        return keys
