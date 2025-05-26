import requests
from google.oauth2 import service_account
import google.auth.transport.requests
from google.cloud import pubsub_v1
import json 
import traceback

class PubSubHandler():
    def __init__(self,service_account_file):
        self.service_account_file = service_account_file
        self.project_id = service_account_file['project_id']
        self.credentials = self.getCredentials()
        self.ACCESS_TOKEN = self.credentials.token

    def getCredentials(self):
        credentials = service_account.Credentials.from_service_account_info(self.service_account_file,
                                                                            scopes=['https://www.googleapis.com/auth/cloud-platform'])
        request = google.auth.transport.requests.Request()
        credentials.refresh(request)
        return credentials

    def getTopics(self):
        url = f"https://pubsub.googleapis.com/v1/projects/{self.project_id}/topics"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.get(url, headers=headers)
        return response.json()

    def getTopic(self,topic_name):
        url = f"https://pubsub.googleapis.com/v1/{topic_name}"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.get(url, headers=headers)
        return response.json()

    def createTopic(self,topic_name):
        publisher = pubsub_v1.PublisherClient()
        topic_name = f'projects/{self.project_id}/topics/{topic_name}'.format(
            project_id=self.project_id,
            topic=topic_name,  
        )
        try:
            publisher.create_topic(name=topic_name)
            return True
        except Exception as e:
            print(e)
            traceback.print_exc()
            return False
        