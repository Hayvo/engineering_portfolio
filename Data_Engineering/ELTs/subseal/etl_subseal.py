import yaml
import requests
import traceback
import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader

class SubSealSubscriptionETL():
    def __init__(self, storageServiceAccountCredential, debug=False):
        self.storageServiceAccountCredential = storageServiceAccountCredential
        self.project_id = storageServiceAccountCredential['project_id']
        self.debug = debug
        credentials = self.getCredentials()
        self.API_SECRET = credentials[0]
        self.API_KEY = credentials[1]
        self.MERCHANT = credentials[2]
        self.bigQueryLoader = BigQueryLoader(self.storageServiceAccountCredential,debug=self.debug)

    def getCredentials(self):
        """Get credentials"""
        credentials = yaml.load(open(f'./src/var/login_credentials/{self.project_id}/subseal_login.yml'),Loader=yaml.FullLoader)
        return credentials['subseal']['API_SECRET'],credentials['subseal']['API_KEY'],credentials['subseal']['MERCHANT']

    def loadDataToBigQuery(self,data,BQtable,BQdataset = 'marketplace_SealSubscription',base_table = None,force_schema = False,WRITE_DISPOSITION = 'WRITE_TRUNCATE'):
        """Load data to BigQuery
        Args:
            data (list): Data to load
            BQtable (str): BigQuery table to load data to
            BQdataset (str): BigQuery dataset to load data to
            base_table (str): Base table to use
            force_schema (bool): Whether to force schema
            WRITE_DISPOSITION (str): Write disposition
        Returns:
            None
        """
        if len(data) == 0:
            return None
        try:
            self.bigQueryLoader.loadDataToBQ(data,BQdataset,BQtable,platform= 'sealsub',base_table=base_table,force_schema=force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            traceback.print_exc()

    def getSubscriptions(self):
        """Get subscriptions"""
        url = f"https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.API_SECRET}'
        }
        data = []
        page = 0
        try:
            while True:
                response = requests.get(url, headers=headers, params={'page': page})
                response = response.json()
                new_data = response['payload']['subscriptions']
                data += new_data
                page = response['payload']['page'] + 1
                if page > response['payload']['total_pages']:
                    break
            return data
        except Exception as e:
            print(f"Error getting subscriptions: {e}")
            traceback.print_exc()
            return []
        
    def getSubscriptionDetails(self,subscription_id):
        """Get subscription details"""
        url = f"https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id={subscription_id}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.API_SECRET}'
        }
        try:
            response = requests.get(url, headers=headers)
            response = response.json()
            return response['payload']
        except Exception as e:
            print(f"Error getting subscription details: {e}")
            traceback.print_exc()
            return {}