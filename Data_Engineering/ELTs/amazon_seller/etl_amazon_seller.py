import sys
import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
import requests
import yaml 
import json
from datetime import date,timedelta
import traceback
import gzip 
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryHandler
from Utils.GoogleCloudHandlers.cloud_storage_handler import CloudStorageHandler
import time

class AmazonSellerETL():
    def __init__(self,storageServiceAccountCredential,amazonSellerCredentials = None,debug = False):
        self.storageServiceAccountCredential = storageServiceAccountCredential
        self.cloudStorageHandler = CloudStorageHandler(storageServiceAccountCredential)
        self.project_id = storageServiceAccountCredential['project_id']
        self.bigQueryLoader = BigQueryHandler(adminServiceAccount= storageServiceAccountCredential,debug = debug)
        
        if amazonSellerCredentials is None:
            self.amazonSellerCrendentials = yaml.load(self.cloudStorageHandler.get_stored_file('amazon_stored_params','seller_login.yml'),Loader=yaml.FullLoader)
        # print(self.amazonSellerCrendentials)
        self.client_id = self.amazonSellerCrendentials['amazon_seller']['CLIENT_ID']
        self.client_secret = self.amazonSellerCrendentials['amazon_seller']['CLIENT_SECRET']
        self.refresh_token = self.amazonSellerCrendentials['amazon_seller']['REFRESH_TOKEN']
        self.token = self.amazonSellerCrendentials['amazon_seller']['ACCESS_TOKEN']
        self.MARKETPLACE_IDS = json.loads(json.dumps(self.amazonSellerCrendentials['amazon_seller']['MARKETPLACE_IDS']))
        self.reportQueries = []

    def updateToken(self,redirect_uri):
        url = "https://api.amazon.com/auth/o2/token"
        params = {'grant_type' : 'refresh_token',
                    'refresh_token' : self.refresh_token,
                    'client_id' : self.client_id,
                    'client_secret' : self.client_secret,
                    'redirect_uri' : redirect_uri}
        try:
            print('Refreshing token...')
            response = requests.post(url, params=params)
            expires_in = response.json()['expires_in']
            print('New token expiration in seconds: ', expires_in)
            self.token = response.json()['access_token']
            self.refresh_token = response.json()['refresh_token']
   
            self.amazonSellerCrendentials['amazon_seller']['REFRESH_TOKEN'] = self.refresh_token
           
            self.cloudStorageHandler.update_stored_file('amazon_stored_params','seller_login.yml',yaml.dump(self.amazonSellerCrendentials))
        except Exception as e:
            print('Error refreshing token')
            traceback.print_exc()
      
    def get_report_status(self,reportId):
       
        url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{reportId}"
        headers = {"x-amz-access-token": self.token,
                "content-type": "application/json"} 
        response = requests.get(url, headers=headers).json()
    
        if response['processingStatus'] == 'DONE':
           
            return "DONE",response['reportDocumentId']
        else:
            return response['processingStatus'],None
        
    def get_report_data(self,reportDocumentId):
        # update_token()
        try:
            print("Getting url")
            url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{reportDocumentId}"
            headers = {"x-amz-access-token": self.token,
                    "content-type": "application/json"} 
            response = requests.get(url, headers=headers).json()
          
            downloadUrl = response['url']
         
            try:
                print("Getting report")
                response = requests.get(downloadUrl,stream=True)
                content = response.content
                try:
                    print("Decompressing report")
                    content = gzip.decompress(content).decode('cp1252')
                    lines = content.strip().split("\n")
                    headers = lines[0].split("\t")
                    data_rows = [line.split("\t") for line in lines[1:]]
                    json_data = [dict(zip(headers, row)) for row in data_rows]
                    return json_data
                except Exception as e:
                    print("Couldn't decompress report")
                 
                    traceback.print_exc()
            except:
                print("Couldn't fetch report")
                
                traceback.print_exc()
        except Exception as e:
            print("Couldn't fetch url")
            print(response)
            traceback.print_exc()
        return []
    
    def get_report(self,report):
       
        url = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports"
        headers = {"x-amz-access-token": self.token,
                "content-type": "application/json"} 
        reportPayload = report['params']
        print(f"Initiate {reportPayload['reportType']} report for {reportPayload['dataStartTime']} to {reportPayload['dataEndTime']}")
        try:
            response = requests.post(url, headers=headers, json=reportPayload).json()
         
            reportId = response['reportId']
            while True:
                try:
                    reportStatus,reportDocumentId = self.get_report_status(reportId)
                    if reportStatus == 'DONE':
                        try:
                            data = self.get_report_data(reportDocumentId)
                            return data
                        except Exception as e:
                            print('Error getting report data')
                            traceback.print_exc()
                            break
                    elif reportStatus == 'CANCELLED':
                        print('Report cancelled')
                        return []
                    else:
                        print(f"Report status: {reportStatus}")
                        time.sleep(15)
                except Exception as e:
                    print('Error getting report status')
                    traceback.print_exc()
        except Exception as e:
            print("Could not initiate report")
            print(response)
            traceback.print_exc()

    def loadDataToBigQuery(self,BQdataset,BQtable,data,force_schema = False,WRITE_DISPOSITION='WRITE_TRUNCATE'):
        try:
            print('Loading data to BigQuery...')    
            self.bigQueryLoader.loadDataToBQ(data,BQdataset,BQtable,platform='amazon_seller',force_schema=force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)
            print('Data loaded to BigQuery')
        except Exception as e:
            print('Error loading data to BigQuery')
            traceback.print_exc()

    def addReport(self,type,startTime,endTime,BQdataset,BQtable):
        report = {'params' : {'reportType': type,
                              'dataStartTime': startTime,
                              'dataEndTime': endTime,
                          'marketplaceIds': list(self.MARKETPLACE_IDS.values())},
              'BQproject': self.project_id,
              'BQdataset': BQdataset,
              'BQtable': BQtable}
        self.reportQueries.append(report)

    def run(self):
        redirect_uri = "https://www.amazon.com"
        self.updateToken(redirect_uri)
        for report in self.reportQueries:
            data = self.get_report(report)
            try:
                if data != []:
                    self.loadDataToBigQuery(report['BQdataset'],report['BQtable'],data)
            except Exception as e:
                print('Error loading data to BigQuery')
                traceback.print_exc()

