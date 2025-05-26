import requests
import yaml 
import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
from datetime import datetime, timedelta
import time 
import gzip 
import json
import traceback
from urllib.parse import urlparse
from urllib.parse import parse_qs
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader
from Utils.GoogleCloudHandlers.cloud_storage_handler import CloudStorageHandler

class AmazonAdsETL():
    def __init__(self, storageServiceAccountCredential,dataset_id = 'marketplace_Amazon_Ads',debug = False):
        self.project_id = storageServiceAccountCredential['project_id']
        self.dataset_id = dataset_id
        self.bigQueryLoader = BigQueryLoader(storageServiceAccountCredential,debug=debug)
        self.cloudStorageHandler = CloudStorageHandler(storageServiceAccountCredential)
        
        credentials_login = self.update_token(yaml.load(self.cloudStorageHandler.get_stored_file('amazon_stored_params','login.yml'),Loader=yaml.FullLoader))
        self.cloudStorageHandler.update_stored_file('amazon_stored_params','login.yml',yaml.dump(credentials_login))
        self.CLIENT_ID = credentials_login['amazon_ads']['CLIENT_ID']
        self.CLIENT_SECRET = credentials_login['amazon_ads']['CLIENT_SECRET']
        self.ACCESS_TOKEN = credentials_login['amazon_ads']['ACCESS_TOKEN']

        self.profiles = self.fetch_profiles()
        try:
            print('Fetching pending reports...')
            self.reports = self.get_old_reports()
            print(f"{len(self.reports)} pending reports found")
        except Exception as e:
            print('No old reports found')
            reports = {}
        

    def update_token(credentials_login):
        CLIENT_ID = credentials_login['amazon_ads']['CLIENT_ID']
        CLIENT_SECRET = credentials_login['amazon_ads']['CLIENT_SECRET']
        REFRESH_TOKEN = credentials_login['amazon_ads']['REFRESH_TOKEN']
        
        url = "https://api.amazon.com/auth/o2/token"
        params = {'grant_type' : 'refresh_token',
                    'refresh_token' : REFRESH_TOKEN,
                    'client_id' : CLIENT_ID,
                    'client_secret' : CLIENT_SECRET,
                    'redirect_uri' : 'https://www.bravosierra.com/account/login'}
        try:
            print('Fetching new token...')
            response = requests.post(url, params=params)
            expires_in = response.json()['expires_in']
            print('New token expiration in seconds: ', expires_in)
            new_token = response.json()['access_token']
            new_refresh_token = response.json()['refresh_token']
            credentials_login['amazon_ads']['ACCESS_TOKEN'] = new_token
            credentials_login['amazon_ads']['REFRESH_TOKEN'] = new_refresh_token
            return credentials_login
        except Exception as e:
            print('Error fetching token')
            traceback.print_exc()



    def fetch_profiles(self):
        url = "https://advertising-api.amazon.com/v2/profiles"
        headers = {'Authorization' : 'Bearer ' + self.ACCESS_TOKEN,
                'Amazon-Advertising-API-ClientId' : self.CLIENT_ID,
                'Content-Type' : 'application/json'}
        params = {}
        response = requests.get(url, headers=headers, params=params).json()
        profiles = {profile['countryCode']: profile['profileId']  for profile in response}
        return profiles


    def get_old_reports(self):
        file = self.cloudStorageHandler.get_stored_file('amazon_stored_params','reports.txt')
        old_reports = json.loads(file)
        if old_reports == {}:
            return {}
        return old_reports

    def add_report(self,reportTypeId,adProduct,fields):
        new_report = {'reportTypeId': reportTypeId,
                    'adProduct': adProduct,
                    'fields': fields}
        self.reports.append(new_report)
        
    def initiate_reports(self,reports):
        new_reports = {}
        url = "https://advertising-api.amazon.com/reporting/reports"
        headers = {'Amazon-Advertising-API-ClientId' : self.CLIENT_ID,
                'Amazon-Advertising-API-Scope' : str(self.profiles['US']),
                'Authorization' : 'Bearer ' + self.ACCESS_TOKEN,
                'Content-Type' : 'application/json'}
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        end_date = end_date.strftime("%Y-%m-%d")
        start_date = start_date.strftime("%Y-%m-%d")

        for reportQuery in reports:
            if reports == {}:
                dates = []
            else:
                dates = [v['dates'] for k,v in reports.items() if v['reportTypeId'] == reportQuery['reportTypeId']]
            if f"{start_date} to {end_date}" in dates:
                print(f"Report {reportQuery['reportTypeId']} already initiated for {start_date} to {end_date}")
            else:
                print(f"Initiate {reportQuery['reportTypeId']} report for {start_date} to {end_date}")
                payload = {"name": f"Sponsored Products Campaign Performance Report - {start_date} - {end_date}",
                            "endDate": end_date,
                        "startDate": start_date,
                        "configuration": {"adProduct": reportQuery['adProduct'],
                                        "groupBy" : ["campaign"],
                                        "format" : "GZIP_JSON",
                                        "reportTypeId" : reportQuery['reportTypeId'],
                                        "columns" : reportQuery['fields'],
                                        "timeUnit": "DAILY",}
                        }
                try:
                    response = requests.post(url, headers=headers, json=payload).json()
                    new_reports[response['reportId']] = {"status": response['status'], 
                                                        "reportTypeId": reportQuery['reportTypeId'],
                                                        "table_id":f"P_TD_AmazonAds_{reportQuery['adProduct']}_temp",
                                                        "dates":f"{start_date} to {end_date}"}
                except Exception as e:
                    print("Could not initiate report")
                    print(response)
                    traceback.print_exc()
        return new_reports
  
    def fetch_report(self,downloadUrl):
        try:
            parsed_url = urlparse(downloadUrl)
            token = parse_qs(parsed_url.query)['X-Amz-Security-Token'][0]
            algorithm = parse_qs(parsed_url.query)['X-Amz-Algorithm'][0]
            params = {"X-Amz-Algorithm": algorithm,}
            headers = {
                        'Amazon-Advertising-API-ClientId' : self.CLIENT_ID,
                        'Content-Type' : 'application/json'}
            response = requests.get(downloadUrl, params= params,stream=True,headers=headers).content
            return json.loads(gzip.decompress(response).decode('utf-8'))
        except:
            print("Couldn't decompress report")
            print(response)
            traceback.print_exc()
            return []

    def get_reports(self,reports):
        for reportId in reports:
            print(f"Fetching report {reportId}")
            if reports[reportId]['status'] in ['SUCCESS','COMPLETED']:
                print(f"Report current status {reports[reportId]['status']}")
                try:
                    new_data = self.fetch_report(reportId)
                    if new_data != []:
                        try:
                            self.bigQueryLoader.loadDataToBQ(new_data,self.project_id,self.dataset_id,reports[reportId]['table_id'],force_schema = False)
                            reports[reportId]['status'] = 'FETCHED'
                        except:
                            print(f"Couldn't load report {reportId} to BQ")
                            traceback.print_exc()
                            pass
                except:
                    print(f"Couldn't fetch report {reportId}")
                    # print(response)
                    traceback.print_exc()
                    pass
            else:
                try:
                    url = f"https://advertising-api.amazon.com/reporting/reports/{reportId}"
                    headers = {'Amazon-Advertising-API-ClientId' : self.CLIENT_ID,
                            'Amazon-Advertising-API-Scope' : str(self.profiles['US']),
                            'Authorization' : 'Bearer ' + self.ACCESS_TOKEN,
                            'Content-Type' : 'application/json'}
                    response = requests.get(url, headers=headers).json()
                    # print(response)
                    if response['status'] in ['SUCCESS','COMPLETED']:
                        print(f"Report current status {response['status']}")
                        try:
                            download_url = response['url']
                            # print(download_url)
                            new_data = self.fetch_report(download_url)
                            if new_data != []:
                                try:
                                    print(f"Loading report {reportId} to BQ")
                                    self.bigQueryLoader.loadDataToBQ(new_data,self.project_id,self.dataset_id,reports[reportId]['table_id'],force_schema = False,platform='amazon_ads')
                                    reports[reportId]['status'] = 'FETCHED'
                                except Exception as e:
                                    print(f"Couldn't load report {reportId} to BQ")
                                    traceback.print_exc()
                                    pass
                            else:
                                print(f"Report {reportId} is empty")
                                Exception("Empty report")
                        except:
                            print(f"Couldn't fetch report {reportId}")
                            print(response)
                            traceback.print_exc()
                            pass
                    else:
                        print(f"Report {reportId} not ready yet. Current status: {response['status']}")
                except:
                    print(f"Couldn't fetch report {reportId} status")
        
                    print(response)
                    traceback.print_exc()
                    pass
        new_reports = {}
        for k,v in reports.items():
            if v['status'] != 'FETCHED':
                new_reports[k] = v
        print(f"Reports not fetched yet:  {len(new_reports)} out of {len(reports)}")
        reports = new_reports
        try:
            print('Saving reports...')
            self.cloudStorageHandler.update_stored_file('amazon_stored_params','reports.txt',json.dumps(reports))
            print('Reports saved')
        except:
            print("Couldn't save reports")
            traceback.print_exc()
            pass
  


