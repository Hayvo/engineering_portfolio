import json 
import requests 
import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader
from Utils.GoogleCloudHandlers.cloud_storage_handler import CloudStorageHandler
import time
import yaml
import traceback
import zipfile
import csv
import regex as re
import io 

class ETLBingAds:
    def __init__(self, adminServiceAccountCredential, bingCredentials = None,debug=False):
        self.project_id = adminServiceAccountCredential['project_id']
        self.bigQueryLoader = BigQueryLoader(adminServiceAccount= adminServiceAccountCredential, debug=debug)
        self.cloudStorageHandler = CloudStorageHandler(adminServiceAccountCredential)
        
        if bingCredentials is None:
            self.credentials = self.get_credentials()
        else:
            self.credentials = bingCredentials

        token_expiration_date = self.credentials["bing-ads"]['access_token_expiration']
        if time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) > token_expiration_date:
            self.refresh_token()

        self.access_token = self.credentials["bing-ads"]['access_token']
        self.client_id = self.credentials["bing-ads"]['client_id']
        self.client_secret = self.credentials["bing-ads"]['client_secret']
        self.developer_token = self.credentials["bing-ads"]['developer_token']
        self.account_id = self.credentials["bing-ads"]['account_id']
        self.customer_id = self.credentials["bing-ads"]['customer_id']

    def get_credentials(self):
        try:
            print("Getting credentials from Cloud Storage")
            credentials = self.cloudStorageHandler.get_stored_file('stored_params','bing_ads_login.yml')
            return yaml.load(credentials,Loader=yaml.FullLoader)
        except Exception as e:
            print(f"Error getting credentials: {e}")
            traceback.print_exc()

    def refresh_token(self):
        print("Refreshing token")
        url = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "client_id": self.credentials["bing-ads"]['client_id'],
            "scope": "https://ads.microsoft.com/msads.manage offline_access",
            "refresh_token": self.credentials["bing-ads"]['refresh_token'],
            "grant_type": "refresh_token",
            "client_secret": self.credentials["bing-ads"]['client_secret']
        }
        response = requests.post(url, headers=headers, data=data)
        response = response.json()
        self.credentials["bing-ads"]['access_token'] = response['access_token']
        self.credentials["bing-ads"]['refresh_token'] = response['refresh_token']
        expires_in = response['expires_in']
        expiration_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + expires_in))
        self.credentials["bing-ads"]['access_token_expiration'] = expiration_date
        with open(f'./src/var/login_credentials/{self.project_id}/bing_ads_login.yml', 'w') as file:
            yaml.dump(self.credentials, file)
        try:
            print("Updating credentials in Cloud Storage")
            self.cloudStorageHandler.update_stored_file('stored_params','bing_ads_login.yml',yaml.dump(self.credentials))
        except Exception as e:
            print(f"Error updating credentials in Cloud Storage: {e}")
            traceback.print_exc()

    def loadDataToBigQuery(self,data,BQtable,BQdataset = 'marketplace_BingAds',base_table = None,force_schema = False,WRITE_DISPOSITION = 'WRITE_TRUNCATE'):
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
            self.bigQueryLoader.loadDataToBQ(data,BQdataset,BQtable,platform= 'bing_ads',base_table=base_table,force_schema=force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            traceback.print_exc()


    def requestCampaignsReport(self,
                        reportName = "Campaign Insights", 
                        excludeColumnsHeaders=False, 
                        excludeReportFooter=True, 
                        excludeReportHeader=True, 
                        format="CSV", 
                        formatVersion="2.0", 
                        returnOnlyCompleteData=False, 
                        aggregation="Daily",
                        accountStatus="Active,Inactive,Paused",
                        predefinedTime="Last30Days",
                        reportTimeZone="EasternTimeUSCanada"):
        '''
        Setup a Campaigns report from Bing Ads API
        Args:
            reportName (str): Name of the report
            excludeColumnsHeaders (bool): Exclude the column headers
            excludeReportFooter (bool): Exclude the report footer
            excludeReportHeader (bool): Exclude the report header
            format (str): Format of the report
            formatVersion (str): Version of the format
            returnOnlyCompleteData (bool): Return only complete data
            aggregation (str): Aggregation of the report
            accountStatus (str): Status of the account
            predefinedTime (str): Time period of the report
            reportTimeZone (str): Time zone of the report
        Returns:
            dict: Response from the Bing Ads API
        '''

        campaignQuery = {
            "ExcludeColumnHeaders": excludeColumnsHeaders,
            "ExcludeReportFooter": excludeReportFooter,
            "ExcludeReportHeader": excludeReportHeader,
            "Format": format,
            "FormatVersion": formatVersion,
            "ReportName": reportName,
            "ReturnOnlyCompleteData": returnOnlyCompleteData,
            "Type": "CampaignPerformanceReportRequest",
            "Aggregation": aggregation,
            "Columns": ["TimePeriod ",
                "AccountId","AccountName","AccountNumber","AccountStatus","AdDistribution","AdRelevance",
                "BaseCampaignId","CampaignId","CampaignLabels","CampaignName","CampaignStatus","CampaignType",
                "CurrencyCode","CustomerId","CustomerName","DeviceType",
                "AllConversions","Conversions","Clicks","ClickSharePercent",
                "Impressions","ImpressionSharePercent","Installs",
                "Purchases","QualityScore","AllRevenue","Revenue","Spend"
            ],
            "Filter": {
                "AccountStatus": accountStatus,
            },
            "Scope": {
                "AccountIds": [
                    f"{self.account_id}"
                ],
            },
            "Time": {
                "PredefinedTime": predefinedTime,
                "ReportTimeZone": reportTimeZone
            }
            }

        url = "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Submit"

        headers = {"Authorization" : f"Bearer {self.access_token}",
                "CustomerAccountId" : f"{self.account_id}",
                "CustomerId" : f"{self.customer_id}",
                "DeveloperToken" : self.developer_token}
        try:
            response = requests.post(url, headers=headers, json={"ReportRequest": campaignQuery})
            return response.json()['ReportRequestId']
        except Exception as e:
            print(f"Error requesting report: {e}")
            traceback.print_exc()
            print(response.json())
    
    def getReportStatus(self, reportId):
        '''
        Get the report from Bing Ads API
        Args:
            reportId (str): Id of the report
        Returns:
            dict: Response from the Bing Ads API
        '''

        url = f"https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Poll"

        headers = {"Authorization" : f"Bearer {self.access_token}",
                "CustomerAccountId" : f"{self.account_id}",
                "CustomerId" : f"{self.customer_id}",
                "DeveloperToken" : self.developer_token}
        
        params = {
            "ReportRequestId": reportId
        }
        try:
            response = requests.post(url, headers=headers, json=params)
            return response.json()['ReportRequestStatus']
        except Exception as e:
            print(f"Error getting report status: {e}")
            traceback.print_exc()

    def downloadReport(self, downloadUrl):
        '''
        Download the report from Bing Ads API
        Args:
            downloadUrl (str): Url to download the report
        Returns:
            str: Content of the report
        '''

        urlParams = re.findall(r".*zip\?(.*)", downloadUrl)[0]
        urlParams = urlParams.split("&")
        headers = {}
        for param in urlParams:
            key, value = param.split("=")
            headers[key] = value

        try:
            response = requests.get(downloadUrl, stream=True , headers=headers)
            content = io.BytesIO(response.content)
            with zipfile.ZipFile(content) as z:
                csv_file = z.namelist()[0]
                with z.open(csv_file) as f:
                    csv_reader = csv.reader(io.TextIOWrapper(f, 'utf-8'))
                    report = [row for row in csv_reader]
            json_data = [dict(zip(report[0], row)) for row in report[1:]]
            return json_data
        except Exception as e:
            print(f"Error downloading report: {e}")
            traceback.print_exc()

        
    def getCampaignsInsights(self,
                        reportName = "Campaign Insights", 
                        excludeColumnsHeaders=False, 
                        excludeReportFooter=True, 
                        excludeReportHeader=True, 
                        format="CSV", 
                        formatVersion="2.0", 
                        returnOnlyCompleteData=False, 
                        aggregation="Daily",
                        accountStatus="Active,Inactive,Paused",
                        predefinedTime="Last30Days",
                        reportTimeZone="EasternTimeUSCanada"):
        
        '''
        Get the Campaigns insights from Bing Ads API
        Args:
            reportName (str): Name of the report
            excludeColumnsHeaders (bool): Exclude the column headers
            excludeReportFooter (bool): Exclude the report footer
            excludeReportHeader (bool): Exclude the report header
            format (str): Format of the report
            formatVersion (str): Version of the format
            returnOnlyCompleteData (bool): Return only complete data
            aggregation (str): Aggregation of the report
            accountStatus (str): Status of the account
            predefinedTime (str): Time period of the report
            reportTimeZone (str): Time zone of the report
        Returns:
            list: List of dictionaries with the insights
        '''

        reportId = self.requestCampaignsReport(reportName, excludeColumnsHeaders, excludeReportFooter, excludeReportHeader, format, formatVersion, returnOnlyCompleteData, aggregation, accountStatus, predefinedTime, reportTimeZone)
         

        while True:
            status = self.getReportStatus(reportId)
            print(f"Report status: {status['Status']}")
            if status['Status'] == "Success":
                downloadUrl = status['ReportDownloadUrl']
                report = self.downloadReport(downloadUrl)
                return report
            elif status['Status'] == "Error":
                print(f"Error getting report: {status['Error']}")
                return []
            