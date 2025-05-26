from datetime import datetime, timedelta
import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader
from Utils.GoogleCloudHandlers.cloud_storage_handler import CloudStorageHandler
from Utils.lib.mail_alert_handler import MailAlertHandler
import requests
import traceback
import json 
import yaml
import time 


class ETLMetaMarketingAPI():
    def __init__(self, adminServiceAccount,metaAdsCredentials = None,debug=False):

        self.adminServiceAccount = adminServiceAccount
        self.bqLoader = BigQueryLoader(adminServiceAccount=adminServiceAccount,debug=debug)
        self.cloudStorageHandler = CloudStorageHandler(credentials=adminServiceAccount)
        self.mailAlertHandler = MailAlertHandler()
        self.project_id = self.adminServiceAccount['project_id']
        
        if metaAdsCredentials is None:
            self.metaAdsCredentials = self.getCredentials()
        else:
            self.metaAdsCredentials = metaAdsCredentials

        self.USER_ACCESS_TOKEN = self.metaAdsCredentials['facebook']['USER_ACCESS_TOKEN']
        self.APP_VERSION = self.metaAdsCredentials['facebook']['APP_VERSION']
        self.AD_ACCOUNT_IDS = self.metaAdsCredentials['facebook']['AD_ACCOUNT_IDS']  
        self.APP_ID = self.metaAdsCredentials['facebook']['APP_ID']
        self.APP_SECRET = self.metaAdsCredentials['facebook']['APP_SECRET']
        self.client_id = self.metaAdsCredentials['facebook']['APP_ID']
    
        self.tokenExipration = self.checkCredentialValidity()

        if self.tokenExipration < 10:
            print("Sending alert email")
            self.sendAlertEmail()

    def getCredentials(self):
        bucket_name = self.project_id.replace('-','_') + '_meta_stored_params'
        credentials = self.cloudStorageHandler.get_stored_file(bucket_name,'meta_ads_login.yml')
        return yaml.load(credentials,Loader=yaml.FullLoader)

    def checkCredentialValidity(self):
        url = 'https://graph.facebook.com/debug_token'
        params = {'access_token': self.USER_ACCESS_TOKEN,
                'input_token': self.USER_ACCESS_TOKEN,
                'client_id' : self.APP_ID}
        response = requests.get(url, params=params).json()
        time_left_unix = response['data']['expires_at'] - time.time()
        time_in_days = time_left_unix / 60 / 60 / 24
        print("Time left in days: ", time_in_days)
        return time_in_days


    def sendAlertEmail(self):
        subject = "Action Required: Refresh Meta Ads API Credentials for project " + self.project_id
        message = f"""
        <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; background-color: #f4f4f9; padding: 20px; margin: 0;">
                <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);">
                    <!-- Header Section -->
                    <div style="text-align: center; border-bottom: 1px solid #ddd; padding-bottom: 20px; margin-bottom: 20px;">
                        <h1 style="color: #0056b3; font-size: 24px;">⚠️ Action Required: Refresh Your Meta Ads API Credentials</h1>
                        <p style="color: #999; font-size: 14px;">Brought to you by DataGem Consulting</p>
                    </div>
                    
                    <!-- Body Content -->
                    <p style="font-size: 16px;">Dear User,</p>
                    <p style="font-size: 16px;">
                        The Meta Ads API credentials for project <strong style="color: #0056b3;">{self.project_id}</strong> are about to expire in 
                        <strong style="color: #e63946;">{int(self.tokenExipration)} days</strong>.
                    </p>
                    <p style="font-size: 16px;">
                        Please complete the following steps to ensure uninterrupted service:
                    </p>
                    <ol style="padding-left: 20px; font-size: 16px; color: #555;">
                        <li>Visit the <a href="https://developers.facebook.com/tools/explorer/" target="_blank" style="color: #0056b3; text-decoration: none;">Meta Developers Tools Explorer</a>.</li>
                        <li>Generate a short-lived token using the tool.</li>
                        <li>Run the provided code in <strong>MetaApiKeyExchange.ipynb</strong> to refresh the credentials.</li>
                        <li>Upload the new credentials to the GCP Bucket.</li>
                    </ol>
                    
                    <!-- Call to Action -->
                    <div style="text-align: center; margin: 30px 0;">
                        <a href="https://developers.facebook.com/tools/explorer/" target="_blank" style="display: inline-block; background-color: #0056b3; color: #ffffff; text-decoration: none; padding: 12px 20px; border-radius: 5px; font-size: 16px;">Refresh Credentials Now</a>
                    </div>
                    
                    <!-- Support Message -->
                    <p style="font-size: 16px; color: #555;">
                        If you encounter any issues, please contact the DataGem Consulting Team for assistance.
                    </p>
                    
                    <!-- Footer -->
                    <div style="border-top: 1px solid #ddd; padding-top: 20px; margin-top: 20px; text-align: center; font-size: 14px; color: #999;">
                        <p>&copy; {self.project_id} | Powered by DataGem Consulting</p>
                        <p>Need help? Reach out to us at <a href="mailto:pierre@datagem-consulting.com" style="color: #0056b3; text-decoration: none;">support@datagemconsulting.com</a></p>
                    </div>
                </div>
            </body>
        </html>
        """

        self.mailAlertHandler.sendEmail(plateform='Meta Ads', project_id=self.project_id, subject=subject, message=message)

    def refreshCredentials(self,fb_exhange_token):
        print("Refreshing credentials...")
        url = f"https://graph.facebook.com/{self.APP_VERSION}/oauth/access_token"
        params = {'grant_type': 'fb_exchange_token',
            'client_id': self.client_id,
            'client_secret': self.APP_SECRET,
            'fb_exchange_token': fb_exhange_token}
        response = requests.get(url, params=params).json()
        try:
            self.USER_ACCESS_TOKEN = response['access_token']
            self.metaAdsCredentials['facebook']['USER_ACCESS_TOKEN'] = self.USER_ACCESS_TOKEN
            self.cloudStorageHandler.update_stored_file(self.project_id.replace('-','_') + '_meta_stored_params','meta_ads_login.yml',yaml.dump(self.metaAdsCredentials))
            print("Credentials refreshed")
        except Exception as e:
            print("Could not refresh credentials")
            traceback.print_exc()

    def fetch_entity_ids(self,entity,date_preset='last_7d'):
        entity_ids =[]
        for AD_ACCOUNT_ID in self.AD_ACCOUNT_IDS:
            print(f"Fetching {entity} ids for ad account {AD_ACCOUNT_ID}...")
            if entity == 'ads':
                updated_since = int((datetime.now() - timedelta(days=7)).timestamp())
            else:
                updated_since = None
            url = f"https://graph.facebook.com/{self.APP_VERSION}/act_{AD_ACCOUNT_ID}/{entity}?" 
            while True :
                try:
                    params = {"access_token" : self.USER_ACCESS_TOKEN,
                            "date_preset":date_preset,
                            "effective_status":"['ACTIVE','PAUSED']",
                            "updated_since": updated_since,
                            "limit":5000}
                    response = requests.request("GET", url, params=params).json()
                    if 'error' in response:
                        print(f"Error in fetching {entity} ids : code {response['error']['code']} message {response['error']['message']}")
                        break
                    # print(response)
                    ids = [x['id'] for x in response['data']]
                    data_len = len(response['data'])
                    if data_len < 5000 or (response['paging'].get('next',False) == False):
                        entity_ids += ids 
                        break 
                    else:
                        new_url = response['paging']['next']
                        if url == new_url:
                            break
                        else:
                            entity_ids += ids     
                            url = new_url
                except Exception as e:
                    print(response)
                    traceback.print_exc()
                    pass
        return entity_ids

    def fetch_entity_infos(self,entity_ids,info_fields=[]):
        nb_ids = len(entity_ids)
        entity_info = []
        for i in range(0,len(entity_ids),40):
            batch = []
            print(f"Fetching entities {i} to {min(i+40,nb_ids)} over {nb_ids} entities...")
            for entity_id in entity_ids[i:min(i+40,len(entity_ids))]:
                request = {"method":"GET",
                        "relative_url":f"{self.APP_VERSION}/{entity_id}?fields={info_fields}&date_preset=last_7d"}
                batch.append(request)
            url = f"https://graph.facebook.com"
            params = {"access_token" : self.USER_ACCESS_TOKEN,
                    "batch":json.dumps(batch)}
            response = requests.request("POST", url, params=params).json() 
            for i in range(len(response)):
                if 'error' in response[i]:
                    print(f"Error in batch : code {response[i]['error']['code']} message {response[i]['error']['message']}")
                    break
                else:
                    entity_info.append(json.loads(response[i]['body']))
            else:
                continue
            break
        return entity_info

    def fetch_entity_insights(self,entity_ids,date_preset = "last_7d",insight_fields=[]):
        entities_insights = []
        has_error = False
        for i in range(0,len(entity_ids),1):
            if has_error:
                break
            print(f"Fetching insights for entity {i+1} over {len(entity_ids)} entities...")
            entity_insights = []
            page = ""
            page_num = 1
            while not(has_error):
                try:
                    print(f"Fetching page {page_num} : {page} ...")
                    url = f"https://graph.facebook.com/{self.APP_VERSION}/{entity_ids[i]}/insights?"
                    params = {"access_token" : self.USER_ACCESS_TOKEN,
                            "fields":insight_fields,
                            "breakdowns":"publisher_platform",
                            "time_increment":1,
                            "level":"campaign",
                            "action_attribution_windows":"1d_view",
                            "date_preset":date_preset,
                            "sort":"date_start_descending",
                            "page":page,
                            "limit":10000}
                    response = requests.request("GET", url, params=params).json()
                    if 'data' not in response:
                        has_error = True
                        break
                    if 'paging' not in response:          
                        for j in range(len(response['data'])):
                            response['data'][j]['entity_id'] =  entity_ids[i]
                        entity_insights += response['data']
                        break
                    if 'after' in response['paging']['cursors'] :
                        if (response['paging']['cursors']['after'] == response['paging']['cursors']['before']):
                            for j in range(len(response['data'])):
                                response['data'][j]['entity_id'] =  entity_ids[i]
                            entity_insights += response['data']
                            break
                        elif page == response['paging']['cursors']['after']:  
                            break  
                        else:
                            page = response['paging']['cursors']['after']
                            page_num += 1
                            for j in range(len(response['data'])):
                                response['data'][j]['entity_id'] =  entity_ids[i]
                            entity_insights += response['data']
                    else:
                        entity_insights += response['data']
                        break
                except Exception as e:
                    print(response)
                    has_error = True
                    traceback.print_exc()
                    break
            entities_insights += entity_insights
        if has_error:
            print(response)
            print("An error occured")
        return entities_insights

    def runQuery(self,query):
        query["BQproject"] = self.project_id
        entity = query['entity']
        try:
            print(f"Fetching {entity} ids...")
            entity_ids = self.fetch_entity_ids(entity,date_preset=query['date_preset'])
            if len(entity_ids) == 0:
                print(f"No {entity} ids fetched")
            print(f"{len(entity_ids)} {entity} ids fetched")
            try:
                print(f"Fetching {entity} infos...")
                entity_info = self.fetch_entity_infos(entity_ids,info_fields= query['info_fields'])
                try:
                    print(f"Loading {entity} infos to BigQuery...")
                    self.bqLoader.loadDataToBQ(entity_info,query['BQdataset'],query['BQtable_info'],platform='meta_ads',force_schema=query['force_schema'],base_table=query['BQtable_info_base'],WRITE_DISPOSITION='WRITE_TRUNCATE')
                    print(f"{entity} infos loaded to BigQuery")
                except Exception as e:
                    print(f"Could not load {entity} infos to BigQuery")
                    traceback.print_exc() 
            except Exception as e:
                print(f"Could not fetch {entity} infos")
                traceback.print_exc()
            try:
                print(f"Fetching {entity} insights...")
                entity_insights = self.fetch_entity_insights(entity_ids,insight_fields= query['insights_fields'],date_preset=query['date_preset'])
                try:
                    print(f"Loading {entity} insights to BigQuery...")
                    self.bqLoader.loadDataToBQ(entity_insights,query['BQdataset'],query['BQtable_insights'],platform='meta_ads',force_schema=query['force_schema'],base_table=query['BQtable_insights_base'],WRITE_DISPOSITION='WRITE_TRUNCATE')
                    print(f"{entity} insights loaded to BigQuery")
                except Exception as e:
                    print(f"Could not load {entity} insights to BigQuery")
                    traceback.print_exc()
            except Exception as e:
                print(f"Could not fetch {entity} insights")
                traceback.print_exc()
        except Exception as e:
            print(f"Could not fetch {entity} ids")
            traceback.print_exc()