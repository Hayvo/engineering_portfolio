import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader
from Utils.GoogleCloudHandlers.cloud_storage_handler import CloudStorageHandler
import traceback
import requests
import regex as re
from google.cloud import bigquery
from datetime import datetime,timedelta
import time 

class KlaviyoETL():
    def __init__(self,adminServiceAccountCredential,
                 credentials,
                 BQdataset = 'marketplace_Klaviyo',
                 debug = False,
                 defaultEndTime = datetime.now().strftime("%Y-%m-%dT00:00:00Z"),
                 defaultStartTime = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%dT00:00:00Z")):
        self.adminServiceAccountCredential = adminServiceAccountCredential
        self.cloudStorageHandler = CloudStorageHandler(adminServiceAccountCredential)
        self.project_id = adminServiceAccountCredential['project_id']
        self.bigQueryLoader = BigQueryLoader(adminServiceAccountCredential,debug = debug)
        self.PRIVATE_API_KEY = credentials['klaviyo']['PRIVATE_API_KEY']
        self.API_VERSION = "2024-10-15"
        self.BQdataset = BQdataset
        self.defaultEndTime = defaultEndTime
        self.defaultStartTime = defaultStartTime
        self.jobs = []

    def loadDataToBigQuery(self,data,BQtable,base_table = None,force_schema = False,WRITE_DISPOSITION='WRITE_TRUNCATE'):
        self.bigQueryLoader.loadDataToBQ(data,self.BQdataset,BQtable,platform= 'klaviyo',base_table= base_table,force_schema = force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)

    def getDataFromBigQuery(self,BQtable,fields):
        client = bigquery.Client.from_service_account_info(self.adminServiceAccountCredential)
        try:
            try:
                query = f"SELECT {','.join(fields)} FROM `{self.project_id}.{self.BQdataset}.{BQtable}`"
                query_job = client.query(query)
                result = query_job.result().to_dataframe().to_dict(orient='records')
                return result
            except Exception as e:
                print('Error getting data from BigQuery')
                traceback.print_exc()
        except Exception as e:
            print('Table was not found in the location provided')
            traceback.print_exc()
    
    def getMetrics(self):
        data = []
        url = f"https://a.klaviyo.com/api/metrics"
        headers = {"accept": "application/json",
                    "revision": self.API_VERSION,
                    "content-type": "application/json",
                    "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
        while url != None:
            try:
                response = requests.get(url, headers=headers).json()
                while 'errors' in response:
                    if 'throttled' in [error['code'] for error in response['errors']]:
                        error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                        wait_time = int(re.search(r'\d+',error_details).group())
                        print(f"Throttled, waiting for {wait_time} second")
                        time.sleep(wait_time)
                        response = requests.get(url, headers=headers).json()
                    else:
                        print('Error getting metrics')
                        print(response)
                        break
                for x in response['data']:
                    x['attributes'].update({'id': x['id']})
                    data.append(x['attributes'])
                url = response.get('links',{}).get('next',None)
            except Exception as e:
                print('Error getting metrics')
                traceback.print_exc()
                url = None
                break
        return data
    
    

    def getMetricData(self,metric_ids,groupby,interval = "day",timezone = 'UTC',startTime = None,endTime = None):
        # print(metric_ids)
        if startTime == None:
            startTime = self.defaultStartTime
        if endTime == None:
            endTime = self.defaultEndTime
        # metric_ids = [metric['id'] for metric in self.getDataFromBigQuery('raw_Klaviyo_Metrics',['id'])]
        data = []
        for metric_id in metric_ids:
            temp_data = []
            time.sleep(0.7)
            url = f"https://a.klaviyo.com/api/metric-aggregates/"
            headers = {"accept": "application/json",
                        "revision": self.API_VERSION,
                        "content-type": "application/json",
                        "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
            payload = { "data": 
                        {"type": "metric-aggregate",
                            "attributes": {
                                "metric_id": metric_id,
                                "interval": interval,
                                "page_size": 500,
                                "timezone": timezone,
                                "filter" : [f"greater-or-equal(datetime,{startTime}),less-than(datetime,{endTime})"],
                                "measurements": ["count", "sum_value", "unique"],
                                "by": groupby,
                                "page_size": 500,
                                "timezone": "America/New_York",}}}
            while url != None:
                try:
                    response = requests.post(url, headers=headers, json=payload).json()
                    while 'errors' in response:
                        if 'throttled' in [error['code'] for error in response['errors']]:
                            error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                            wait_time = int(re.search(r'\d+',error_details).group())
                            print(f"Throttled, waiting for {wait_time} second")
                            time.sleep(wait_time)
                            response = requests.get(url, headers=headers).json()
                        else:
                            print('Error in response')
                            print(response)
                            traceback.print_exc()
                            break
                    response['data']['attributes'].update({'id': response['data']['id']})
                    response['data']['attributes'].update({'metric_id': metric_id})
                    temp_data.append(response['data']['attributes'])
                    url = response.get('links',{}).get('next',None)
                except Exception as e:
                    print('Error getting metric data')
                    traceback.print_exc()
                    url = None
                    break
            data += temp_data
        return data

    def getLists(self):
        data = []
        url = f"https://a.klaviyo.com/api/lists"
        headers = {"accept": "application/json",
                    "revision": self.API_VERSION,
                    "content-type": "application/json",
                    "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
        while url != None:
            try:
                response = requests.get(url, headers=headers).json()
                while 'errors' in response:
                    if 'throttled' in [error['code'] for error in response['errors']]:
                        error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                        wait_time = int(re.search(r'\d+',error_details).group())
                        print(f"Throttled, waiting for {wait_time} second")
                        time.sleep(wait_time)
                        response = requests.get(url, headers=headers).json()
                    else:
                        print('Error getting lists')
                        print(response)
                        break
                for x in response['data']:
                    x['attributes'].update({'id': x['id']})
                    data.append(x['attributes'])
                url = response.get('links',{}).get('next',None)
            except Exception as e:
                print('Error getting lists')
                traceback.print_exc()
                url = None
                break
        return data
    
    def getListMembers(self,list_ids,startTime = None):
        if startTime == None:
            startTime = self.defaultStartTime
        data = []
        for list_id in list_ids:
            url = f"https://a.klaviyo.com/api/lists/{list_id}/profiles"
            headers = {"accept": "application/json",
                        "revision": self.API_VERSION,
                        "content-type": "application/json",
                        "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
            params = {"page[size]":100,
                                        "fields[profile]":"id",
                                        "filter" : f"greater-or-equal(joined_group_at,{startTime})"}
            is_first = True
            while url != None:
                try:
                    if is_first:
                        response = requests.get(url, headers=headers,params=params).json()
                        is_first = False
                    else:
                        response = requests.get(url, headers=headers).json()
                    while 'errors' in response:
                        if 'throttled' in [error['code'] for error in response['errors']]:
                            error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                            wait_time = int(re.search(r'\d+',error_details).group())
                            print(f"Throttled, waiting for {wait_time} second")
                            time.sleep(wait_time)
                            response = requests.get(url, headers=headers,params = params).json()
                        else:
                            print('Error getting list members')
                            print(response)
                            break
                    # print(response)
                    for x in response['data']:
                        x['attributes'].update({'id': x['id']})
                        x['attributes'].update({'list_id': list_id})
                        data.append(x['attributes'])
                    url = response.get('links',{}).get('next',None)
                except Exception as e:
                    print('Error getting list members')
                    traceback.print_exc()
                    url = None
                    break
        return data

    def getSegments(self):
        data = []
        url = f"https://a.klaviyo.com/api/segments"
        headers = {"accept": "application/json",
                    "revision": self.API_VERSION,
                    "content-type": "application/json",
                    "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
        while url != None:
            try:
                response = requests.get(url, headers=headers).json()
                while 'errors' in response:
                    if 'throttled' in [error['code'] for error in response['errors']]:
                        error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                        wait_time = int(re.search(r'\d+',error_details).group())
                        print(f"Throttled, waiting for {wait_time} second")
                        time.sleep(wait_time)
                        response = requests.get(url, headers=headers).json()
                    else:
                        print('Error getting segments')
                        print(response)
                        break
                for x in response['data']:
                    x['attributes'].update({'id': x['id']})
                    data.append(x['attributes'])
                url = response.get('links',{}).get('next',None)
            except Exception as e:
                print('Error getting segments')
                traceback.print_exc()
                url = None
                break
        return data
    
    def getSegmentMembers(self,segment_ids,startTime = None):
        if startTime == None:
            startTime = self.defaultStartTime
        data = []
        for segment_id in segment_ids:
            # print(segment_id)
            url = f"https://a.klaviyo.com/api/segments/{segment_id}/profiles"
            headers = {"accept": "application/json",
                        "revision": self.API_VERSION,
                        "content-type": "application/json",
                        "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
            params = {"page[size]":100,
                        "fields[profile]":"id",
                        "filter" : f"greater-or-equal(joined_group_at,{startTime})"}
            is_first = True
            while url != None:
                try:
                    if is_first:
                        response = requests.get(url, headers=headers,params=params).json()
                        is_first = False
                    else:
                        response = requests.get(url, headers=headers).json()
                    while 'errors' in response:
                        if 'throttled' in [error['code'] for error in response['errors']]:
                            error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                            wait_time = int(re.search(r'\d+',error_details).group())
                            print(f"Throttled, waiting for {wait_time} second")
                            time.sleep(wait_time)
                            response = requests.get(url, headers=headers,params = params).json()
                        else:
                            print('Error getting segment members')
                            print(response)
                            break
                    # print(response)
                    for x in response['data']:
                        x['attributes'].update({'id': x['id']})
                        x['attributes'].update({'segment_id': segment_id})
                        data.append(x['attributes'])
                    url = response.get('links',{}).get('next',None)
                except Exception as e:
                    print('Error getting segment members')
                    traceback.print_exc()
                    url = None
                    break
        return data

    def getFlows(self):
        data = []
        url = f"https://a.klaviyo.com/api/flows"
        headers = {"accept": "application/json",
                    "revision": self.API_VERSION,
                    "content-type": "application/json",
                    "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
        while url != None:
            try:
                response = requests.get(url, headers=headers).json()
                while 'errors' in response:
                    if 'throttled' in [error['code'] for error in response['errors']]:
                        error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                        wait_time = int(re.search(r'\d+',error_details).group())
                        print(f"Throttled, waiting for {wait_time} second")
                        time.sleep(wait_time)
                        response = requests.get(url, headers=headers).json()
                    else:
                        print('Error getting flows')
                        print(response)
                        break
                for x in response['data']:
                    x['attributes'].update({'id': x['id']})
                    data.append(x['attributes'])
                url = response.get('links',{}).get('next',None)
            except Exception as e:
                print('Error getting flows')
                traceback.print_exc()
                url = None
                break
        return data
    
    def getProfiles(self,startTime = None):
        if startTime == None:
            startTime = self.defaultStartTime
        data = []
        url = f"https://a.klaviyo.com/api/profiles"
        headers = {"accept": "application/json",
                    "revision": self.API_VERSION,
                    "content-type": "application/json",
                    "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
        params = {"page[size]":100,
                "fields[profile]": "email,phone_number,external_id,first_name,last_name,organization,title,created,updated,location",
                "additional-fields[profile]":"subscriptions",
                "filter" : f"greater-than(updated,{startTime})"}
        is_first = True
        while url != None:
            try:
                if is_first:
                    response = requests.get(url, headers=headers,params=params).json()
                    is_first = False
                else:
                    response = requests.get(url, headers=headers).json()
                while 'errors' in response:
                    if 'throttled' in [error['code'] for error in response['errors']]:
                        error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                        wait_time = int(re.search(r'\d+',error_details).group())
                        print(f"Throttled, waiting for {wait_time} second")
                        time.sleep(wait_time)
                        response = requests.get(url, headers=headers,params = params).json()
                    else:
                        print('Error getting profiles')
                        print(response)
                        break
                for x in response['data']:
                    x['attributes'].update({'id': x['id']})
                    data.append(x['attributes'])
                url = response.get('links',{}).get('next',None)
            except Exception as e:
                print('Error getting profiles')
                traceback.print_exc()
                url = None
                break
        return data

    def getEvents(self,metric_id,startTime = None):
        if startTime == None:
            startTime = self.defaultStartTime
        data = []
        url = f"https://a.klaviyo.com/api/events"
        headers = {"accept": "application/json",
                    "revision": self.API_VERSION,
                    "content-type": "application/json",
                    "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
        params = {"filter": f"greater-than(datetime,{startTime}),equals(metric_id,\"{metric_id}\")",
                    "fields[event]":"timestamp,datetime,uuid",
                    "fields[metric]":"name",
                    "include":"attributions,metric,profile"}
        is_first = True
        while url != None:
            try:
                if is_first:
                    response = requests.get(url, headers=headers,params=params).json()
                    is_first = False
                else:
                    response = requests.get(url, headers=headers).json()
                while 'errors' in response:
                    if 'throttled' in [error['code'] for error in response['errors']]:
                        error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                        wait_time = int(re.search(r'\d+',error_details).group())
                        print(f"Throttled, waiting for {wait_time} second")
                        time.sleep(wait_time)
                        response = requests.get(url, headers=headers,params = params).json()
                    else:
                        print('Error getting events')
                        print(response)
                        break
                for x in response['data']:
                    x['attributes'].update({'id': x['id']})
                    data.append(x['attributes'])
                url = response.get('links',{}).get('next',None)
            except Exception as e:
                print('Error getting events')
                traceback.print_exc()
                url = None
                break
        return data

    def getEmailCampaigns(self):
        data = []
        url = f"https://a.klaviyo.com/api/campaigns"
        headers = {"accept": "application/json",
                    "revision": self.API_VERSION,
                    "content-type": "application/json",
                    "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
        params = {"filter":"equals(messages.channel,'email')",
                    "fields[campaign-message]": "label,channel,content,send_times,created_at,updated_at",
                    "fields[campaign]":"name,status,audiences,created_at,scheduled_at,updated_at,send_time",
                    "include":"campaign-messages",
                    "sort":"created_at"}
        is_first = True
        while url != None:
            try:
                if is_first:
                    response = requests.get(url, headers=headers,params=params).json()
                    is_first = False
                else:
                    response = requests.get(url, headers=headers).json()
                while 'errors' in response:
                    if 'throttled' in [error['code'] for error in response['errors']]:
                        error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                        wait_time = int(re.search(r'\d+',error_details).group())
                        print(f"Throttled, waiting for {wait_time} second")
                        time.sleep(wait_time)
                        response = requests.get(url, headers=headers,params = params).json()
                    else:
                        print('Error getting email campaigns')
                        print(response)
                        break
                for x in response['data']:
                    x['attributes'].update({'campaign_type': 'email'})
                    x['attributes'].update({'id': x['id']})
                    x['attributes'].update({'campaign_messages': x['relationships']['campaign-messages']['data']})
                    data.append(x['attributes'])
                url = response.get('links',{}).get('next',None)
            except Exception as e:
                print('Error getting email campaigns')
                traceback.print_exc()
                url = None
                break
        return data
        
    def getSmsCampaigns(self):
        data = []
        url = f"https://a.klaviyo.com/api/campaigns"
        headers = {"accept": "application/json",
                    "revision": self.API_VERSION,
                    "content-type": "application/json",
                    "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
        params = {"filter":"equals(messages.channel,'sms')",
                    "fields[campaign-message]": "label,channel,content,send_times,created_at,updated_at",
                    "fields[campaign]":"name,status,audiences,created_at,scheduled_at,updated_at,send_time",
                    "include":"campaign-messages",
                    "sort":"created_at"}
        is_first = True
        while url != None:
            try:
                if is_first:
                    response = requests.get(url, headers=headers,params=params).json()
                    is_first = False
                else:
                    response = requests.get(url, headers=headers).json()
                while 'errors' in response:
                    if 'throttled' in [error['code'] for error in response['errors']]:
                        error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                        wait_time = int(re.search(r'\d+',error_details).group())
                        print(f"Throttled, waiting for {wait_time} second")
                        time.sleep(wait_time)
                        response = requests.get(url, headers=headers,params = params).json()
                    else:
                        print('Error getting sms campaigns')
                        print(response)
                        break
                for x in response['data']:
                    x['attributes'].update({'campaign_type': 'sms'})
                    x['attributes'].update({'id': x['id']})
                    x['attributes'].update({'campaign_messages': x['relationships']['campaign-messages']['data']})
                    data.append(x['attributes'])
                url = response.get('links',{}).get('next',None)
            except Exception as e:
                print('Error getting sms campaigns')
                traceback.print_exc()
                url = None
                break
        return data
    
    def getCampaignRecipient(self,campaign_ids):
        data = []
        for campaign_id in campaign_ids:
            url = f"https://a.klaviyo.com/api/campaign-recipient-estimations/{campaign_id}"
            headers = {"accept": "application/json",
                        "revision": self.API_VERSION,
                        "content-type": "application/json",
                        "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
            params = {"fields[campaign-recipient-estimation]" : "estimated_recipient_count"}
            is_first = True
            while url != None:
                try:
                    if is_first:
                        response = requests.get(url, headers=headers,params=params).json()
                        is_first = False
                    else:
                        response = requests.get(url, headers=headers).json()
                    while 'errors' in response:
                        if 'throttled' in [error['code'] for error in response['errors']]:
                            error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                            wait_time = int(re.search(r'\d+',error_details).group())
                            print(f"Throttled, waiting for {wait_time} second")
                            time.sleep(wait_time)
                            response = requests.get(url, headers=headers,params = params).json()
                        else:
                            print('Error getting campaign recipients')
                            print(response)
                            break
                    response= response['data']
                    response['attributes'].update({'id': response['id']})
                    response['attributes'].update({'campaign_id': campaign_id})
                    data.append(response['attributes'])
                    url = response.get('links',{}).get('next',None)
                except Exception as e:
                    print('Error getting campaign recipients')
                    traceback.print_exc()
                    url = None
                    break
        return data

    def getFlows(self):
        data = []
        url = f"https://a.klaviyo.com/api/flows"
        headers = {"accept": "application/json",
                    "revision": self.API_VERSION,
                    "content-type": "application/json",
                    "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
        params =  {"fields[flow-action]": "id,action_type,send_options",
                            "fields[flow]":"name,status,created,updated,trigger_type,archived",
                            "include":"flow-actions,tags"}
        is_first = True
        while url != None:
            try:
                if is_first:
                    response = requests.get(url, headers=headers,params=params).json()
                    is_first = False
                else:
                    response = requests.get(url, headers=headers).json()
                while 'errors' in response:
                    if 'throttled' in [error['code'] for error in response['errors']]:
                        error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                        wait_time = int(re.search(r'\d+',error_details).group())
                        print(f"Throttled, waiting for {wait_time} second")
                        time.sleep(wait_time)
                        response = requests.get(url, headers=headers,params = params).json()
                    else:
                        print('Error getting flows')
                        print(response)
                        break
                data += response['data']
                url = response.get('links',{}).get('next',None)
            except Exception as e:
                print('Error getting flows')
                traceback.print_exc()
                url = None
                break
        return data

    def getFlowActions(self,action_ids):
        data = []
        for action_id in action_ids:
            temp_data = []
            url = f"https://a.klaviyo.com/api/flow-actions/{action_id}/flow-messages/"
            headers = {"accept": "application/json",
                        "revision": self.API_VERSION,
                        "content-type": "application/json",
                        "Authorization": f"Klaviyo-API-Key {self.PRIVATE_API_KEY}"}
            params = {"fields[flow-message]":"id,name"}
            is_first = True
            while url != None:
                try:
                    if is_first:
                        response = requests.get(url, headers=headers,params=params).json()
                        is_first = False
                    else:
                        response = requests.get(url, headers=headers).json()
                    while 'errors' in response:
                        if 'throttled' in [error['code'] for error in response['errors']]:
                            error_details = [error['detail'] for error in response['errors'] if error['code'] == 'throttled'][0]
                            wait_time = int(re.search(r'\d+',error_details).group())
                            print(f"Throttled, waiting for {wait_time} second")
                            time.sleep(wait_time)
                            response = requests.get(url, headers=headers,params = params).json()
                        else:
                            print('Error getting flow actions')
                            print(response)
                            break
                    temp_data += response['data']
                    url = response.get('links',{}).get('next',None)
                except Exception as e:
                    print('Error getting flow actions')
                    print(response)
                    traceback.print_exc()
                    url = None
                    break
            for x in temp_data:
                x['action_id'] = action_id
            data += temp_data
        return data

    def getCampaignIds(self,campaignTable,BQdataset = None):
        if BQdataset == None:
            BQdataset = self.BQdataset
        client = bigquery.Client.from_service_account_info(self.adminServiceAccountCredential)
        try:
            table_ref = client.dataset(BQdataset).table(campaignTable)
            try:
                QUERY = f"SELECT DISTINCT id FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` WHERE DATE_DIFF(CURRENT_DATE(),DATE(send_time),DAY) < 10"    
                query_job = client.query(QUERY)
                result = query_job.result().to_dataframe().to_dict(orient='records')
                return [campaign['id'] for campaign in result]
            except Exception as e:
                print('Error getting campaign ids')
                traceback.print_exc()
        except Exception as e:
            print('Flow table was not found in the location provided')
            traceback.print_exc()
    
    def getFlowIds(self,flowTable,BQdataset = None):
        if BQdataset == None:
            BQdataset = self.BQdataset
        return [flow['id'] for flow in self.getDataFromBigQuery(flowTable,['id'])]
    
    def getSegmentIds(self,segmentTable,BQdataset = None):
        if BQdataset == None:
            BQdataset = self.BQdataset
        return [segment['id'] for segment in self.getDataFromBigQuery(segmentTable,['id'])]
    
    def getListIds(self,listTable,BQdataset = None):
        if BQdataset == None:
            BQdataset = self.BQdataset
        return [list['id'] for list in self.getDataFromBigQuery(listTable,['id'])]
    
    def getMetricIds(self,metricTable,BQdataset = None):
        if BQdataset == None:
            BQdataset = self.BQdataset
        client = bigquery.Client.from_service_account_info(self.adminServiceAccountCredential)
        try:
            table_ref = client.dataset(BQdataset).table(metricTable)
            try:
                QUERY = f"SELECT DISTINCT id FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` WHERE REGEXP_CONTAINS(LOWER(name),'(email|sms|order)')"    
                query_job = client.query(QUERY)
                result = query_job.result().to_dataframe().to_dict(orient='records')
                return [metric['id'] for metric in result]
            except Exception as e:
                print('Error getting metric ids')
                traceback.print_exc()
        except Exception as e:
            print('Flow table was not found in the location provided')
            traceback.print_exc()
    
    def getFlowActionIds(self,flowTable = 'raw_Klaviyo_Flows',BQdataset = None):
        if BQdataset == None:
            BQdataset = self.BQdataset
        client = bigquery.Client.from_service_account_info(self.adminServiceAccountCredential)
        try:
            table_ref = client.dataset(BQdataset).table(flowTable)
            try:
                QUERY = f"SELECT DISTINCT actions.id FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`, UNNEST(relationships.flow_actions.data) AS actions"    
                query_job = client.query(QUERY)
                result = query_job.result().to_dataframe().to_dict(orient='records')
                return [int(action['id']) for action in result]
            except Exception as e:
                print('Error getting flow action ids')
                traceback.print_exc()
        except Exception as e:
            print('Flow table was not found in the location provided')
            traceback.print_exc()


    
    
    