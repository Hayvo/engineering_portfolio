import requests 
import datetime
import traceback 
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader
from Utils.GoogleCloudHandlers.cloud_storage_handler import CloudStorageHandler
import time,json,yaml

class YotpoETL():
    def __init__(self,
                 storageServiceAccountCredential,
                 defaultStartTime = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%dT%H:%M:%S%z'),
                 defaultEndTime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z'),
                 debug=False):
        self.storageServiceAccountCredential = storageServiceAccountCredential
        self.project_id = storageServiceAccountCredential['project_id']
        self.bigQueryLoader = BigQueryLoader(adminServiceAccount=self.storageServiceAccountCredential,debug=debug)
        self.cloudStorageHandler = CloudStorageHandler(storageServiceAccountCredential)
        yotpoCredentials = self.getYotpoCredentials()
        self.APP_KEY = yotpoCredentials['APP_KEY']
        self.SECRET_KEY = yotpoCredentials['SECRET_KEY']
        self.CLIENT_ID = yotpoCredentials['CLIENT_ID']
        self.ACCESS_TOKEN = yotpoCredentials['ACCESS_TOKEN']

        self.defaultStartTime = defaultStartTime
        self.defaultEndTime = defaultEndTime
              

    def getYotpoCredentials(self):
        f = self.cloudStorageHandler.get_stored_file('yotpo_stored_params','login_yotpo.yml')
        conf = yaml.load(f,Loader=yaml.CLoader)           
        APP_KEY = conf['yopto']['APP_KEY']
        SECRET_KEY = conf['yopto']['SECRET_KEY']
        url = f"https://api.yotpo.com/core/v3/stores/{APP_KEY}/access_tokens"
        payload = {"secret": SECRET_KEY}
        headers = {"accept": "application/json","Content-Type": "application/json"}
        response = requests.post(url, headers=headers, json=payload)
        # print(response.json())
        ACCESS_TOKEN = response.json()['access_token']
        conf['yopto']['ACCESS_TOKEN'] = ACCESS_TOKEN
        return conf['yopto']
                
    def loadDataToBigQuery(self,data,BQtable,BQdataset = 'marketplace_Yotpo',force_schema = False,WRITE_DISPOSITION = 'WRITE_TRUNCATE'):
        try:
            self.bigQueryLoader.loadDataToBQ(data,BQdataset,BQtable,platform='yotpo',force_schema=force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            traceback.print_exc()

    def updateToken(self):
        url = "https://api.yotpo.com/oauth/token"
        payload = f"client_id={self.CLIENT_ID}&client_secret={self.SECRET_KEY}&grant_type=client_credentials"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        response = requests.request("POST", url, headers=headers, data = payload)
        return response.json()['access_token']
    
    def fetchQueryData(self,query):
        data= []
        new_page = ''
        query['params']['limit (100)'] = 100
        if query['version'] == 'v3':
            pages = {}
        elif query['version'] == 'v1':
            query['params']['page'] = 1
        while True:
            headers = {
                "accept": "application/json",
                "Content-Type": "application/json",
                "X-Yotpo-Token": f"{self.ACCESS_TOKEN}"}
            response = requests.request("GET", query['url'],params= query['params'], headers=headers).json()
            # print(response)
            try:
                new_data = response[query['object']] 
                data += new_data
                print('New data length : ',len(data))
                if query['version'] == 'v3':
                    if len(new_data)<100 or pages.get(response['pagination']['next_page_info'],False):
                        print('Fetching completed')
                        break
                    new_page=response['pagination']['next_page_info']
                    pages[new_page] = True
                    param = {}
                    param['page_info'] = new_page
                else:
                    if len(new_data)<100:
                        print('Fetching completed')
                        break
                    query['params']['page'] += 1
            except Exception:
                print('Error in response')
                traceback.print_exc()

                # print(response)
                break
        return(data)
    

    def getReviews(self,deleted = False,updated_at_min = None):
        if updated_at_min is None:
            updated_at_min = self.defaultStartTime
        if deleted:
            deleted = 'true'
        else:
            deleted = 'false'
        query = {'url' : f"https://api.yotpo.com/v1/apps/{self.APP_KEY}/reviews",
                 'params' : {'count': 100,
                           'since_updated_at': updated_at_min,
                            'deleted': deleted},
                 'version' : 'v1'}
        return self.fetchQueryData(query)
    
    def getOrders(self,order_date_min = None):
        if order_date_min is None:
            order_date_min = self.defaultStartTime
        query = {'url' : f"https://api.yotpo.com/core/v3/stores/{self.APP_KEY}/orders",
                 'params' : {'order_date_min': order_date_min},
                 'version' : 'v3'}
        return self.fetchQueryData(query)
    
    def getAggregation(self,dimensions,measures,filters= [],sort = []):
        dim = 'dimensions[]='+'&dimensions[]='.join(dimensions)
        mes = 'measures[]='+'&measures[]='.join(measures)
        headers = {
                    "accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Yotpo-Token": f"{self.ACCESS_TOKEN}"}
        if filters == []:
            url = f"https://api.yotpo.com/messaging/v3/stores/{self.APP_KEY}/analytics/query/aggregations?{dim}&{mes}"
            try:
                response = requests.get(url, headers=headers)
                status = response.status_code
                if status == 200:
                    return response.json()
                else:
                    print(f"Error in response : {status}")
                    print(response.text)
                    return None
            except Exception:
                print('Error in response')
                traceback.print_exc()
                return None
        else:
            data = []
            for i,f in enumerate(filters):
                print(f"Fetching data for filter {i+1}/{len(filters)} : {f}")
                fil = 'filters[]='+'&filters[]='.join(f)
                url = f"https://api.yotpo.com/messaging/v3/stores/{self.APP_KEY}/analytics/query/aggregations?{dim}&{mes}&{fil}"
                try:
                    response = requests.get(url, headers=headers)
                    status = response.status_code
                    if status == 200:
                        data += response.json()
                    else:
                        print(f"Error in response : {status}")
                        print(response.text)
                        break
                except Exception:
                    print('Error in response')
                    traceback.print_exc()
                    break
            return data
        
    def getAggregationMeasures(self):
        url = f'https://api.yotpo.com/messaging/v3/stores/{self.APP_KEY}/analytics/explore/measures'
        headers = {"accept": "application/json","Content-Type": "application/json","X-Yotpo-Token": f"{self.ACCESS_TOKEN}"}
        try:
            response = requests.get(url, headers=headers)
            status = response.status_code
            if status == 200:
                return response.json()
            else:
                print(f"Error in response : {status}")
                print(response.text)
                return None
        except Exception:
            print('Error in response')
            traceback.print_exc()
            return None
        
    def getAggregationDimensions(self):
        url = f'https://api.yotpo.com/messaging/v3/stores/{self.APP_KEY}/analytics/explore/dimensions'
        headers = {"accept": "application/json","Content-Type": "application/json","X-Yotpo-Token": f"{self.ACCESS_TOKEN}"}
        try:
            response = requests.get(url, headers=headers)
            status = response.status_code
            if status == 200:
                return response.json()
            else:
                print(f"Error in response : {status}")
                print(response.text)
                return None
        except Exception:
            print('Error in response')
            traceback.print_exc()
            return None