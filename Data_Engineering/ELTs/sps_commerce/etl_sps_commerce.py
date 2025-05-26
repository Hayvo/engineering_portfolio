import requests 
import yaml
import datetime
import traceback 
import xmltodict
import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader
from Utils.GoogleCloudHandlers.cloud_storage_handler import CloudStorageHandler

class SPScommerceETL():
    def __init__(self,
                 storageServiceAccountCredential,
                 defaultStartTime = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime('%Y-%m-%dT%H:%M:%S%z'),
                 defaultEndTime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z'),
                 debug=False,
                 areCredentialsLocal = False):
        self.storageServiceAccountCredential = storageServiceAccountCredential
        self.project_id = storageServiceAccountCredential['project_id']
        self.bigQueryLoader = BigQueryLoader(storageServiceAccountCredential,debug=debug)
        self.defaultStartTime = defaultStartTime
        self.defaultEndTime = defaultEndTime
        print("Retrieve SPScommerce credentials")
        storageHandler = CloudStorageHandler(self.storageServiceAccountCredential)
        try:
            if areCredentialsLocal:
                with open("./var/login_credentials/sps_login.yml", "r") as creds:
                    spsCredentials = yaml.load(creds.read(), Loader=yaml.FullLoader)
            else:
                spsCredentials = storageHandler.get_stored_file('sps_stored_params','spscommerce_login.yml')
                spsCredentials = yaml.load(spsCredentials,Loader=yaml.FullLoader)
           
            self.ACCESS_TOKEN = spsCredentials['sps_commerce']['ACCESS_TOKEN']
            self.REFRESH_TOKEN = spsCredentials['sps_commerce']['REFRESH_TOKEN']
            self.CLIENT_ID = spsCredentials['sps_commerce']['CLIENT_ID']
            self.CLIENT_SECRET = spsCredentials['sps_commerce']['CLIENT_SECRET']
            print("SPScommerce credentials retrieved")
            def refresh_token(token,refresh_token):
                
                url = f"https://api.spscommerce.com/auth-check"
                headers = {'Authorization':"Bearer " + token}
                try:
                    response = requests.post(url, headers=headers)

                    if response.status_code == 204:
                        print("Token is still valid")
                        return token, refresh_token
                    else:
                        print("Token is invalid, refreshing token")
                        url = f"https://auth.spscommerce.com/oauth/token"
                        params = {'client_id':self.CLIENT_ID,
                                'client_secret':self.CLIENT_SECRET,
                                'refresh_token':refresh_token,
                                'grant_type':'refresh_token',
                                }
                        try:
                            response = requests.post(url, params)
                            ACCESS_TOKEN = response.json()['access_token']
                            REFRESH_TOKEN = refresh_token
                            return ACCESS_TOKEN, REFRESH_TOKEN
                        except Exception as e:
                            print("Couldn't refresh token")
                            traceback.print_exc()
                except Exception as e:
                    print("Couldn't refresh token")
                    traceback.print_exc()
            self.ACCESS_TOKEN, self.REFRESH_TOKEN = refresh_token(self.ACCESS_TOKEN,self.REFRESH_TOKEN)
            storageHandler.update_stored_file('sps_stored_params','spscommerce_login.yml',yaml.dump({'sps_commerce':{'ACCESS_TOKEN':self.ACCESS_TOKEN,'REFRESH_TOKEN':self.REFRESH_TOKEN,'CLIENT_ID':self.CLIENT_ID,'CLIENT_SECRET':self.CLIENT_SECRET}}))
        except Exception as e:
            print("Couldn't retrieve SPScommerce credentials")
            traceback.print_exc()

    def loadDataToBigQuery(self,data,BQtable,BQdataset = 'marketplace_SPScommerce',force_schema = False,WRITE_DISPOSITION = 'WRITE_TRUNCATE'):
        try:
            self.bigQueryLoader.loadDataToBQ(data,BQdataset,BQtable,platform='sps',force_schema=force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            traceback.print_exc()

    def getTransactionsPaths(self,in_out=''):
        try:
            urls = [f"https://api.spscommerce.com/transactions/v5/data{in_out}"]
            headers = {'Authorization':"Bearer " + self.ACCESS_TOKEN}

            params = {'limit':1000}
            data = []
            while len(urls) > 0:
                for url in urls:
                    try:
                        response = requests.get(url, headers=headers, params=params)
                        if response.status_code == 200:
                            for dat in response.json()['results']:
                                if dat['type'] == 'file':
                                    data.append(dat['url'])
                                else:
                                    print(dat)
                                    urls.append(dat['url'])
                            if response.json().get('paging',{}).get('next',{}).get('url',None) != None:
                                urls.append(response.json()['paging']['next']['url'])
                            urls.remove(url)
                        else:
                            print("Error getting transactions")
                            print(f"Error type :" + str(response.status_code))
                            print(response.json())
                            urls = []
                            break
                    except Exception as e:
                        print("Error getting transactions")
                        traceback.print_exc()
                        break
            return data
        except Exception as e:
            print(f"Error getting transactions: {e}")
            traceback.print_exc()

    def getTransactionsHisotry(self,after = None,before = None,limit = 1000,offset = 0):
        try:
            url = f"https://api.spscommerce.com/transactions/v5/history/"
            headers = {'Authorization':"Bearer " + self.ACCESS_TOKEN}

            params = {'limit':limit,
                    'offset':offset}
            if after:
                params['after'] = after
            if before:
                params['before'] = before

            data = []
            while True:
                try:
                    response = requests.get(url, headers=headers, params=params)
                    if response.status_code == 200:
                        data += response.json()['results']
                        if response.json().get('paging',{}).get('next',{}).get('url',None) != None:
                            url = response.json()['paging']['next']['url']
                        else:
                            print("Got all transactions")
                            break
                    else:
                        print("Error getting transactions")
                        print(f"Error type :" + str(response.status_code))
                        print(response.json())
                        break
                except Exception as e:
                    print("Error getting transactions")
                    traceback.print_exc()
                    break
            return data
        except Exception as e:
            print(f"Error getting transactions: {e}")
            traceback.print_exc()

    def getTransactions(self,urls):
        try:
            data = []
            headers = {'Authorization':"Bearer " + self.ACCESS_TOKEN}
            n = len(urls)
            for i,url in enumerate(urls):
                print(f"Getting transactions {i+1}/{n}")
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        data.append(xmltodict.parse(response.text.encode('utf8'))['Order'])
                    else:
                        print("Error getting transactions")
                        print(f"Error type :" + str(response.status_code))
                        print(response.json())
                except Exception as e:
                    print("Error getting transactions")
                    traceback.print_exc()
            return data
        except Exception as e:
            print(f"Error getting transactions: {e}")
            traceback.print_exc()

    def getTradingPartners(self):
        url = f"https://api.spscommerce.com/v1/trading-partners"
        headers = {'Authorization':"Bearer " + self.ACCESS_TOKEN,
                    'Content-Type':'application/json'}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                print("Error getting trading partners")
                print(response.json())
        except Exception as e:
            print("Error getting trading partners")
            print(f"Error type :" + str(response.status_code))
            traceback.print_exc()

    def getAllSubmissionForms(self,limit = 1,offset = 0):
        url = f"https://api.spscommerce.com/v1/forms"
        headers = {'Authorization':"Bearer " + self.ACCESS_TOKEN,
                    'Content-Type':'application/json'}
        params = {'limit':limit,
                    'offset':offset}
        try:
            response = requests.get(url, headers=headers,params=params)
            if response.status_code == 200:
                return response.json()
            else:
                print("Error getting submission form")
                print(f"Error type :" + str(response.status_code))
                print(response.json())
        except Exception as e:
            print("Error getting submission form")
            traceback.print_exc()

    def getSumbissionForm(self,formId):
        url = f"https://api.spscommerce.com/v1/forms/{formId}"
        headers = {'Authorization':"Bearer " + self.ACCESS_TOKEN,
                    'Content-Type':'application/json'}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                print("Error getting submission form")
                print(response.json())
        except Exception as e:
            print("Error getting submission form")
            traceback.print_exc()

    def getGetAllShippingLabels(self,orderDesc = True,name = None,ownerName = None,ownerID = None,canRander = True):
        url = f"https://api.spscommerce.com/label/v1/"
        headers = {'Authorization':"Bearer " + self.ACCESS_TOKEN,
                    'Content-Type':'application/json'}
        params = {'orderDesc':orderDesc,
                    'name':name,
                    'ownerName':ownerName,
                    'ownerID':ownerID,
                    'canRander':canRander}
        try:
            response = requests.get(url, headers=headers,params=params)
            if response.status_code == 200:
                return response.json()
            else:
                print("Error getting shipping labels")
                print(response.json())
        except Exception as e:
            print("Error getting shipping labels")
            traceback.print_exc()

    def getShippingLabel(self,shippingLabelId):
        url = f"https://api.spscommerce.com/label/v1/{shippingLabelId}"
        headers = {'Authorization':"Bearer " + self.ACCESS_TOKEN,
                    'Content-Type':'application/json'}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                print("Error getting shipping label")
                print(response.json())
        except Exception as e:
            print("Error getting shipping label")
            traceback.print_exc()