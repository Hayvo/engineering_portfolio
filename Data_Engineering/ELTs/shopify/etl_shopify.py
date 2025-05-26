import requests 
import datetime
import traceback 
import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader
from Utils.GoogleCloudHandlers.cloud_storage_handler import CloudStorageHandler
import json

class ShopifyETL():
    def __init__(self,
                 adminServiceAccount,
                 shopifyCredentials,
                 defaultStartTime = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%dT%H:%M:%S%z'),
                 defaultEndTime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z'),
                 debug=False):
        self.adminServiceAccount = adminServiceAccount
        self.API_KEY = shopifyCredentials['shopify']['API_KEY']
        self.API_TOKEN = shopifyCredentials['shopify']['API_TOKEN']
        self.MERCHANT = shopifyCredentials['shopify']['MERCHANT']
        self.project_id = adminServiceAccount['project_id']
        self.bigQueryLoader = BigQueryLoader(adminServiceAccount=adminServiceAccount,debug=debug)
        self.defaultStartTime = defaultStartTime
        self.defaultEndTime = defaultEndTime
        self.VERSION = '2024-10'

    def loadDataToBigQuery(self,data,BQtable,BQdataset = 'marketplace_Shopify',base_table = None,force_schema = False,WRITE_DISPOSITION = 'WRITE_TRUNCATE'):
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
            self.bigQueryLoader.loadDataToBQ(data,BQdataset,BQtable,platform= 'shopify',base_table=base_table,force_schema=force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            traceback.print_exc()

    def fetchQueryData(self,object,param):
        """
        Fetch data from Shopify API \n
        Args:
            object (str): Object to fetch
            param (dict): Parameters to use
        Returns:
            data (list): Data fetched
        """
        last = 0
        data = []
        param['limit'] = 250
        data_length = 0
        while True:
            param['since_id'] = last
            url = f"https://{self.API_KEY}:{self.API_TOKEN}@{self.MERCHANT}.myshopify.com/admin/api/{self.VERSION}/{object}.json"
            response = requests.request("GET", url,params= param)
            try:
                new_data = response.json()[object] 
                data += new_data
                data_length += len(new_data)
                last=data[-1]['id']
                if len(new_data)<250 or param['since_id'] == last:
                    break
            except Exception:
                break
        return(data)
    
    def getOrders(self,status = 'any',updated_at_min = None, updated_at_max = None):
        """
        Get Orders from Shopify API \n
        Args:
            status (str): Order status to fetch
            updated_at_min (str): Minimum updated time
            updated_at_max (str): Maximum updated time
        Returns:
            data (list): Orders fetched
        """
        object = 'orders'
        if updated_at_min is None:
            updated_at_min = self.defaultStartTime
        param = {"status":status,
                 "updated_at_min":updated_at_min,
                 "updated_at_max":updated_at_max}
        return self.fetchQueryData(object,param)
    
    def getCustomers(self,updated_at_min = None):
        object = 'customers'
        if updated_at_min is None:
            updated_at_min = self.defaultStartTime
        param = {"updated_at_min":updated_at_min}
        return self.fetchQueryData(object,param)
    
    def getProducts(self):
        object = 'products'
        param = {}
        return self.fetchQueryData(object,param)
    
    def getLocations(self):
        object = 'locations'
        param = {}
        return self.fetchQueryData(object,param)
    
    def getInventoryLevels(self):
        object = 'inventory_levels'
        param = {}
        return self.fetchQueryData(object,param)
    
    def getInventoryItems(self):
        object = 'inventory_items'
        param = {}
        return self.fetchQueryData(object,param)
    

class ShopifyETLv2():
    def __init__(self,
                 adminServiceAccount,
                 shopifyCredentials,
                 defaultStartTime = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%dT%H:%M:%S%z'),
                 defaultEndTime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z'),
                 debug=False):
        self.adminServiceAccount = adminServiceAccount
        self.API_KEY = shopifyCredentials['shopify']['API_KEY']
        self.API_TOKEN = shopifyCredentials['shopify']['API_TOKEN']
        self.MERCHANT = shopifyCredentials['shopify']['MERCHANT']
        self.project_id = adminServiceAccount['project_id']
        self.bigQueryLoader = BigQueryLoader(adminServiceAccount=adminServiceAccount,debug=debug)
        self.defaultStartTime = defaultStartTime
        self.defaultEndTime = defaultEndTime
        self.VERSION = '2024-10'
      

    def loadDataToBigQuery(self,data,BQtable,BQdataset = 'marketplace_Shopify',base_table = None,force_schema = False,WRITE_DISPOSITION = 'WRITE_TRUNCATE'):
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
            self.bigQueryLoader.loadDataToBQ(data,BQdataset,BQtable,platform= 'shopify',base_table=base_table,force_schema=force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            traceback.print_exc()
        
    def getOrders(self,status = 'any',updated_at_min = None, updated_at_max = None):
        """
        Get Orders from Shopify API \n
        Args:
            status (str): Order status to fetch
            updated_at_min (str): Minimum updated time
            updated_at_max (str): Maximum updated time
        Returns:
            data (list): Orders fetched
        """
        
        url = f"https://{self.MERCHANT}.myshopify.com/admin/api/{self.VERSION}/graphql.json"
        headers = {
            'Content-Type': 'application/graphql',
            'X-Shopify-Access-Token': self.API_TOKEN
        }

        query = """
{orders(first: 10){edges{node{}}}} 
"""
                 
        
        response = requests.post(url, headers=headers, data=query)
        print(response.content)