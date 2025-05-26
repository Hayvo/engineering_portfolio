import sys
import sys, os
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
import requests
import traceback
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader
from Utils.GoogleCloudHandlers.cloud_storage_handler import CloudStorageHandler
from datetime import datetime, timedelta

class MasonHubETL():
    def __init__(self,
                 storageServiceAccountCredential,
                 masonHubCredential,
                 basePath = 'v1',
                 defaultStartTime = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%dT%H:%M:%S%z'),
                 defaultEndTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z'),
                 debug = False,):
        self.storageServiceAccountCredential = storageServiceAccountCredential
        self.project_id = storageServiceAccountCredential['project_id']
        self.bigQueryLoader = BigQueryLoader(adminServiceAccount=self.storageServiceAccountCredential,debug=debug)
        self.cloudStorageHandler = CloudStorageHandler(storageServiceAccountCredential)
        self.masonHubCrededentials = "token"
        self.defaultStartTime = defaultStartTime
        self.defaultEndTime = defaultEndTime
        self.debug = debug
        self.account_slug = masonHubCredential['masonhub']['account_slug']
        self.bearer_auth = masonHubCredential['masonhub']['bearer_auth']
        self.API_URL = f"https://app.masonhub.co/{self.account_slug}/api/{basePath}"

    def loadDataToBigQuery(self,data,BQtable,BQdataset = 'marketplace_MasonHub',base_table = None,force_schema = False,WRITE_DISPOSITION = 'WRITE_TRUNCATE'):
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
            self.bigQueryLoader.loadDataToBQ(data,BQdataset,BQtable,platform= 'masonhub',base_table=base_table,force_schema=force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            traceback.print_exc()

    def getSkus(self,id = None,cid = None,list_type="details",limit=100,offset=0,include_counts=True):
        """Get SKUs from MasonHub API
        Args:
            id (list) : SKU IDs
            cid (list) : Customer SKU IDs
            list_type (str) : List type
            limit (int) : Limit
            offset (int) : Offset
            include_counts (bool) : Include counts
        Returns:
            dict : SKUs
        """
        try:
            url = self.API_URL + "/skus"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "id": id,
                "cid": cid,
                "list_type": list_type,
                "limit": limit,
                "offset": offset,
                "include_counts": include_counts
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()['data']
        except Exception as e:
            print(f"Error getting SKUs: {e}")
            traceback.print_exc()
            print(response.content)
            return None
        
    def getAllSkus(self,list_type : str = "details",incude_counts : bool = True):
        """Get All SKUs from MasonHub API"""
        try:
            data = []
            offset = 0
            while True:
                new_data = self.getSkus(list_type=list_type,include_counts=incude_counts,limit=100,offset=offset)
                offset += 100
                data += new_data
                if len(new_data) < 100:
                    break
            return data
        except Exception as e:
            print(f"Error getting All SKUs: {e}")
            traceback.print_exc()
            return None

    def getSkusInventorySnapshot(self,cid : list = None,include_ledger : str = "true",limit : int = 100,offset : int = 0):
        """Get SKUs Inventory Snapshot from MasonHub API
        Args:
            cid (list) : SKU IDs
            include_ledger (str) : Include ledger
            limit (int) : Limit
            offset (int) : Offset
        Returns:
            dict : SKUs Inventory Snapshot
        """
        try:
            url = self.API_URL + "/sku_snapshots"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "id": cid,
                "include_ledger": include_ledger,
                "limit": limit,
                "offset": offset
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()["data"]
        except Exception as e:
            print(f"Error getting SKUs Inventory Snapshot: {e}")
            traceback.print_exc()
            print(response.content)
            return None
        
    def getAllSkusInventorySnapshot(self,include_ledger : str = "true"):
        """Get All SKUs Inventory Snapshot from MasonHub API"""
        try:
            data = []
            offset = 0
            while True:
                new_data = self.getSkusInventorySnapshot(include_ledger=include_ledger,limit=100,offset=offset)
                offset += 100
                data += new_data
                if len(new_data) < 100:
                    break
            return data
        except Exception as e:
            print(f"Error getting All SKUs Inventory Snapshot: {e}")
            traceback.print_exc()
            return None
    
    def requestFullSnapshot(self,snapshot_as_of_date : str,snapshot_type : str = "full"):
        """Request a full snapshot from MasonHub API
        Args:
            snapshot_as_of_date (str) : Snapshot as of date
            snapshot_type (str) : Snapshot type
        Returns:
            dict : Full Snapshot
        """
        try:
            url = self.API_URL + "/snapshot_requests"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "snapshot_type": snapshot_type,
                "snapshot_as_of_date": snapshot_as_of_date
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()['data']
        except Exception as e:
            print(f"Error getting Full Snapshot: {e}")
            traceback.print_exc()
            print(response.content)
            return None
        
    def getInventoryLocations(self):
        """Get Inventory Locations from MasonHub API"""
        try:
            url = self.API_URL + "/inventory_locations"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "account_slug": self.account_slug
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()
        except Exception as e:
            print(f"Error getting Inventory Locations: {e}")
            traceback.print_exc()
            print(response.content)
            return None
        
    def getInboundShipmentASN(self,id : list = None, cid : list = None,cpid : list = None, sdt : str = None, edt : str = None, limit : int = 100, offset : int = 0, status : str = None, list_type : str = "detail"):  
        """Get Inbound Shipment ASN from MasonHub API
         All date/time records should be submitted in RFC3339 format ("2018-10-21T14:00:21Z" or "2018-10-21T14:00:21-05:00").
         Args:
            id (list) : Array of strings <uuid> [ 1 .. 30 ] items. Include an array of MasonHub UUID's to find.
            cid (list) : Array of strings. Include an array of customer_identifiers's to find.
            cpid (list) : Array of strings. Include an array of customer_purchase_order_id's to find.
            sdt (str) : Start date/time
            edt (str) : End date/time
            limit (int) : Limit. Default: 30. Limit the number of records returned.
            offset (int) : Offset. Default: 0. Offset the number of records returned.
            status (str) : Status. Default: null. What item status would you like to retrieve? Enum: "created" "shipped" "received" "cancelled" "all"
            list_type (str) : List type. Default: "detail". Enum: "detail" "summary"
        """

        try:
            url = self.API_URL + "/inbound_shipments"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "id": id,
                "cid": cid,
                "cpid": cpid,
                "status": status,
                "sdt": sdt,
                "edt": edt,
                "limit": limit,
                "offset": offset,
                "list_type": list_type
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()["data"]
        except Exception as e:
            print(f"Error getting Inbound Shipment ASN: {e}")
            traceback.print_exc()
            print(response.content)
            return []
        
    def getAllInboundShipmentASN(self,sdt : str = None, edt : str = None):
        """Get All Inbound Shipment ASN from MasonHub API"""
        try:
            data = []
            offset = 0
            while True:
                new_data = self.getInboundShipmentASN(limit=100,offset=offset,sdt=sdt,edt=edt)
                offset += 100
                data += new_data
                if len(new_data) < 100:
                    break
            return data
        except Exception as e:
            print(f"Error getting All Inbound Shipment ASN: {e}")
            traceback.print_exc()
            return []

    def getShipments(self,id : list = None, coid : list = None, offset : int = 0, limit : int = 100, sdt : str = None, edt : str = None):
        """Get Shipmens from MasonHub API """
        try:
            url = self.API_URL + "/shipments"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "id": id,
                "coid":coid,
                "sdt":sdt,
                "edt":edt,
                "offset": offset,
                "limit": limit,
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()["data"]
        except Exception as e:
            print(f"Error getting Orders: {e}")
            traceback.print_exc()
            print(response.content)
            return []

    def getAllShipments(self,sdt : str = None, edt : str = None):
        """Get All Shipments from MasonHub API"""
        try:
            data = []
            offset = 0
            while True:
                new_data = self.getShipments(offset=offset,sdt=sdt,edt=edt)
                offset += 100
                data += new_data
                if len(new_data) < 100:
                    break
            return data
        except Exception as e:
            print(f"Error getting All Orders: {e}")
            traceback.print_exc()
            return []

    def getOrders(self,id : list = None, cid : list = None, offset : int = 0, limit : int = 100, list_type : str = "detail", sdt : str = None):
        """Get Orders from MasonHub API
           All date/time records should be submitted in RFC3339 format ("2018-10-21T14:00:21Z" or "2018-10-21T14:00:21-05:00")."""
        try:
            url = self.API_URL + "/orders"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "id": id,
                "cid": cid,
                "list_type": list_type,
                "limit": limit,
                "offset": offset,
                "sdt": sdt
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()["data"]
        except Exception as e:
            print(f"Error getting Orders: {e}")
            traceback.print_exc()
            print(response.content)
            return []
        
    def getAllOrders(self,list_type : str = "detail",sdt : str = None):
        """Get All Orders from MasonHub API"""
        try:
            data = []
            offset_pad = 30
            offset = 0
            while True:
                new_data = self.getOrders(list_type=list_type,sdt=sdt,limit=offset_pad,offset=offset)
                offset += offset_pad
                data += new_data
                if len(new_data) < offset_pad:
                    break
            return data
        except Exception as e:
            print(f"Error getting All Orders: {e}")
            traceback.print_exc()
            return []

    def getOrderUpdates(self, id : list = None, cid : list = None, offset : int = 0, limit : int = 100):
        """Get Order Updates from MasonHub API"""
        try:
            url = self.API_URL + "/order_update_requests"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "id": id,
                "cid": cid,
                "limit": limit,
                "offset": offset,
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()["data"]
        except Exception as e:
            print(f"Error getting Order Updates: {e}")
            traceback.print_exc()
            print(response.content)
            return []
        
    def getAllOrderUpdates(self):
        """Get All Order Updates from MasonHub API"""
        try:
            data = []
            offset = 0
            while True:
                new_data = self.getOrderUpdates(limit=100,offset=offset)
                offset += 100
                data += new_data
                if len(new_data) < 100:
                    break
            return data
        except Exception as e:
            print(f"Error getting All Order Updates: {e}")
            traceback.print_exc()
            return []

    def getOrderCancels(self,id : list = None, cid : list = None, offset : int = 0, limit : int = 100):
        """Get Order Cancels from MasonHub API"""
        try:
            url = self.API_URL + "/order_cancel_requests"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "id": id,
                "cid": cid,
                "limit": limit,
                "offset": offset,
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()["data"]
        except Exception as e:
            print(f"Error getting Order Cancels: {e}")
            traceback.print_exc()
            print(response.content)
            return []
        
    def getAllOrderCancels(self):
        """Get All Order Cancels from MasonHub API"""
        try:
            data = []
            offset = 0
            while True:
                new_data = self.getOrderCancels(limit=100,offset=offset)
                offset += 100
                data += new_data
                if len(new_data) < 100:
                    break
            return data
        except Exception as e:
            print(f"Error getting All Order Cancels: {e}")
            traceback.print_exc()
            return []

    def getOrderShipments(self, id : list = None, coid : list = None, sdt : str = None, edt : str = None, limit : int = 100, offset : int = 0):
        """Get Order Shipments from MasonHub API"""
        try:
            url = self.API_URL + "/order_shipments"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "id": id,
                "coid": coid,
                "sdt": sdt,
                "edt": edt,
                "limit": limit,
                "offset": offset
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()["data"]
        except Exception as e:
            print(f"Error getting Order Shipments: {e}")
            traceback.print_exc()
            return []
        
    def getAllOrderShipments(self,sdt : str = None, edt : str = None):
        """Get All Order Shipments from MasonHub API"""
        try:
            data = []
            offset = 0
            while True:
                new_data = self.getOrderShipments(limit=100,offset=offset,sdt=sdt,edt=edt)
                offset += 100
                data += new_data
                if len(new_data) < 100:
                    break
            return data
        except Exception as e:
            print(f"Error getting All Order Shipments: {e}")
            traceback.print_exc()
            return []
        
    def getReturns(self, id : list = None, sdt : str = None, edt : str = None, status : str = None, limit : int = 100, offset : int = 0, list_type : str = "detail"):
        """Get Returns from MasonHub API"""
        try:
            url = self.API_URL + "/rmas"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "id": id,
                "status": status,
                "sdt": sdt,
                "edt": edt,
                "limit": limit,
                "offset": offset,
                "list_type": list_type
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()["data"]
        except Exception as e:
            print(f"Error getting Returns: {e}")
            traceback.print_exc()
            print(response.content)
            return []
        
    def getAllReturns(self,sdt : str = None, edt : str = None):
        """Get All Returns from MasonHub API"""
        try:
            data = []
            offset = 0
            while True:
                new_data = self.getReturns(limit=100,offset=offset,sdt=sdt,edt=edt)
                offset += 100
                data += new_data
                if len(new_data) < 100:
                    break
            return data
        except Exception as e:
            print(f"Error getting All Returns: {e}")
            traceback.print_exc()
            return []

    def getCallbackUrl(self, message_type : str = "all"):
        """Get Callback URL from MasonHub API
        message_type :
            string
            Default: "all"
            Enum: "skuInventoryChange" "orderEvent" "orderUpdateResolution" "orderCancelResolution" "inboundShipmentEvent" "rmaEvent" "all"
        """
        try:
            url = self.API_URL + "/callback_url"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            params = {
                "message_type": message_type
            }
            response = requests.get(url, headers=header, params=params)
            return response.json()
        except Exception as e:
            print(f"Error getting Callback URL: {e}")
            traceback.print_exc()
            return None
        
    def generateNewToken(self,secret_phrase : str):
        """Generate New Token from MasonHub API
        secret_phrase : str : Secret"""
        try:
            url = self.API_URL + "/secrets"
            header = {"Authorization": "Bearer " + self.bearer_auth,
                      "Content-Type": "application/json"}
            body = {
                "secret_phrase": secret_phrase
            }
            response = requests.post(url, headers=header, data=body)
            return response.json()
        except Exception as e:
            print(f"Error generating New Token: {e}")
            traceback.print_exc()
            print(response.content)
            return None