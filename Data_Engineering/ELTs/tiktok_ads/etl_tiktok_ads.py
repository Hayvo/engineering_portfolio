import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader
import requests
import yaml 
from datetime import datetime, timedelta
import traceback

class TikTokAdsETL:
    def __init__(self, adminServiceAccountCredential, tikTokAdsCredentials, debug=False, default_start_date = None, default_end_date = None):
        
        """
        TikTok Ads ETL \n
        Args:
            adminServiceAccountCredential (dict): Admin Service Account Credential
            tikTokAdsCredentials (dict): TikTok Ads Credentials
            debug (bool): Debug mode
            default_start_date (str): Default start date
            default_end_date (str): Default end date
        """

        self.adminServiceAccountCredential = adminServiceAccountCredential
        self.project_id = adminServiceAccountCredential['project_id']
        self.debug = debug
        self.API_ID = tikTokAdsCredentials['tiktok_ads']['APP_ID']
        self.ACCESS_TOKEN = tikTokAdsCredentials['tiktok_ads']['ACCESS_TOKEN']
        self.API_SECRET = tikTokAdsCredentials['tiktok_ads']['SECRET']
        self.ACCOUNT_ID = tikTokAdsCredentials['tiktok_ads']['ACCOUNT_ID']
        self.bigQueryLoader = BigQueryLoader(self.adminServiceAccountCredential, debug=self.debug)
        self.API_URL = "https://business-api.tiktok.com/open_api/v1.3"

        self.default_start_date = default_start_date if default_start_date is not None else (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        self.default_end_date = default_end_date if default_end_date is not None else datetime.today().strftime("%Y-%m-%d")

    def loadDataToBigQuery(self,data,BQtable,BQdataset = 'marketplace_TikTok_Ads',base_table = None,force_schema = False,WRITE_DISPOSITION = 'WRITE_TRUNCATE'):
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
            self.bigQueryLoader.loadDataToBQ(data,BQdataset,BQtable,platform= 'tiktok',base_table=base_table,force_schema=force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            traceback.print_exc()

    def getAds(self, advertiser_id : str = None, creation_filter_start_time : str = None, creation_filter_end_time : str = None,
               fields : list = None, exclude_field_types_in_response : list = None, filtering : dict = None, page : int = 1,
               page_size : int = 1000):
        """
        Get ads from TikTok API \n
        All time in the format of YYYY-MM-DD HH:MM:SS (UTC time zone) \n
        Args:
            advertiser_id (str): Advertiser ID
            creation_filter_start_time (str): Creation filter start time
            creation_filter_end_time (str): Creation filter end time
            fields (list): Fields to get.
            exclude_field_types_in_response (list): Field types to exclude
            filtering (dict): Filtering
            page (int): Page
            page_size (int): Page size 1 - 1000
        """

        if advertiser_id is None:
            advertiser_id = self.ACCOUNT_ID

        data = []

        url = f"{self.API_URL}/ad/get/" 
        headers = {
            "Content-Type": "application/json",
            "Access-Token": self.ACCESS_TOKEN}
        
        if filtering is None and (creation_filter_start_time is not None or creation_filter_end_time is not None):
            filtering = {}
            filtering["creation_filter_start_time"] = creation_filter_start_time
            filtering["creation_filter_end_time"] = creation_filter_end_time

        params = {
            "advertiser_id": advertiser_id,
            "fields": str(fields).replace("'", '"'),
            "exclude_field_types_in_response": exclude_field_types_in_response,
            "filtering": filtering,
            "page": page,
            "page_size": page_size
        }

        while True:
            try:
                response = requests.get(url, headers=headers, params=params)
                response = response.json()

                data += response["data"]["list"] 

                paging = response["data"]["page_info"]
                if paging["page"] == paging["total_page"]:
                    break

                params["page"] += 1
            except Exception as e:
                print(response)
                traceback.print_exc()
                break

        return data
    
    def getCampaigns(self,advertiser_id : str = None, creation_filter_start_time : str = None, creation_filter_end_time : str = None,
               fields : list = None, exclude_field_types_in_response : list = None, filtering : dict = None, page : int = 1,
               page_size : int = 1000):
        """
        Get campaigns from TikTok API \n
        All time in the format of YYYY-MM-DD HH:MM:SS (UTC time zone) \n
        Args:
            advertiser_id (str): Advertiser ID
            creation_filter_start_time (str): Creation filter start time
            creation_filter_end_time (str): Creation filter end time
            fields (list): Fields to get.
            exclude_field_types_in_response (list): Field types to exclude
            filtering (dict): Filtering
            page (int): Page
            page_size (int): Page size 1 - 1000
        """

        if advertiser_id is None:
            advertiser_id = self.ACCOUNT_ID

        data = []

        url = f"{self.API_URL}/campaign/get/"
        headers = {
            "Content-Type": "application/json",
            "Access-Token": self.ACCESS_TOKEN}
        
        if filtering is None and (creation_filter_start_time is not None or creation_filter_end_time is not None):
            filtering = {}
            filtering["creation_filter_start_time"] = creation_filter_start_time
            filtering["creation_filter_end_time"] = creation_filter_end_time

        params = {
            "advertiser_id": advertiser_id,
            "fields": str(fields).replace("'", '"'),
            "exclude_field_types_in_response": exclude_field_types_in_response,
            "filtering": filtering,
            "page": page,
            "page_size": page_size
        }

        while True:
            try:
                response = requests.get(url, headers=headers, params=params)
                response = response.json()

                data += response["data"]["list"] 

                paging = response["data"]["page_info"]
                if paging["page"] == paging["total_page"]:
                    break

                params["page"] += 1
            except Exception as e:
                traceback.print_exc()
                print(response)
                break

        return data
    
    def runSynchronousReport(self,advertiser_id :str = None, advertiser_ids : list = None,bc_id : str = None,service_type : str = "AUCTION",
                             report_type : str = "BASIC", data_level : str = "AUCTION_CAMPAIGN", dimensions : list = ["campaign_id", "stat_time_day"],
                              metrics : list = ["spend", "impressions"],  enable_total_metrics : bool = False, start_date : str = None, end_date : str = None,
                              query_lifetime : bool = False, multi_adb_report_in_utc_time : bool = False, order_field : str = None, order_type : str = "DESC", 
                              filtering : list = None, page : int = 1, page_size : int = 1000):
        """
        Run synchronous report from TikTok API \n
        All time in the format of YYYY-MM-DD (UTC time zone) \n
        
        Args:
            advertiser_id (str) : Advertiser ID
            advertiser_ids (list): Advertiser IDs
            bc_id (str): Business Center ID
            service_type (str): Service type. AUCTION or RESERVATION
            report_type (str): Report type. BASIC, AUDIENCE, PLAYABLE_MATERIAL, CATALOG, BC, TT_SHOP
            data_level (str): Data level. AUCTION_AD, AUCTION_ADGROUP, AUCTION_CAMPAIGN, AUCTION_ADVERTISER, RESERVATION_AD, RESERVATION_ADGROUP, RESERVATION_CAMPAIGN, RESERVATION_ADVERTISER
            dimensions (list): Dimensions. advertiser_id, campaign_id, adgroup_id, ad_id, stat_time_day, stat_time_hour, country_code,, ad_typr, custom_event_type, event_source_id, page_id, component_name, room_id, post_id, image_id, video_material_id, search_terms
            metrics (list): Metrics. spend, billed_cost, cash_spend, voucher_spend, cpc, cpm, impressions, gross_impressions, clicks, ctr, reach, cost_per_1000_reach, frequency, conversion, cost_per_conversion, conversion_rate, conversion_rate_v2, real_time_conversion, real_time_cost_per_conversion, real_time_conversion_rate, real_time_conversion_rate_v2, result, cost_per_result, result_rate, real_time_result, real_time_cost_per_result, real_time_result_rate, real_time_result_rate, real_time_result_rate, real_time_result_rate
            enable_total_metrics (bool): Enable total metrics
            start_date (str): Start date
            end_date (str): End date
            query_lifetime (bool): Query lifetime
            multi_adb_report_in_utc_time (bool): Multi adb report in UTC time
            order_field (str): Order field
            order_type (str): Order type
            filtering (list): List of filters. Eg : {"field_name": "stat_time_day", "filter_type": "BETWEEN", "filter_value": ["2021-01-01", "2021-01-31"]}
            page (int): Page
            page_size (int): Page size 1 - 1000

        Returns:
            dict: Report data

        Refer to https://business-api.tiktok.com/portal/docs?id=1740302848100353 for more information
        """

        if advertiser_id == advertiser_ids == bc_id == None:
            raise ValueError("advertiser_id, advertiser_ids or bc_id must be provided")

        data = []
        url = f"{self.API_URL}/report/integrated/get/"
        headers = {
            "Content-Type": "application/json",
            "Access-Token": self.ACCESS_TOKEN}
        params = {
            "advertiser_id": advertiser_id,
            "advertiser_ids": advertiser_ids,
            "bc_id": bc_id,
            "service_type": service_type,
            "report_type": report_type,
            "data_level": data_level,
            "dimensions": dimensions,
            "metrics": metrics,
            "enable_total_metrics": enable_total_metrics,
            "start_date": start_date,
            "end_date": end_date,
            "query_lifetime": query_lifetime,
            "multi_adb_report_in_utc_time": multi_adb_report_in_utc_time,
            "order_field": order_field,
            "order_type": order_type,
            "filtering": filtering,
            "page": page,
            "page_size": page_size
        }

        while True:
            try:
                response = requests.get(url, headers=headers, params=params)
                response = response.json()
                
                new_data = [x["metrics"] | x["dimensions"] for x in response["data"]["list"]]
                
                data += new_data 

                paging = response["data"]["page_info"]
                if paging["page"] == paging["total_page"]:
                    break

                params["page"] += 1
            except Exception as e:
                traceback.print_exc()
                print(response)
                break

        return data
    
    def getCampaignInsights(self,advertiser_id : str = None, campaign_ids : list = None, start_date : str = None, end_date : str = None, query_lifetime : bool = False):
        """
        Get campaign insights from TikTok API \n
        All time in the format of YYYY-MM-DD (UTC time zone) \n
        Args:
            advertiser_id (str): Advertiser ID
            campaign_ids (list): Campaign IDs
            start_date (str): Start date
            end_date (str): End date
        """

        if advertiser_id is None:
            advertiser_id = self.ACCOUNT_ID

        if campaign_ids is not None:
            filtering = [{"field_name": "campaign_id", "filter_type": "IN", "filter_value": campaign_ids}]
        else:
            filtering = None

        if start_date is None:
            start_date = self.default_start_date
        
        if end_date is None:
            end_date = self.default_end_date

        dimensions = ["campaign_id"]
        extra_dimensions = ["stat_time_day","country_code"]
        if not(query_lifetime):
            dimensions += extra_dimensions
        dimensions = str(dimensions).replace("'", '"')

        metrics = ["advertiser_id","campaign_name","spend", "impressions", "clicks", "conversion","purchase","total_purchase_value"]
        metrics = str(metrics).replace("'", '"')

        try:
            data = self.runSynchronousReport(advertiser_id = advertiser_id, service_type = "AUCTION", report_type= "BASIC", data_level = "AUCTION_CAMPAIGN",
                                         dimensions = dimensions, metrics = metrics, enable_total_metrics = False,
                                         start_date = start_date, end_date = end_date, query_lifetime = query_lifetime, filtering = filtering)
        except Exception as e:
            traceback.print_exc()
            data = []
            
        return data
    
    def getAdsInsights(self,advertiser_id : str = None, ads_ids : list = None, start_date : str = None, end_date : str = None, query_lifetime : bool = False):
        """
        Get ad insights from TikTok API \n
        All time in the format of YYYY-MM-DD (UTC time zone) \n
        Args:
            advertiser_id (str): Advertiser ID
            ads_ids (list): Ad IDs
            start_date (str): Start date
            end_date (str): End date
        """

        if advertiser_id is None:
            advertiser_id = self.ACCOUNT_ID

        if ads_ids is not None:
            filtering = [{"field_name": "ad_id", "filter_type": "IN", "filter_value": ads_ids}]
        else:
            filtering = None

        if start_date is None:
            start_date = self.default_start_date
        
        if end_date is None:
            end_date = self.default_end_date

        dimensions = ["ad_id"]
        extra_dimensions = ["stat_time_day","country_code"]
        if not(query_lifetime):
            dimensions += extra_dimensions
        dimensions = str(dimensions).replace("'", '"')

        metrics = ["advertiser_id","campaign_name","spend", "impressions", "clicks", "conversion","purchase","total_purchase_value"]
        metrics = str(metrics).replace("'", '"')

        try:
            data = self.runSynchronousReport(advertiser_id = advertiser_id, service_type = "AUCTION", report_type= "BASIC", data_level = "AUCTION_AD",
                                         dimensions = dimensions, metrics = metrics, enable_total_metrics = False,
                                         start_date = start_date, end_date = end_date, query_lifetime = query_lifetime, filtering = filtering)
        except Exception as e:
            traceback.print_exc()
            data = []
            
        return data