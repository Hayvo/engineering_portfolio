import json 
import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
superPath = os.path.realpath(os.path.dirname(superPath))
sys.path.append(superPath)
from Data_Engineering.Utils.GoogleCloudHandlers.bigquery_loader import BigQueryLoader
from Utils.GoogleCloudHandlers.cloud_storage_handler import CloudStorageHandler
import requests
import yaml
import traceback
import base64

class PinterestAdsETL():
    def __init__(self, storageServiceAccountCredential, debug=False):
        self.storageServiceAccountCredential = storageServiceAccountCredential
        self.project_id = storageServiceAccountCredential['project_id']
        self.debug = debug
        self.bigQueryLoader = BigQueryLoader(self.storageServiceAccountCredential,debug=self.debug)
        self.cloudStorageHandler = CloudStorageHandler(self.storageServiceAccountCredential)
        self.getCredentials()
        self.API_URL = "https://api.pinterest.com/v5/"

    def getCredentials(self):
        """Get credentials"""
        credentials = self.cloudStorageHandler.get_stored_file(bucket='stored_params',file_name='login_pinterest.yml')
        conf = yaml.load(credentials,Loader=yaml.FullLoader)
        self.updateCredentials(conf)
    
    def updateCredentials(self,conf):
        CLIENT_SECRET_encoded = base64.b64encode((str(conf['pinterest']['CLIENT_ID'])+':'+str(conf['pinterest']['CLIENT_SECRET'])).encode()).decode()
        url = "https://api.pinterest.com/v5/oauth/token"
        headers = {"Content-Type":"application/x-www-form-urlencoded",
                "Authorization":"Basic "+CLIENT_SECRET_encoded}
        data = {"grant_type":"refresh_token",
                "refresh_token" : conf['pinterest']['REFRESH_TOKEN'],
                "refresh_on" : True}
        request = requests.post(url,headers=headers,data=data)
        try:
            conf['pinterest']['ACCESS_TOKEN'] = request.json()['access_token']
            conf['pinterest']['REFRESH_TOKEN'] = request.json()['refresh_token']
            self.cloudStorageHandler.update_stored_file(bucket='stored_params',file_name='login_pinterest.yml',data=conf)
            self.ACCESS_TOKEN = conf['pinterest']['ACCESS_TOKEN']
            self.ADVERTISER_ID = conf['pinterest']['ADVERTISER_ID']
        except Exception as e:
            print(f"Error updating credentials: {e}")
            traceback.print_exc()
        
    def loadDataToBigQuery(self,data,BQtable,BQdataset = 'marketplace_Pinterest_Ads',base_table = None,force_schema = False,WRITE_DISPOSITION = 'WRITE_TRUNCATE'):
        """Load data to BigQuery \n
        Args:
            data (list): Data to load
            BQtable (str): BigQuery table to load data to
            BQdataset (str): BigQuery dataset to load data to. Default is 'marketplace_Pinterest_Ads'
            base_table (str): Base table to use. Default is None
            force_schema (bool): Whether to force schema. Default is False
            WRITE_DISPOSITION (str): Write disposition. WRITE_TRUNCATE WRITE_APPEND. Default is 'WRITE_TRUNCATE' 
        Returns:
            None
        """
        if len(data) == 0:
            return None
        try:
            self.bigQueryLoader.loadDataToBQ(data,BQdataset,BQtable,platform= 'pinterest',base_table=base_table,force_schema=force_schema,WRITE_DISPOSITION=WRITE_DISPOSITION)
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            traceback.print_exc()

    def getCampaigns(self,campaign_ids : list = None, entity_statuses : list = None, page_size : int = 25, order : str = 'ASCENDING', bookmark : str = None):
        """Get Pinterest campaigns \n
        Args:
            campaign_ids (list) : Campaign ids
            entity_statuses (list) : ACTIVE PAUSE ARCHIVED DELETED_DRAFT
            page_size (int) : Page size, default 25, max 250
            order (str) : ASCENDING or DESCENDING. Default is 'ASCENDING'
            bookmark (str) : Bookmark used to get the next page. Default is None
        Returns:
            dict : Pinterest campaigns
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/campaigns"
        headers = {'Authorization': f'Bearer {self.ACCESS_TOKEN}',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        params = {'campaign_ids': campaign_ids,
                  'entity_statuses': entity_statuses,
                  'page_size': page_size,
                  'order': order,
                  'bookmark': bookmark}
        request = requests.get(url, headers=headers, params=params)
        response = request.json()
        try:
            data = response
            while 'bookmark' in response and response['bookmark'] != None and response['bookmark'] != bookmark:
                bookmark = response['bookmark']
                params['bookmark'] = bookmark
                request = requests.get(url, headers=headers, params=params)
                response = request.json()
                print(response)
                data += response
            return data
        except Exception as e:
            print(f"Error getting Pinterest campaigns: {e}")
            traceback.print_exc()

    def getCampaignAnalytics(self, start_date : str, end_date : str, campaign_ids : list, columns : list, 
                           granularity : str = 'DAY', click_window_days : int = 30, engagement_window_days : int = 30,
                           view_window_days : int = 1, conversion_report_time : str = 'TIME_OF_AD_ACTION'):
        """Get Pinterest campaign metrics \n
        Args:
            start_date (str) : Start date in format 'YYYY-MM-DD'
            end_date (str) : End date in format 'YYYY-MM-DD'
            campaign_ids (list) : Campaign ids
            columns (list) : Columns to get
            granularity (str) : DAY HOUR WEEK MONTH. Default is 'DAY'
            click_window_days (int) : Click window days 1 7 14 30 60. Default is 30
            engagement_window_days (int) : Engagement window days 1 7 14 30 60. Default is 30
            view_window_days (int) : View window days 1 7 14 30 60. Default is 1
            conversion_report_time (str) : TIME_OF_AD_ACTION TIME_OF_CONVERSION. Default is 'TIME_OF_AD_ACTION'
        Returns:
            dict : Pinterest campaign metrics
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/campaigns/analytics"
        headers = {'Authorization': f'Bearer {self.ACCESS_TOKEN}',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        data = {'start_date': start_date,
                'end_date': end_date,
                'campaign_ids': campaign_ids,
                'columns': columns,
                'granularity': granularity,
                'click_window_days': click_window_days,
                'engagement_window_days': engagement_window_days,
                'view_window_days': view_window_days,
                'conversion_report_time': conversion_report_time}
        request = requests.post(url, headers=headers, json=data)
        try:
            return request.json()
        except Exception as e:
            print(f"Error getting Pinterest campaign metrics: {e}")
            traceback.print_exc()

    def getCampaignTargetingAnalytics(self,campaign_ids : list, start_date : str, end_date : str,
                                      targeting_types : list, columns : list, granularity : str = 'DAY',
                                      click_window_days : int = 30, engagement_window_days : int = 30,
                                      view_window_days : int = 1, conversion_report_time : str = 'TIME_OF_AD_ACTION',
                                      attribution_types : list = None):
        """Get Pinterest campaign targeting metrics \n
        Args:
            campaign_ids (list) : Campaign ids
            start_date (str) : Start date in format 'YYYY-MM-DD'
            end_date (str) : End date in format 'YYYY-MM-DD'
            targeting_types (list) : KEYWORD APPTYPE GEO LOCATION PLACEMENT COUNTRY TARGETED_INTEREST PINNER_INTEREST AUDIENCE_INCLUDE AGE_BUCKET REGION CREATIVE_TYPE AGE
            columns (list) : Columns to get
            granularity (str) : DAY HOUR WEEK MONTH. Default is 'DAY'
            click_window_days (int) : Click window days 1 7 14 30 60. Default is 30
            engagement_window_days (int) : Engagement window days 1 7 14 30 60. Default is 30
            view_window_days (int) : View window days 1 7 14 30 60. Default is 1
            conversion_report_time (str) : TIME_OF_AD_ACTION TIME_OF_CONVERSION. Default is 'TIME_OF_AD_ACTION'
            attribution_types (list) : INDIVIDUAL HOUSE. Default is None
        Returns:
            dict : Pinterest campaign targeting metrics
        """

        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/campaigns/targeting_metrics"
        headers = {'Authorization': f'Bearer {self.ACCESS_TOKEN}',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        data = {'campaign_ids': campaign_ids,
                'start_date': start_date,
                'end_date': end_date,
                'granularity': granularity,
                'targeting_types': targeting_types,
                'columns': columns,
                'click_window_days': click_window_days,
                'engagement_window_days': engagement_window_days,
                'view_window_days': view_window_days,
                'conversion_report_time': conversion_report_time,
                'attribution_types': attribution_types}
        request = requests.post(url, headers=headers, json=data)
        try:
            return request.json()
        except Exception as e:
            print(f"Error getting Pinterest campaign targeting metrics: {e}")
            traceback.print_exc()

    def getCampaign(self,campaign_id : str):
        """Get Pinterest campaign \n
        Args:
            campaign_id (str) : Campaign id
        Returns:
            dict : Pinterest campaign
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/campaigns/{campaign_id}"
        headers = {'Authorization': f'Bearer {self.ACCESS_TOKEN}',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        request = requests.get(url, headers=headers)
        try:
            return request.json()
        except Exception as e:
            print(f"Error getting Pinterest campaign: {e}")
            traceback.print_exc()

    def getAds(self, campaign_ids : list = None, ad_group_id : list = None, ad_ids : list = None,
               entity_statuses : list = None, page_size : int = 25, order : str = 'ASCENDING', bookmark : str = None):
        """Get Pinterest ads \n
        Args:
            campaign_ids (list) : Campaign ids
            ad_group_id (list) : Ad group id
            ad_ids (list) : Ad ids
            entity_statuses (list) : ACTIVE PAUSE ARCHIVED DELETED_DRAFT
            page_size (int) : Page size, default 25, max 250.
            order (str) : ASCENDING or DESCENDING. Default is 'ASCENDING'
            bookmark (str) : Bookmark used to get the next page. Default is None
        Returns:
            dict : Pinterest ads
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/ads"
        headers = {'Authorization': f'Bearer {self.ACCESS_TOKEN}',
                     'Content-Type': 'application/json',
                     'Accept': 'application/json'}
        params = {'campaign_ids': campaign_ids,
                    'ad_group_id': ad_group_id,
                    'ad_ids': ad_ids,
                    'entity_statuses': entity_statuses,
                    'page_size': page_size,
                    'order': order,
                    'bookmark': bookmark}
        request = requests.get(url, headers=headers, params=params)
        response = request.json()
        try:
            data = response
            while 'bookmark' in response and response['bookmark'] != None and response['bookmark'] != bookmark:
                bookmark = response['bookmark']
                params['bookmark'] = bookmark
                request = requests.get(url, headers=headers, params=params)
                response = request.json()
                data += response
            return data
        except Exception as e:
            print(f"Error getting Pinterest ads: {e}")
            traceback.print_exc()

    def getAdAnalytics(self, start_date : str, end_date : str, columns : list, granularity : str = "DAY",
                       ad_ids : list = None, click_window_days : int = 30, engagement_window_days : int = 30,
                       view_window_days : int = 1, conversion_report_time : str = "TIME_OF_AD_ACTION",
                       pin_ids : list = None, campaign_ids : list = None):
        """Get Pinterest ad metrics \n
        Args:
            start_date (str) : Start date in format 'YYYY-MM-DD'
            end_date (str) : End date in format 'YYYY-MM-DD'
            columns (list) : Columns to get
            granularity (str) : DAY HOUR WEEK MONTH. Default is 'DAY'
            ad_ids (list) : Ad ids. Default is None
            click_window_days (int) : Click window days 1 7 14 30 60. Default is 30
            engagement_window_days (int) : Engagement window days 1 7 14 30 60. Default is 30
            view_window_days (int) : View window days 1 7 14 30 60. Default is 1
            conversion_report_time (str) : TIME_OF_AD_ACTION TIME_OF_CONVERSION. Default is 'TIME_OF_AD_ACTION'
            pin_ids (list) : Pin ids. Default is None
            campaign_ids (list) : Campaign ids. Default is None
        Returns:
            dict : Pinterest ad metrics
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/ads/analytics"
        headers = {'Authorization': f'Bearer {self.ACCESS_TOKEN}',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        data = {'start_date': start_date,
                'end_date': end_date,
                'columns': columns,
                'granularity': granularity,
                'ad_ids': ad_ids,
                'click_window_days': click_window_days,
                'engagement_window_days': engagement_window_days,
                'view_window_days': view_window_days,
                'conversion_report_time': conversion_report_time,
                'pin_ids': pin_ids,
                'campaign_ids': campaign_ids}
        request = requests.post(url, headers=headers, json=data)
        try:
            return request.json()
        except Exception as e:
            print(f"Error getting Pinterest ad metrics: {e}")
            traceback.print_exc()

    def getAdTargetingAnalytics(self, ad_ids : list, start_date : str, end_date : str, targeting_types : list,
                                columns : list, granularity : str = "DAY", click_window_days : int = 30,
                                engagement_window_days : int = 30, view_window_days : int = 1, conversion_report_time : str = "TIME_OF_AD_ACTION",
                                attribution_types : list = None):
        """Get Pinterest ad targeting metrics \n
        Args:
            ad_ids (list) : Ad ids
            start_date (str) : Start date in format 'YYYY-MM-DD'
            end_date (str) : End date in format 'YYYY-MM-DD'
            targeting_types (list) : KEYWORD APPTYPE GEO LOCATION PLACEMENT COUNTRY TARGETED_INTEREST PINNER_INTEREST AUDIENCE_INCLUDE AGE_BUCKET REGION CREATIVE_TYPE AGE
            columns (list) : Columns to get
            granularity (str) : DAY HOUR WEEK MONTH. Default is 'DAY'
            click_window_days (int) : Click window days 1 7 14 30 60. Default is 30
            engagement_window_days (int) : Engagement window days 1 7 14 30 60. Default is 30
            view_window_days (int) : View window days 1 7 14 30 60. Default is 1
            conversion_report_time (str) : TIME_OF_AD_ACTION TIME_OF_CONVERSION. Default is 'TIME_OF_AD_ACTION'
            attribution_types (list) : INDIVIDUAL HOUSE. Default is None
        Returns:
            dict : Pinterest ad targeting metrics
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/ads/targeting_metrics"
        headers = {'Authorization': f'Bearer { self.ACCESS_TOKEN}',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        data = {'ad_ids': ad_ids,
                'start_date': start_date,
                'end_date': end_date,
                'targeting_types': targeting_types,
                'columns': columns,
                'granularity': granularity,
                'click_window_days': click_window_days,
                'engagement_window_days': engagement_window_days,
                'view_window_days': view_window_days,
                'conversion_report_time': conversion_report_time,
                'attribution_types': attribution_types}
        request = requests.post(url, headers=headers, json=data)
        try:
            return request.json()
        except Exception as e:
            print(f"Error getting Pinterest ad targeting metrics: {e}")
            traceback.print_exc()

    def getAd(self, ad_id : str):
        """Get Pinterest ad \n
        Args:
            ad_id (str) : Ad id
        Returns:
            dict : Pinterest ad
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/ads/{ad_id}"
        headers = {'Authorization': f'Bearer {self.ACCESS_TOKEN}',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        request = requests.get(url, headers=headers)
        try:
            return request.json()
        except Exception as e:
            print(f"Error getting Pinterest ad: {e}")
            traceback.print_exc()
        
    def getAdGroups(self, campaign_ids : list = None, ad_group_ids : list = None, entity_statuses : list = None,
                    page_size : int = 25, order : str = 'ASCENDING', bookmark : str = None, translate_interests_to_names : bool = False):
        """Get Pinterest ad groups \n
        Args:
            campaign_ids (list) : Campaign ids
            ad_group_ids (list) : Ad group ids
            entity_statuses (list) : ACTIVE PAUSE ARCHIVED DELETED_DRAFT. Default is None
            page_size (int) : Page size, default 25, max 250
            order (str) : ASCENDING or DESCENDING. Default is 'ASCENDING'
            bookmark (str) : Bookmark used to get the next page. Default is None
            translate_interests_to_names (bool) : Translate interests to names. Default is False
        Returns:
            dict : Pinterest ad groups
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/ad_groups"
        headers = {'Authorization': f'Bearer { self.ACCESS_TOKEN}',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        params = {'campaign_ids': campaign_ids,
                    'ad_group_ids': ad_group_ids,
                    'entity_statuses': entity_statuses,
                    'page_size': page_size,
                    'order': order,
                    'bookmark': bookmark,
                    'translate_interests_to_names': translate_interests_to_names}
        request = requests.get(url, headers=headers, params=params)
        response = request.json()
        try:
            data = response
            while 'bookmark' in response and response['bookmark'] != None and response['bookmark'] != bookmark:
                bookmark = response['bookmark']
                params['bookmark'] = bookmark
                request = requests.get(url, headers=headers, params=params)
                response = request.json()
                data += response
            return data
        except Exception as e:
            print(f"Error getting Pinterest ad groups: {e}")
            traceback.print_exc()

    def getAdGroupAnalytics(self, start_date : str, end_date : str, ad_group_ids : list, columns : list,
                            granularity : str = "DAY", click_window_days : int = 30, engagement_window_days : int = 30,
                            view_window_days : int = 1, conversion_report_time : str = "TIME_OF_AD_ACTION"):
        """Get Pinterest ad group metrics  \n
        Args:
            start_date (str) : Start date in format 'YYYY-MM-DD'
            end_date (str) : End date in format 'YYYY-MM-DD'
            ad_group_ids (list) : Ad group ids
            columns (list) : Columns to get
            granularity (str) : DAY HOUR WEEK MONTH. Default is 'DAY'
            click_window_days (int) : Click window days 1 7 14 30 60. Default is 30
            engagement_window_days (int) : Engagement window days 1 7 14 30 60. Default is 30
            view_window_days (int) : View window days 1 7 14 30 60. Default is 1
            conversion_report_time (str) : TIME_OF_AD_ACTION TIME_OF_CONVERSION. Default is 'TIME_OF_AD_ACTION'
        Returns:
            dict : Pinterest ad group metrics
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/ad_groups/analytics"
        headers = {'Authorization': f'Bearer {self.ACCESS_TOKEN}',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        data = {'start_date': start_date,
                'end_date': end_date,
                'ad_group_ids': ad_group_ids,
                'columns': columns,
                'granularity': granularity,
                'click_window_days': click_window_days,
                'engagement_window_days': engagement_window_days,
                'view_window_days': view_window_days,
                'conversion_report_time': conversion_report_time}
        request = requests.post(url, headers=headers, json=data)
        try:
            return request.json()
        except Exception as e:
            print(f"Error getting Pinterest ad group metrics: {e}")
            traceback.print_exc()

    def getAdGroupTargetingAnalytics(self, ad_group_ids : list, start_date : str, end_date : str, targeting_types : list,
                                     columns : list, granularity : str = "DAY", click_window_days : int = 30,
                                    engagement_window_days : int = 30, view_window_days : int = 1, conversion_report_time : str = "TIME_OF_AD_ACTION",
                                    attribution_types : list = None):
        """Get Pinterest ad group targeting metrics \n
        Args:
            ad_group_ids (list) : Ad group ids
            start_date (str) : Start date in format 'YYYY-MM-DD'
            end_date (str) : End date in format 'YYYY-MM-DD'
            targeting_types (list) : KEYWORD APPTYPE GEO LOCATION PLACEMENT COUNTRY TARGETED_INTEREST PINNER_INTEREST AUDIENCE_INCLUDE AGE_BUCKET REGION CREATIVE_TYPE AGE
            columns (list) : Columns to get
            granularity (str) : DAY HOUR WEEK MONTH. Default is 'DAY'
            click_window_days (int) : Click window days 1 7 14 30 60. Default is 30
            engagement_window_days (int) : Engagement window days 1 7 14 30 60. Default is 30
            view_window_days (int) : View window days 1 7 14 30 60. Default is 1
            conversion_report_time (str) : TIME_OF_AD_ACTION TIME_OF_CONVERSION. Default is 'TIME_OF_AD_ACTION'
            attribution_types (list) : INDIVIDUAL HOUSE. Default is None
        Returns:
            dict : Pinterest ad group targeting metrics
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/ad_groups/targeting_metrics"
        headers = {'Authorization': f'Bearer {self.ACCESS_TOKEN}',
                     'Content-Type': 'application/json',
                     'Accept': 'application/json'}  
        data = {'ad_group_ids': ad_group_ids,
                'start_date': start_date,
                'end_date': end_date,
                'targeting_types': targeting_types,
                'columns': columns,
                'granularity': granularity,
                'click_window_days': click_window_days,
                'engagement_window_days': engagement_window_days,
                'view_window_days': view_window_days,
                'conversion_report_time': conversion_report_time,
                'attribution_types': attribution_types}
        request = requests.post(url, headers=headers, json=data)
        try:
            return request.json()
        except Exception as e:
            print(f"Error getting Pinterest ad group targeting metrics: {e}")
            traceback.print_exc()

    def getAdGroup(self, ad_group_id : str):
        """Get Pinterest ad group \n
        Args:
            ad_group_id (str) : Ad group id
        Returns:
            dict : Pinterest ad group
        """
        url = self.API_URL + f"ad_accounts/{self.ADVERTISER_ID}/ad_groups/{ad_group_id}"
        headers = {'Authorization': f'Bearer {self.ACCESS_TOKEN}',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        request = requests.get(url, headers=headers)
        try:
            return request.json()
        except Exception as e:
            print(f"Error getting Pinterest ad group: {e}")
            traceback.print_exc()

    