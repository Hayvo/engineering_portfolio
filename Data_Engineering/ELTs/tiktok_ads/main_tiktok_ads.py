import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from etl_tiktok_ads import TikTokAdsETL
import json,yaml
import traceback
import time
from datetime import datetime,timedelta

def main(a=None,b=None):
    # project_id = 'bonjout-shopify'
    project_id = '{{project_id}}'

    if project_id == '{{project_id}}':
        project_id = input("Enter project_id: ")

    adminServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/admin_service_account.json'))
    tiktokAdsCredentials = yaml.load(open(f'./src/var/login_credentials/{project_id}/tiktok_ads_login.yml'),Loader=yaml.CLoader)
    
    ETL = TikTokAdsETL(adminServiceAccountCredential ,tikTokAdsCredentials= tiktokAdsCredentials,debug=False)
    
    force_schema = False

    try:
        print("Getting Tiktok Ads Campaigns") 
        fields = ["advertiser_id","campaign_id","create_time","objective_type","is_search_campaign","is_smart_performance_campaign","campaign_type","app_id","is_advanced_dedicated_campaign","campaign_app_profile_page_state","rf_campaign_type","campaign_product_source","campaign_name","bid_type","budget_mode","budget","budget_mode","operation_status"]
        campaigns = ETL.getCampaigns(fields=fields)
        print(f"Got Tiktok Ads Campaigns: {len(campaigns)} rows fetched") 
        try:
            print("Uploading Tiktok Ads Campaigns to GCS")
            ETL.loadDataToBigQuery(data=campaigns,BQtable='raw_Tiktok_Ads_Campaigns_temp',base_table="P_TD_Tiktok_Ads_Campaigns",force_schema=force_schema)
        except Exception as e:
            print("Failed to upload Tiktok Ads Campaigns to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Tiktok Ads Campaigns")
        print(e)

    try:
        print("Getting Tiktok Ads Campaigns Insights")
        campaignsInsights = ETL.getCampaignInsights()
        print(f"Got Tiktok Ads Campaigns Insights: {len(campaignsInsights)} rows fetched")
        try:
            print("Uploading Tiktok Ads Campaigns Insights to GCS")
            ETL.loadDataToBigQuery(data=campaignsInsights,BQtable='raw_Tiktok_Ads_Campaigns_Insights_temp',base_table="P_TD_Tiktok_Ads_Campaigns_Insights",force_schema=force_schema)
        except Exception as e:
            print("Failed to upload Tiktok Ads Campaigns Insights to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Tiktok Ads Campaigns Insights")
        print(e)

    try:
        print("Getting Tiktok Ads Ads") 
        fields = ['adgroup_id', 'optimization_event', 'tracking_app_id', 'flight_ids', 'destination_ids', 'ad_text', 'dynamic_destination', 'image_ids', 'creative_type', 'app_name', 'card_id', 'carousel_image_labels', 'shopping_ads_deeplink_type', 'ad_name', 'product_specific_type', 'tiktok_item_id', 'call_to_action_id', 'aigc_disclosure_type', 'is_new_structure', 'music_id', 'phone_region_calling_code', 'hotel_ids', 'catalog_id', 'brand_safety_postbid_partner', 'ad_texts', 'phone_number', 'disclaimer_type', 'identity_id', 'showcase_products', 'landing_page_url', 'deeplink_type', 'ad_id', 'is_aco', 'playable_url', 'ad_format', 'image_mode', 'tracking_offline_event_set_ids', 'sku_ids', 'video_id', 'dynamic_format', 'tracking_pixel_id', 'campaign_id', 'utm_params', 'adgroup_name', 'ad_ref_pixel_id', 'item_group_ids', 'branded_content_disabled', 'click_tracking_url', 'tracking_message_event_set_id', 'vehicle_ids', 'display_name', 'operation_status', 'create_time', 'avatar_icon_web_uri', 'media_title_ids', 'viewability_vast_url', 'shopping_ads_video_package_id', 'secondary_status', 'deeplink', 'tiktok_page_category', 'landing_page_urls', 'call_to_action', 'identity_type', 'page_id', 'shopping_ads_fallback_type', 'creative_authorized', 'phone_region_code', 'advertiser_id', 'vast_moat_enabled', 'domain', 'deeplink_format_type', 'profile_image_url', 'deeplink_utm_params', 'carousel_image_index', 'product_set_id', 'modify_time', 'vertical_video_strategy', 'disclaimer_clickable_texts', 'impression_tracking_url', 'cpp_url', 'video_view_tracking_url', 'promotional_music_disabled', 'identity_authorized_bc_id', 'fallback_type', 'campaign_name', 'interactive_motion_id', 'item_stitch_status', 'dark_post_status', 'disclaimer_text', 'brand_safety_vast_url', 'item_duet_status', 'viewability_postbid_partner']
        ads = ETL.getAds(fields=fields)
        print(f"Got Tiktok Ads Ads: {len(ads)} rows fetched") 
        try:
            print("Uploading Tiktok Ads Ads to GCS")
            ETL.loadDataToBigQuery(data=ads,BQtable='raw_Tiktok_Ads_Ads_temp',base_table="P_TD_Tiktok_Ads_Ads",force_schema=force_schema)
        except Exception as e:
            print("Failed to upload Tiktok Ads Ads to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Tiktok Ads Ads")
        print(e)

    try:
        print("Getting Tiktok Ads Ad Insights")
        adsInsights = ETL.getAdsInsights()
        print(f"Got Tiktok Ads Ads Insights: {len(adsInsights)} rows fetched")
        try:
            print("Uploading Tiktok Ads Ads Insights to GCS")
            ETL.loadDataToBigQuery(data=adsInsights,BQtable='raw_Tiktok_Ads_Ads_Insights_temp',base_table="P_TD_Tiktok_Ads_Ads_Insights",force_schema=force_schema)
        except Exception as e:
            print("Failed to upload Tiktok Ads Ads Insights to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Tiktok Ads Ads Insights")
        print(e)

if __name__ == "__main__":
    main()