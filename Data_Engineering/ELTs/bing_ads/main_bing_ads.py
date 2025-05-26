import json 
import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from etl_bing_ads import ETLBingAds


def main(a='a',b='b'):
    project_id = 'bonjout-shopify'
    # project_id = '{{project_id}}'
    adminServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/admin_service_account.json'))
    ETL = ETLBingAds(adminServiceAccountCredential ,debug=False)

    try:
        print("Getting Bing Ads Campaigns") 
        campaignsInsights = ETL.getCampaignsInsights()
        print(f"Got Bing Ads Campaigns: {len(campaignsInsights)} rows fetched") 
        try:
            print("Uploading Bing Ads Campaigns to GCS")
            ETL.loadDataToBigQuery(data=campaignsInsights,BQtable='raw_BingAds_Campaigns_Insights',base_table="P_TD_BingAds_Campaigns_Insights")
            print("Bing Ads Campaigns uploaded to GCS")
        except Exception as e:
            print("Failed to upload Bing Ads Campaigns to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Bing Ads Campaigns")
        print(e)

main()
