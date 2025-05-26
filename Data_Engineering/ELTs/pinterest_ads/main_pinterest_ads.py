import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
import json 
from etl_pinterest_ads import PinterestAdsETL
import traceback

def main(a=None,b=None):
    # project_id = '{{project_id}}'
    project_id = 'bonjout-beauty'
    storageServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/storage_service_account.json'))
    pinterestAdsETL = PinterestAdsETL(storageServiceAccountCredential,debug=True)

    # Get all campaigns
    try:
        print('Getting all campaigns')
        campaigns = pinterestAdsETL.getCampaigns()
        print(f'Found {len(campaigns)} campaigns')
        print('Loading campaigns to BigQuery')
        pinterestAdsETL.loadDataToBigQuery(campaigns,"P_TD_PinterestAds_Campaigns")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

if __name__ == '__main__':
    main()