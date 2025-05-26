import json
import yaml
import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from etl_klaviyo import KlaviyoETL
from datetime import datetime, timedelta


def main(a='a',b='b'):
    force_schema = False
    WRITE_DISPOSITION = 'WRITE_TRUNCATE'
    debug = False
    project_id = '{{project_id}}'

    if project_id == '{{project_id}}':
        project_id = input("Please enter the project_id: ")

    adminServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/admin_service_account.json'))
    klaviyoCredentials = yaml.load(open(f'./src/var/login_credentials/{project_id}/klaviyo_login.yml'),Loader=yaml.FullLoader)
    
    ten_days_ago =  datetime.now() - timedelta(days=20)
    startTime = ten_days_ago.strftime('%Y-%m-%d')
    klaviyoETL = KlaviyoETL(adminServiceAccountCredential,klaviyoCredentials,debug=debug,defaultStartTime=startTime)

    print("Fetching data from Klaviyo API")

    try:
        print("Fetching Metrics")
        klaviyoETL.loadDataToBigQuery(klaviyoETL.getMetrics(), 'raw_Klaviyo_Metrics', force_schema=force_schema, WRITE_DISPOSITION=WRITE_DISPOSITION)
    except Exception as e:
        print(f"Error fetching Metrics: {e}")

    try:
        print("Fetching Metric Data by campaign")
        klaviyoETL.loadDataToBigQuery(
            klaviyoETL.getMetricData(
                klaviyoETL.getMetricIds('raw_Klaviyo_Metrics'),
                groupby=['$message', '$attributed_message'],
                interval='day',
                timezone="America/New_York",
                startTime=startTime
            ),
            BQtable= 'raw_Klaviyo_Metrics_Aggregate_Data_by_campaign_temp',
            base_table = "raw_Klaviyo_Metrics_Aggregate_Data_by_campaign_temp",
            force_schema=force_schema,
            WRITE_DISPOSITION=WRITE_DISPOSITION
        )
    except Exception as e:
        print(f"Error fetching Metric Data by campaign: {e}")

    try:
        print("Fetching Metric Data by flow")
        klaviyoETL.loadDataToBigQuery(
            klaviyoETL.getMetricData(
                klaviyoETL.getMetricIds('raw_Klaviyo_Metrics'),
                groupby=['$attributed_flow'],
                interval='day',
                timezone="America/New_York",
                startTime=startTime
            ),
            'raw_Klaviyo_Metrics_Aggregate_Data_by_flow_temp',
            base_table='raw_Klaviyo_Metrics_Aggregate_Data_by_flow_temp',
            force_schema=force_schema,
            WRITE_DISPOSITION=WRITE_DISPOSITION
        )
    except Exception as e:
        print(f"Error fetching Metric Data by flow: {e}")

    try:
        print("Fetching Lists")
        klaviyoETL.loadDataToBigQuery(klaviyoETL.getLists(), 'raw_Klaviyo_Lists', force_schema=force_schema, WRITE_DISPOSITION=WRITE_DISPOSITION)
    except Exception as e:
        print(f"Error fetching Lists: {e}")

    try:
        print("Fetching Campaigns")
        klaviyoETL.loadDataToBigQuery(
            klaviyoETL.getEmailCampaigns() + klaviyoETL.getSmsCampaigns(),
            'raw_Klaviyo_Campaigns',
            base_table='P_TD_Klaviyo_Campaigns',
            force_schema=force_schema,
            WRITE_DISPOSITION=WRITE_DISPOSITION
        )
    except Exception as e:
        print(f"Error fetching Campaigns: {e}")

    try:
        print("Fetching Campaign Recipients")
        klaviyoETL.loadDataToBigQuery(
            klaviyoETL.getCampaignRecipient(klaviyoETL.getCampaignIds('raw_Klaviyo_Campaigns')),
            'raw_Klaviyo_Campaign_Recipients',
            force_schema=force_schema,
            WRITE_DISPOSITION=WRITE_DISPOSITION
        )
    except Exception as e:
        print(f"Error fetching Campaign Recipients: {e}")

    try:
        print("Fetching Flows")
        klaviyoETL.loadDataToBigQuery(klaviyoETL.getFlows(), 'raw_Klaviyo_Flows', force_schema=force_schema, WRITE_DISPOSITION=WRITE_DISPOSITION)
    except Exception as e:
        print(f"Error fetching Flows: {e}")

    try:
        print("Fetching Flow Actions")
        klaviyoETL.loadDataToBigQuery(
            klaviyoETL.getFlowActions(klaviyoETL.getFlowActionIds('raw_Klaviyo_Flows')),
            'raw_Klaviyo_Flow_Actions',
            force_schema=force_schema,
            WRITE_DISPOSITION=WRITE_DISPOSITION
        )
    except Exception as e:
        print(f"Error fetching Flow Actions: {e}")

    try:
        print("Fetching profiles")
        klaviyoETL.loadDataToBigQuery(
            klaviyoETL.getProfiles(),
            'raw_Klaviyo_Profiles',
            base_table='P_TD_Klaviyo_Profiles',
            force_schema=force_schema,
            WRITE_DISPOSITION=WRITE_DISPOSITION
        )
    except Exception as e:
        print(f"Error fetching profiles: {e}")

    print("Fetch done")

if __name__ == '__main__':
    main()