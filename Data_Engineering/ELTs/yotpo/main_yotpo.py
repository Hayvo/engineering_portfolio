import json
import sys,os
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from etl_yotpo import YotpoETL
from datetime import datetime,timedelta

if __name__ == "__main__":
    project_id = '{{project_id}}'
    # print(os.path.dirname(sys.argv[0]))
    storageServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/storage_service_account.json'))
    debug = False
    yotpoETL = YotpoETL(storageServiceAccountCredential,debug=debug)

    # mes = yotpoETL.getAggregationMeasures()
    # print(*[m['name'] for m in mes],sep='\n')

    # dim = yotpoETL.getAggregationDimensions()
    # print(*[d['name'] for d in dim],sep='\n')	

    print("Fetching data from Yotpo API")
    last_90_days = [(datetime.now() - timedelta(days=1*i)).strftime('%Y/%m/%d') for i in range(254)]
    filters = [[f"aggregation_date={d}"] for d in last_90_days]
    data = yotpoETL.getAggregation(dimensions=['channel_type','source_type',"source_name",'source_id','aggregation_date'],
                                   measures=['all_sent','all_delivered','all_open','all_unique_open','all_clicks','all_unique_clicks','all_orders','all_revenue','all_spent','all_roi','all_x_roi','all_unique_orders','all_unsubscribed'],
                                   filters=filters)
    # print(data)
    yotpoETL.loadDataToBigQuery(data,'raw_Yotpo_Aggregated_Metrics_temp',force_schema=True,WRITE_DISPOSITION='WRITE_TRUNCATE')