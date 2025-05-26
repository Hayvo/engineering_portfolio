'''
ETL Pipeline script to extract data from Meta for {{project_id}} project
Last edit : 2024-07-05 by DataGem Consulting
Contact Pierre Denig at pierre@datagem-consulting.com
'''

import sys, os
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from etl_meta_ads import ETLMetaMarketingAPI
import yaml
import warnings 

warnings.simplefilter('ignore')


###################################
####### Loading Credentials #######
###################################

# project_id = '{{project_id}}'
project_id = 'bonjout-shopify'
with open(f'./src/var/login_credentials/{project_id}/admin_service_account.json') as f:
    adminServiceAccount = yaml.load(f,Loader=yaml.CLoader)

###################################
####### Queries Parameters ########
###################################

ads_query = {"entity":"ads",
             "info_fields" :"adset_id,campaign_id,configured_status,effective_status,conversion_domain,created_time,name,id",
             "insights_fields":"ad_id,ad_name,ad_click_actions,actions,action_values,ad_impression_actions,adset_id,adset_name,campaign_id,campaign_name,canvas_avg_view_percent,canvas_avg_view_time,clicks,unique_clicks,conversions,created_time,cpp,cpc,cpm,ctr,date_start,date_stop,frequency,full_view_impressions,impressions,inline_post_engagement,reach,spend",
             "date_preset":"last_30d",
             "force_schema":False,
             "BQdataset":"marketplace_Facebook_Ads",
             "BQtable_info":"P_TD_Ads_Info_Temp",
             "BQtable_info_base":"P_TD_Ad_Info",
             "BQtable_insights":"P_TD_Ad_Metric_Temp",
             "BQtable_insights_base":"P_TD_Ad_Metric"}

campaigns_query = {"entity":"campaigns",
                   "info_fields":"name,status,boosted_object_id,budget_remaining,created_time,daily_budget,effective_status,objective,start_time,stop_time",
                   "insights_fields":"campaign_id,campaign_name,actions,action_values,canvas_avg_view_percent,canvas_avg_view_time,clicks,unique_clicks,conversions,created_time,cpp,cpc,cpm,ctr,date_start,date_stop,frequency,full_view_impressions,impressions,inline_post_engagement,reach,spend",
                   "date_preset":"last_7d",
                   "force_schema":False,
                   "BQdataset":"marketplace_Facebook_Ads",
                   "BQtable_info":"P_TD_Campaign_Info_Temp",
                   "BQtable_info_base":"P_TD_Campaigns_Info",
                   "BQtable_insights":"P_TD_Campaign_Metric_Temp",
                   "BQtable_insights_base":"P_TD_Campaign_Metric"}

queries = [campaigns_query, ads_query]

###################################
###### Actual fetch function ######
###################################

metaAdsCredentials = yaml.load(open(f'./src/var/login_credentials/{project_id}/meta_ads_login.yml'),Loader=yaml.CLoader)

def main(a='a',b='b'):
    etl = ETLMetaMarketingAPI(adminServiceAccount=adminServiceAccount,debug=False)
    for query in queries:
        etl.runQuery(query)

if __name__ == '__main__':
    main()