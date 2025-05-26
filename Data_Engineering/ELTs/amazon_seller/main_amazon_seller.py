import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from datetime import date, timedelta
from etl_amazon_seller import AmazonSellerETL
import json,yaml

if __name__ == "__main__":
    # project_id = '{{project_id}}'
    project_id = 'bravo-sierra'
    storageServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/storage_service_account.json'))
    amazonSellerCredentials = yaml.load(open(f'./src/var/login_credentials/{project_id}/amazon_seller_login.yml'),Loader=yaml.CLoader)
   
    amazonSellerETL = AmazonSellerETL(storageServiceAccountCredential,debug=True)
    
    startTime = (date.today()-timedelta(15)).strftime('%Y-%m-%dT00:00:00.000') + 'Z'
    endTime = (date.today()).strftime('%Y-%m-%dT00:00:00.000') + 'Z'
    amazonSellerETL.addReport('GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL',startTime,endTime,'marketplace_Amazon_Seller','raw_AmazonSellerPartner_orders_temp')
    amazonSellerETL.addReport('GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE',startTime,endTime,'marketplace_Amazon_Seller','raw_AmazonSellerPartner_returns_temp')
    amazonSellerETL.addReport('GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL', startTime,endTime,'marketplace_Amazon_Seller','raw_AmazonSellerPartner_fulfillments_temp')
    amazonSellerETL.run()