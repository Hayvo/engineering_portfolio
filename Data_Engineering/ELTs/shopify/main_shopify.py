import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from etl_shopify import ShopifyETL,ShopifyETLv2
import json,yaml
from datetime import datetime,timedelta

def main(a=None,b=None):
    project_id = 'bonjout-shopify'
    # project_id = '{{project_id}}'
    adminServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/admin_service_account.json'))
    shopifyCredentials = yaml.load(open(f'./src/var/login_credentials/{project_id}/shopify_login.yml'),Loader=yaml.CLoader)
    
    ETL = ShopifyETL(adminServiceAccountCredential ,shopifyCredentials= shopifyCredentials,debug=True, defaultStartTime=datetime.now()-timedelta(days=10),defaultEndTime=datetime.now())
    
    try:
        print("Getting Shopify Orders") 
        orders = ETL.getOrders()
        print(f"Got Shopify Orders: {len(orders)} rows fetched") 
        try:
            print("Uploading Shopify Orders to GCS")
            ETL.loadDataToBigQuery(data=orders,BQtable='raw_Shopify_Orders_temp',base_table="raw_Shopify_Orders",force_schema=True)
        except Exception as e:
            print("Failed to upload Shopify Orders to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Shopify Orders")
        print(e)

    try:
        print("Getting Shopify Customers")
        customers = ETL.getCustomers()
        print(f"Got Shopify Customers: {len(customers)} rows fetched")
        try:
            print("Uploading Shopify Customers to GCS")
            ETL.loadDataToBigQuery(data=customers,BQtable='raw_Shopify_Customers_temp',base_table="raw_Shopify_Customers")
            print("Shopify Customers uploaded to GCS")
        except Exception as e:
            print("Failed to upload Shopify Customers to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Shopify Customers")
        print(e)
    
    try:
        print("Getting Shopify Products")
        products = ETL.getProducts()
        print(f"Got Shopify Products: {len(products)} rows fetched")
        try:
            print("Uploading Shopify Products to GCS")
            ETL.loadDataToBigQuery(data=products,BQtable='raw_Shopify_Products')
            print("Shopify Products uploaded to GCS")
        except Exception as e:
            print("Failed to upload Shopify Products to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Shopify Products")
        print(e)

    try:
        print("Getting Shopify inventory")
        inventory = ETL.getInventoryItems()
        print(f"Got Shopify inventory: {len(inventory)} rows fetched")
        try:
            print("Uploading Shopify inventory to GCS")
            ETL.loadDataToBigQuery(data=inventory,BQtable='raw_Shopify_Inventory')
            print("Shopify inventory uploaded to GCS")
        except Exception as e:
            print("Failed to upload Shopify inventory to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Shopify inventory")
        print(e)

    try:
        print("Getting Shopify Locations")
        locations = ETL.getLocations()
        print(f"Got Shopify Locations: {len(locations)} rows fetched")
        try:
            print("Uploading Shopify Locations to GCS")
            ETL.loadDataToBigQuery(data=locations,BQtable='raw_Shopify_Inventory_Locations')
            print("Shopify Locations uploaded to GCS")
        except Exception as e:
            print("Failed to upload Shopify Locations to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Shopify Locations")
        print(e)

    try:
        print("Getting Shopify Inventory Levels")
        inventory_levels = ETL.getInventoryLevels()
        print(f"Got Shopify Inventory Levels: {len(inventory_levels)} rows fetched")
        try:
            print("Uploading Shopify Inventory Levels to GCS")
            ETL.loadDataToBigQuery(data=inventory_levels,BQtable='raw_Shopify_Inventory_Levels')
            print("Shopify Inventory Levels uploaded to GCS")
        except Exception as e:
            print("Failed to upload Shopify Inventory Levels to GCS")
            print(e)
    except Exception as e:
        print("Failed to get Shopify Inventory Levels")
        print(e)


def mainV2(a='a',b='b'):
    # project_id = '{{project_id}}'
    project_id = 'bonjout-shopify'
    adminServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/admin_service_account.json'))
    shopifyCredentials = yaml.load(open(f'./src/var/login_credentials/{project_id}/shopify_login.yml'),Loader=yaml.CLoader)
    
    ETL = ShopifyETLv2(adminServiceAccountCredential ,shopifyCredentials= shopifyCredentials,debug=False, defaultStartTime=datetime.now()-timedelta(days=10),defaultEndTime=datetime.now())

    orders = ETL.getOrders()
    print(orders[1])

if __name__ == '__main__':
    main()