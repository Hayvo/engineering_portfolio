import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from etl_mason_hub import MasonHubETL
import json,yaml
import traceback
import time
from datetime import datetime,timedelta

def main(a=None,b=None):
    project_id = '{{project_id}}'
    masonHubCredential = yaml.load(open(f'./src/var/login_credentials/{project_id}/mason_hub_login.yml'),Loader=yaml.FullLoader)
    storageServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/storage_service_account.json'))
    masonHubETL = MasonHubETL(storageServiceAccountCredential,masonHubCredential=masonHubCredential,debug=False)


    """All date/time records should be submitted in RFC3339 format ("2018-10-21T14:00:21Z" or "2018-10-21T14:00:21-05:00")."""

    today = datetime.now()
    seven_days_ago = today - timedelta(days=7)
    seven_days_ago = seven_days_ago.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Get all skus
    try:
        print('Getting all skus')
        skus = masonHubETL.getAllSkus()
        skuList =  [sku['unique_sku_name'] for sku in skus]
        skuList = list(set(skuList))
        print(f'Found {len(skuList)} skus')
        print('Loading skus to BigQuery')
        masonHubETL.loadDataToBigQuery(skus,"P_TD_MasonHub_Skus")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

    # Get all inventories
    try:
        print('Getting all inventories')
        inventories = masonHubETL.getSkusInventorySnapshot(cid=skuList)
        print(f'Found {len(inventories)} inventories')
        print('Loading inventories to BigQuery')
        masonHubETL.loadDataToBigQuery(inventories,"P_TD_MasonHub_Inventories")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

    # Get all shipments 
    try:
        print('Getting all shipments')
        shipments = masonHubETL.getAllShipments(sdt=seven_days_ago)
        print(f'Found {len(shipments)} shipments')
        print('Loading shipments to BigQuery')
        masonHubETL.loadDataToBigQuery(shipments,"raw_MasonHub_Shipments",base_table="P_TD_MasonHub_Shipments")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

    # Get all orders
    try:
        print('Getting all orders')
        orders = masonHubETL.getAllOrders(sdt=seven_days_ago)
        print(f'Found {len(orders)} orders')
        print('Loading orders to BigQuery')
        masonHubETL.loadDataToBigQuery(orders,"raw_MasonHub_Orders",base_table="P_TD_MasonHub_Orders")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

    # Get all order updates
    try:
        print('Getting all order updates')
        orderUpdates = masonHubETL.getAllOrderUpdates()
        print(f'Found {len(orderUpdates)} order updates')
        print('Loading order updates to BigQuery')
        masonHubETL.loadDataToBigQuery(orderUpdates,"P_TD_MasonHub_OrderUpdates")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

    # Get all order cancels
    try:
        print('Getting all order cancels')
        orderCancels = masonHubETL.getAllOrderCancels()
        print(f'Found {len(orderCancels)} order cancels')
        print('Loading order cancels to BigQuery')
        masonHubETL.loadDataToBigQuery(orderCancels,"P_TD_MasonHub_OrderCancels")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

    # Get all order shipments
    try:
        print('Getting all order shipments')
        orderShipments = masonHubETL.getAllOrderShipments(sdt=seven_days_ago)
        print(f'Found {len(orderShipments)} order shipments')
        print('Loading order shipments to BigQuery')
        masonHubETL.loadDataToBigQuery(orderShipments,"raw_MasonHub_OrderShipments",base_table="P_TD_MasonHub_OrderShipments")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

    # Get all returns
    try:
        print('Getting all returns')
        returns = masonHubETL.getAllReturns(sdt=seven_days_ago)
        print(f'Found {len(returns)} returns')
        print('Loading returns to BigQuery')
        masonHubETL.loadDataToBigQuery(returns,"raw_MasonHub_Returns",base_table="P_TD_MasonHub_Returns")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

    # Get inventory locations
    try:
        print('Getting all inventory locations')
        locations = masonHubETL.getInventoryLocations()
        print(f'Found {len(locations)} inventory locations')
        print('Loading inventory locations to BigQuery')
        masonHubETL.loadDataToBigQuery(locations,"P_TD_MasonHub_InventoryLocations")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()
    
    # Get all inbound shipments ASN 
    try:
        print('Getting all inbound shipments ASN')
        inboundShipments = masonHubETL.getAllInboundShipmentASN(sdt=seven_days_ago)
        print(f'Found {len(inboundShipments)} inbound shipments ASN')
        print('Loading inbound shipments ASN to BigQuery')
        masonHubETL.loadDataToBigQuery(inboundShipments,"raw_MasonHub_InboundShipments",base_table="P_TD_MasonHub_InboundShipments")
        print('Done')
    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()

if __name__ == "__main__":
    main()