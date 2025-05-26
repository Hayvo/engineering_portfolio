import sys,os
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from etl_sps_commerce import SPScommerceETL
import json

if __name__ == "__main__":
    debug = False
    # project_id = '{{project_id}}'
    project_id = 'bravo-sierra'
    storageServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/storage_service_account.json'))

    spsETL = SPScommerceETL(storageServiceAccountCredential,debug=debug,areCredentialsLocal=False)
    
    history = spsETL.getTransactionsPaths(in_out="/in")
    print(history)
    # print(f"History: {len(history)}")

    # forms = spsETL.getTransactionsHisotry()
    # print(forms)

    # print("Getting transactions paths")
    # transactionsPaths = spsETL.getTransactionsPaths()
    # print(f"Transactions paths: {transactionsPaths} retrieved")

    # print("Getting transactions files")
    # files = spsETL.getTransactions(transactionsPaths)
    
    # print("Loading transactions to BigQuery")
    # spsETL.loadDataToBigQuery(files,'raw_SPScommerce_transactions_temp',force_schema=False)
    # print("Transactions loaded to BigQuery")