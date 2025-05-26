from Utils.GoogleCloudHandlers.bigquery_handler import BigQueryHandler
from utils.fileHandler import FileHandler
from utils.queryFormatter import QueryFormatter
from utils.reDashHandler import ReDashHandler
from utils.errorAnalyzer import ErrorAnalyzer
import json

fileHandler = FileHandler()
bigQueryHandler = BigQueryHandler()
redashHandler = ReDashHandler()
errorAnalyzer = ErrorAnalyzer()

nbReport = redashHandler.getMaxQueryId()
logs = []
errorAnalysis = {"TableNotFoundError": {"count": 0, "errorMessage": []}}

tableFunctions = bigQueryHandler.retrieveDatasetTableFunctions("DL_Redash")


for i in range(1, nbReport+1):
    try:
        if not(bigQueryHandler.checkIfTableFunctionExists(tableFunctions, i)):
            print(f"Processing report {i}/{nbReport}")
            redashJson = redashHandler.openRedashQueryJson(i)
            reportSQL,metadata = fileHandler.redashJsonToBigueryFunction(redashJson, bigQueryHandler.client.project, "Marketplace_Dailylook")
            try:
                queryDDL = bigQueryHandler.createTableFunctionFromSQL(datasetId="DL_Redash", metadata= metadata, sql= reportSQL)
                bigQueryHandler.runQuery(queryDDL)
                pass
            except Exception as e:
                errorType, errorMessage = errorAnalyzer.analyze_error(str(e))
                try:
                    log = {
                        "queryId" : i,
                        "error" : str(e),
                        "sql" : queryDDL,
                        "errorType" : errorType,
                        "errorMessage" : errorMessage,
                    }
                    if errorType == "TableNotFoundError":
                        errorAnalysis[errorType]["count"] += 1
                        errorAnalysis[errorType]["errorMessage"].append(errorMessage)
                    else:
                        errorAnalysis[errorType] = errorAnalysis.get(errorType, 0) + 1
                    logs.append(log)
                except Exception as e:
                    print(f"Error while logging error: {e}")
                    continue
        else:
            print(f"Table function for query {i} already exists.")
    except Exception as e:
        print(f"Error while processing report {i}: {e}")
        continue
errorAnalysis["TableNotFoundError"]["errorMessage"] = list(set(errorAnalysis["TableNotFoundError"]["errorMessage"]))
print("Finished processing reports.")
print(f"Total errors logged: {len(logs)}")
with open("temp/logs.json", "w") as f:
    json.dump(logs, f, indent=4)
print("Logs saved to temp/logs.json")
print("Error analysis:")
print(json.dumps(errorAnalysis, indent=4))
with open("temp/errorAnalysis.json", "w") as f:
    json.dump(errorAnalysis, f, indent=4)