from Utils.GoogleCloudHandlers.bigquery_handler import BigQueryHandler
from RedashToBigQuery.utils.fileHandler import FileHandler
from RedashToBigQuery.utils.queryFormatter import QueryFormatter
from RedashToBigQuery.utils.reDashHandler import ReDashHandler
from RedashToBigQuery.utils.errorAnalyzer import ErrorAnalyzer
import json

fileHandler = FileHandler()
bigQueryHandler = BigQueryHandler()
redashHandler = ReDashHandler()
errorAnalyzer = ErrorAnalyzer()
queryFormatter = QueryFormatter()

i = int(input("Enter the query ID: "))

tableFunctions = bigQueryHandler.retrieveDatasetTableFunctions("DL_Redash")

try:
    if not(bigQueryHandler.checkIfTableFunctionExists(tableFunctions, i)):
        redashJson = redashHandler.openRedashQueryJson(i)
        reportSQL,metadata = fileHandler.redashJsonToBigueryFunction(redashJson, bigQueryHandler.client.project, "Marketplace_Dailylook")
        try:
            queryDDL = bigQueryHandler.createTableFunctionFromSQL(datasetId="DL_Redash", metadata= metadata, sql= reportSQL)
            bigQueryHandler.runQuery(queryDDL)
            pass
        except Exception as e:
            print(f"Error while processing report : {e}")
            print(f"SQL: \n{queryDDL}")
            errorType, errorMessage = errorAnalyzer.analyze_error(str(e))
            translatedSQL = queryFormatter.translateSQL(redashJson["query"], datasetId="Marketplace_Dailylook", projectId=bigQueryHandler.client.project)
            log = {
                "queryId" : i,
                "error" : str(e),
                "sql" : queryDDL,
                "errorType" : errorType,
                "errorMessage" : errorMessage,
                "translatedSQL" : translatedSQL,
            }
            print(json.dumps(log, indent=4))
    else:
        print(f"Table function for query {i} already exists.")
except Exception as e:
    print(f"Error while processing report : {e}")