import os 
import json
import traceback

queryId = input("Enter the query ID: ")
queryId = int(queryId)
with open("temp/logs.json", "r") as f:
    logs = json.load(f)
    for log in logs:
        if log["queryId"] == queryId:
            print(f"Error for query {queryId}: {log['error']}")
            print(f"SQL: {log['sql']}")
            break
    else:
        print(f"No error found for query {queryId}.")