import json
import traceback
import json
from google.cloud import bigquery
import threading
from google.auth import compute_engine
import pandas as pd
from datetime import datetime
from bigquery_loader import BigQueryLoader
from pandas_gbq import to_gbq

class BigQueryHandler:

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:  # Prevent race conditions
                if cls._instance is None:
                    cls._instance = super(BigQueryHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)  
        return cls._instance

    def _initialize(self, serviceAccountJson : dict = None, client: bigquery.Client = None):
        """Initialize only once in a thread-safe way."""
        if hasattr(self, "client"):  # Prevent multiple initializations
            return

        try:
            if client:
                print("Using provided client")
                self.client = client
            elif serviceAccountJson:
                print("Using provided service account JSON")
                self.client = bigquery.Client.from_service_account_info(serviceAccountJson)
                self.Loader = BigQueryLoader(client=self.client)
            else:
                traceback.print_exc()
                try:
                    with open('var/bigquery_service_account.json', 'r') as file:
                        print("Using service account JSON from file")
                        self.client = bigquery.Client.from_service_account_info(json.load(file))
                        self.Loader = BigQueryLoader(serviceAccountJson=json.load(file))
                except Exception:
                    print("Using default credentials")
                    traceback.print_exc()
                    credentials = compute_engine.Credentials()
                    self.client = bigquery.Client(credentials=credentials)
                    self.Loader = BigQueryLoader(client=self.client)
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error initializing GCP credentials: {e}")
        
    def formatParams(self, params : list[dict]) -> list[dict]:
        """Format parameters for query execution.
        Args:
            params (list[dict]): The parameters to format.
        Returns:
            list[dict]: The formatted parameters."""
        dateFormatList = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f","%Y/%m/%d %H:%M:%S","%Y/%m/%d %H:%M:%S.%f","%m/%d/%Y"]
        try: 
            for param in params:
                if param["Type"] == "DATE":
                    for format in dateFormatList:
                        try:
                            param["Value"] = datetime.strptime(param["Value"], format).strftime("%Y-%m-%d")
                            break
                        except:
                            continue
                elif param["Type"] == "TEXT":
                    param["Type"] = "STRING"
                else:
                    param["Value"] = param["Value"]
            return params
        except Exception as e:
            print(traceback.format_exc())

    def runQuery(self, query : str) -> tuple[pd.DataFrame,str]:
        """Run a query and return the results and job ID.
        Args:
            query (str): The query to run.
        Returns:
            results,jobId (tuple[pd.DataFrame,str]): The query results and job ID."""
        try:
            queryJob = self.client.query(query)
            jobId = queryJob.job_id

            results = queryJob.result().to_dataframe()  # Convert to DataFrame
            print(f"Query job ID: {jobId}")
            print(f"Retrieved {len(results)} rows")
            return results, jobId
        except Exception as e:
            # traceback.print_exc()
            raise Exception(f"Error running query: {e}")
        
    def retrieveJobDetails(self, bigQueryJobs: dict) -> list:
        """Retrieve details for a list of BigQuery jobs.
        Args:
            bigQueryJobs (dict): A dictionary of BigQuery jobs.
        Returns:
            list: The job details."""
        try:
            jobIds = list(bigQueryJobs.keys())  # Extract job IDs
            if not jobIds:  # Handle case where jobIds is empty
                return "No job IDs provided."

            jobIdString = ",".join(f'"{id}"' for id in jobIds)  # Ensure job IDs are properly formatted

            query = f"""
            SELECT 
                creation_time, project_id, project_number, job_id, job_type, 
                statement_type, priority, start_time, end_time, query, state 
            FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT 
            WHERE job_id IN ({jobIdString})
            """
            job = self.runQuery(query)
            jobDetails = [dict(row) for row in job]  # Convert to list of dictionaries
            return jobDetails
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error retrieving job details: {e}")
        
    def retrieveDatasetTableFunctions(self, datasetId : str) -> list:
        """Retrieve table functions for a dataset.
        Args:
            datasetId (str): The dataset ID.
        Returns:
            list: The table functions
        """
        try:
            query = f"""
                    SELECT *
                    FROM (
                        SELECT 
                            specific_name,
                            REGEXP_EXTRACT(ddl,'.*description="(.*)".*') AS Name 
                        FROM `{self.client.project}.{datasetId}.INFORMATION_SCHEMA.ROUTINES`)
                    LEFT JOIN `{self.client.project}.{datasetId}.INFORMATION_SCHEMA.PARAMETERS` USING(specific_name)
                    """
            results, _ = self.runQuery(query)
            tableFunctions = {}
            for row in results:
                functionName = row['specific_name']
                reportName = row['Name']
                paramPosition = row['ordinal_position']
                paramName = row['parameter_name']
                paramType = row['data_type']

                tableFunctions.setdefault(functionName, {}).update({
                    "FunctionName": functionName,
                    "ReportName": reportName,
                    "ReportId": functionName.split("_")[-1],
                })

                if paramName:
                    tableFunctions[functionName].setdefault("Params", []).append({
                        "Position": paramPosition,
                        "Name": paramName,
                        "Type": paramType
                    })
                else:
                    tableFunctions[functionName].setdefault("Params", [])
            functionList = [value for key, value in tableFunctions.items()]
            return functionList
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error retrieving dataset functions: {e}")
    
    def runTableFunction(self,datasetId : str, selectedReport : dict, params : dict) -> list:
        """Run a table function and return the results.
        Args:
            datasetId (str): The dataset ID.
            selectedReport (dict): The selected report. The structure should be:
                {
                    "FunctionName": "function_name",
                    "ReportName": "report_name",
                    "ReportId": "report_id",
                }
            params (dict): The parameters for the report. The structure should be:
                {
                    "param_name": {
                        "Position": 1,
                        "Value": "param_value"
                    },
                    "param_name": {
                        "Position": 2,
                        "Value": "param_value"
                    }
                }
        Returns:
            list: The query results."""
        try:
            tableFunctionName = selectedReport['FunctionName']
            paramList = [value for _, value in params.items()]
            paramList.sort(key=lambda x: x['Position'])
            paramList = self.formatParams(paramList)
            query = f"""
            SELECT * FROM `{self.client.project}.{datasetId}.{tableFunctionName}`(""" + ",".join(f"'{param['Value']}'" for param in paramList) + ")"
            results, jobId = self.runQuery(query)
            return results, jobId
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error running table function: {e}")

    def createTableFunctionFromSQL(self, datasetId : str, metadata : dict, sql : str) -> None:
        """Create a table function from SQL.
        Args:
            datasetId (str): The dataset ID.
            metadata (dict): The metadata for the table function. The structure should be:
                {
                    "ReportId": "report_id",
                    "Name": "report_name",
                    "Params": [
                        {"Position": 1, "Name": "param_name", "Type": "STRING"},
                        {"Position": 2, "Name": "param_name", "Type": "INT"}
                    ]
                }
            sql (str): The SQL for the table function.
        """
        try:
            # Construct the function query
            query = f"""CREATE OR REPLACE TABLE FUNCTION `{self.client.project}.{datasetId}.DL_Report_{metadata['ReportId']}`({", ".join(f"{param['Name'].replace(' ', '_')} {param['Type']}" for param in metadata['Params'])}) \nOPTIONS(description = "{metadata['Name']}") \nAS (\n{sql}\n)"""

            return query

        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error creating table function: {e}")

    def checkIfTableFunctionExists(self, tableFunctions : list , reportId : int) -> bool:
        """Check if a table function exists.
        Args:
            tableFunctions (list): The list of table functions.
            reportId (str): The report ID.
        Returns:
            bool: Whether the table function exists."""
        try:
            results = [tableFunction for tableFunction in tableFunctions if tableFunction['ReportId'] == str(reportId)]
            return bool(results)
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error checking if table function exists: {e}")
        