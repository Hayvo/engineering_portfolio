from datetime import datetime
import json
import traceback
import time
import yaml
import json
from google.cloud import bigquery


class GCPHandler:
    def __init__(self,serviceAccountJson = None, client : bigquery.Client = None ):
        if client:
            self.client = client
        elif serviceAccountJson:
            self.client = bigquery.Client.from_service_account_info(serviceAccountJson)
        else:
            self.client = bigquery.Client()

    def retrieveJobDetails(self, bigQueryJobs: dict):
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

            queryJob = self.client.query(query)
            job = queryJob.result()
            jobDetails = [dict(row) for row in job]  # Convert to list of dictionaries
            return jobDetails
        except Exception as e:
            print(traceback.format_exc())
            raise Exception(f"Error retrieving job details: {e}")

    def runQuery(self, query):
        try:
            queryJob = self.client.query(query)
            jobId = queryJob.job_id
            results = [dict(row) for row in queryJob.result()]  # Convert to list of dictionaries
            return results, jobId
        except Exception as e:
            print(traceback.format_exc())
            raise Exception(f"Error running query: {e}")
    
    def retrieveTableSchema(self, tableName: str):
        try:
            table = self.client.get_table(tableName)
            schema = table.schema
            schema_dict = {field.name: field.field_type for field in schema}
            return schema_dict
        except Exception as e:
            print(traceback.format_exc())
            raise Exception(f"Error retrieving table schema: {e}")