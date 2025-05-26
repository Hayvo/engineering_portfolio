import traceback
import json 
import regex as re
from datetime import datetime
from utils.queryFormatter import QueryFormatter

class FileHandler():

    def __init__(self):
        """Initialize the FileHandler class."""
        self.queryFormatter = QueryFormatter()
    
    def openFile(self,filePath : str) -> str:  
        """Open a file and return its contents.
        Args:
            filePath (str): The path to the file.
        Returns:
            str: The contents of the file."""
        try: 
            with open(filePath) as f:
                return f.read()
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Couldn't open file {filePath} : {e}")
    

    def formatQuery(self,projectId :str, datasetId : str, sql : str, params : list) -> str:
        """Format a query.
        Args:
            sql (str): The query to format.
            projectId (str): The project ID.
            datasetId (str): The dataset ID.
        Returns:
            str: The formatted query."""
        try:
            return  self.queryFormatter.formatQuery(
                    projectId= projectId, datasetId= datasetId, sql= sql, 
                    params = params)
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting query: {e}")

    def formatParam(self, param : dict) -> dict:
        """Format a parameter.
        Args:
            param (dict): The parameter to format.
        Returns:
            dict: The formatted parameter."""
        paramName = param["name"]
        matching = {
            "date" : "DATE",
            "datetime-local" : "DATE",
            "text" : "STRING",
            "number" : "INT64",
            "signed" : "INT64"
            }
        paramType = matching.get(param["type"].lower(), "STRING")
        return {'Name' : paramName, 'Type' : paramType}

    def redashJsonToBigueryFunction(self,redashJson : dict, projectId : str, datasetId : str) -> tuple[str,dict]:
        """Convert a Redash query JSON to a BigQuery function.
        Args:
            redashJson (dict): The Redash query JSON.
            projectId (str): The project ID.
            datasetId (str): The dataset ID.
        Returns:
            str: The BigQuery query to create the associated function."""
        try:
            reportName = redashJson["name"]
            reportId = redashJson["id"]
            reportTags = redashJson["tags"]
            parameters = redashJson['options'].get('parameters', [])
            formattedParameters = [self.formatParam(param) for param in parameters]
            reportSQL = self.formatQuery(projectId = projectId, datasetId = datasetId, sql = redashJson["query"], params = formattedParameters)
            metadata = {
                'Name' : reportName,
                'SQL' : reportSQL,
                'ReportId' : reportId,
                'Params' : formattedParameters,
                'Tags' : reportTags
            }
        
            return reportSQL,metadata
        
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error converting Redash JSON to BigQuery function: {e}")
        
