from dotenv import load_dotenv
import os
import requests
import json
import traceback
import glob

class ReDashHandler:
    def __init__(self):
        load_dotenv()
        self.apiKey = os.getenv("REDASH_API_KEY")
        self.baseUrl = os.getenv("REDASH_URL")
        self.headers = {"Authorization": f"Key {self.apiKey}"}

    def openRedashQueryJson(self,queryId : int) -> dict:
        """Open a Redash query JSON file.
        Args:
            queryId (str): The ID of the query.
        Returns:
            dict: The query JSON."""
        try:
            filePath = "queries/redash/DL_Report_" + str(queryId) + ".json"
            return json.loads(open(filePath).read())
        except FileNotFoundError:
            print(f"Query file not found, downloading from Redash API")
            queryJson = self.getQueryById(queryId)
            if queryJson is None:
                raise Exception(f"Couldn't open file {filePath}")
            else:
                self.saveJsonQuery(queryJson)
                return queryJson
        except Exception as e:
            print(e)
            raise Exception(f"Couldn't open file {filePath} : {e}")

    def getQueryById(self, queryId : int) -> dict:
        try:
            response = requests.get(f"{self.baseUrl}/queries/{queryId}", headers=self.headers)
            response.raise_for_status()
            print(response.json())
            return response.json()
        except Exception as e:
            print(e)
            return None

    def getQueries(self, page, page_size):
        try:
            response = requests.get(f"{self.baseUrl}/queries?page={page}&page_size={page_size}", headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(e)
            return None
        
    def getMaxQueryId(self):
        try:
            files = glob.glob("queries/redash/DL_Report_*.json")
            ids = [int(os.path.basename(file).split("_")[2].split(".")[0]) for file in files]
            return max(ids)
        except Exception as e:
            print(e)
            return None

    def getQueryCount(self):
        try:
            response = requests.get(f"{self.baseUrl}/queries", headers=self.headers)
            response.raise_for_status()
            return response.json()["count"]
        except Exception as e:
            print(e)
            raise Exception(f"Couldn't get query count: {e}")
        
    def getAllQueries(self):
        try:
            queryCount = self.getQueryCount()
            queries = []
            currentQueryCount = 0
            while currentQueryCount < queryCount:
                response = self.getQueries(int(currentQueryCount/50)+1, 50)
                queries += response["results"]
                currentQueryCount += 50
            return queries
        except Exception as e:
            print(e)
            return None
        
    def saveJsonQuery(self,query):
        try:
            with open(f"queries/redash/DL_Report_{query['id']}.json", "w") as file:
                json.dump(query, file, indent=4)
        except Exception as e:
            print(e)
            return None
