import requests
import json
import csv 
import pandas as pd
import io 


class TargetETL:
    def __init__(self, credentials : dict = None) -> None:
        self.target_url = "https://api.target.com/sellers/v1/sellers"
        self.credentials = self.getCredentials(credentials)
        self.headers = {
            "x-api-key": f"{self.credentials['app_key']}",
            "x-seller-id": f"{self.credentials['seller_id']}",
            "x-seller-token": f"{self.credentials['api_token']}",
            # No Content-Type: requests handles it for multipart
        }

    def getCredentials(self,credentials : dict = None) -> dict:
        if credentials:
            return credentials
        else:
            try:
                with open("var/target_credentials.json", "r") as file:
                    credentials = json.load(file)
                return credentials
            except FileNotFoundError:
                print("Credentials file not found. Please provide credentials.")
                raise FileNotFoundError("Credentials file not found. Please provide credentials.")
            except json.JSONDecodeError:
                print("Error decoding JSON. Please check the credentials file.")
                raise json.JSONDecodeError("Error decoding JSON. Please check the credentials file.")
            except Exception as e:
                print(f"An error occurred: {e}")
                raise e
            
    def makeRequest(self, endpoint : str = None, method : str = "GET", payload : dict = None, params : dict = None, url : str = None) -> dict:
        """
        Make a request to the Target API.

        Args:
            endpoint (str): The API endpoint to call.
            method (str): The HTTP method to use (GET, POST, PUT, DELETE).
            payload (dict): The data to send in the request body (for POST/PUT).
            params (dict): The query parameters to include in the request.
            url (str): The full URL to call (if not using the endpoint).
        Returns:
                dict: The JSON response from the API.
        """
        if url is None:
            if endpoint is None:
                raise ValueError("Either endpoint or url must be provided.")
            url = self.target_url + endpoint
        try:
            if method == "GET":
                response = requests.get(url, headers=self.headers, params=params)
            elif method == "POST":
                response = requests.post(url, headers=self.headers, json=payload)
            elif method == "PUT":
                response = requests.put(url, headers=self.headers, json=payload)
            elif method == "DELETE":
                response = requests.delete(url, headers=self.headers)
                response.raise_for_status()  # Raise an error for bad responses
                return True
            else:
                raise ValueError("Invalid HTTP method specified.")

            response.raise_for_status()  # Raise an error for bad responses
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            raise e
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            raise e
        except json.JSONDecodeError:
            print("Error decoding JSON response.")
            raise json.JSONDecodeError("Error decoding JSON response.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise e

    def makeMultipartRequest(self, endpoint: str, files: dict) -> dict:
        
        url = self.target_url + endpoint
        try:
            response = requests.post(url, headers=self.headers, files=files)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            raise e


    def initializeReport(self, reportType: str, start_date: str, end_date: str, format: str = "CSV", file_path: str = None) -> dict:
        endpoint = f"/{self.credentials['seller_id']}/report_requests"

        report_json = {
            "type": reportType,
            "start_date": start_date,
            "end_date": end_date,
            "parameters": {
                "include_metadata": True
            },
            "format": format
        }

        # Set up multipart form-data
        files = {
            "report_request": (None, json.dumps(report_json), "application/json"),
        }

        if file_path:
            files["report_input"] = open(file_path, "rb")

        report = self.makeMultipartRequest(endpoint, files=files)

        return report

    def getReports(self, report_type: str = None,status : str = None, format: str = None, created_by: str = None) -> dict:
        endpoint = f"/{self.credentials['seller_id']}/report_requests"
        # Optional filtering parameters
        params = {
            "per_page": "100"
        }
        if report_type:
            params["type"] = report_type
        if format:
            params["format"] = format
        if created_by:
            params["created_by"] = created_by

        try:
            report_response = self.makeRequest(endpoint, params=params)
            if not isinstance(report_response, list):
                print("Unexpected response format. Expected a dictionary.")
                return []
            return report_response
        except Exception as e:
            print(f"Error retrieving report: {e}")
            return {}

    def getReadyReports(self, report_type: str = None, format: str = None, created_by: str = None) -> list:
        try:
            reports_response = self.getReports(report_type=report_type, format=format, created_by=created_by)
            if not reports_response:
                print("No reports found or error occurred.")
                return []
            # Make sure response contains the expected data
            if not isinstance(reports_response, list):
                print("Unexpected response format. Expected a list.")
                return []
            ready_reports = [report for report in reports_response if report.get("status") == "COMPLETE"]
            return ready_reports
        except Exception as e:
            print(f"Error retrieving reports: {e}")
            return []

    def getReportStatus(self, reportId: str) -> dict:
        endpoint = f"/{self.credentials['seller_id']}/report_requests/{reportId}"
        try:
            report_response = self.makeRequest(endpoint)
            if not isinstance(report_response, dict):
                print("Unexpected response format. Expected a dictionary.")
                return {}
            return report_response
        except Exception as e:
            print(f"Error retrieving report status: {e}")
            return {}


    def getReportData(self, report: dict) -> pd.DataFrame:
        response = requests.get(report["download_url"], headers=self.headers, stream=True)
        if response.status_code == 200:
            if report["format"] == "CSV":
                # Decode and read into list of dicts using csv.DictReader
                content = response.content.decode("utf-8")
                reader = csv.DictReader(io.StringIO(content))
                df = pd.DataFrame(reader)
                return df

            elif report["format"] == "EXCEL":
                # Use pandas to read Excel content
                df = pd.read_excel(io.BytesIO(response.content))
                return df

            else:
                print("Unsupported format.")
                return []
        else:
            print(f"Failed to download report: {response.status_code}")
            return []


    def terminateReport(self, reportId: str) -> dict:
        endpoint = f"/{self.credentials['seller_id']}/report_requests/{reportId}"
        try:
            if self.makeRequest(endpoint, method="DELETE"):
                print(f"Report {reportId} terminated successfully.")
            else:
                raise Exception("Failed to terminate report.")
        except Exception as e:
            print(f"Error terminating report: {e}")
            return {}
        
    def terminateReports(self, reportIds: list) -> dict:
        for reportId in reportIds:
            self.terminateReport(reportId=reportId)