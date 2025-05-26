from etl_target import TargetETL
from Utils.GoogleCloudHandlers.bigquery_handler import BigQueryHandler 
import json
import time
import datetime 

class FetchAndImportTargetData:
    def __init__(self, credentials : dict = None) -> None:
        self.targetETL = TargetETL(credentials=credentials)
        self.bigQueryHandler = BigQueryHandler(serviceAccountJson=json.load(open("var/bigquery_service_account.json", "r")))
        self.reportTypes = ["PAYOUT_RECONCILIATION","RETURN_ORDERS_EXTERNAL","ORDERS"]
        self.reports = {}
        last_month_last_day = datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)
        last_month_first_day = last_month_last_day.replace(day=1)   
        self.start_date = last_month_first_day.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self.end_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def run(self):
        ongoing_reports = [r for r in self.targetETL.getReports(created_by="c8fdbb1d2a3d9d5538dccce24b06261d4f801a2b2f1bd35a2a705428c0c125e5") if r["status"] in ["PROCESSING","PENDING","QUEUED"]]
        if ongoing_reports:
            print(f"{len(ongoing_reports)} ongoing reports found. Terminating them...")
        for report in ongoing_reports:
            if report["status"] == "COMPLETE":
                print(f"Report {report['id']} is already complete. Skipping termination.")
                continue
            if report["status"] == "CANCELED":
                print(f"Report {report['id']} is already cancelled. Skipping termination.")
                continue
            else:
                print(f"Terminating report {report['id']}...")
                self.targetETL.terminateReport(reportId=report["id"])

        # Initialize the reports
        for reportType in self.reportTypes:
            print(f"Initializing report for {reportType}...")
            report = self.targetETL.initializeReport(reportType=reportType, start_date= self.start_date, end_date=self.end_date, format="EXCEL")
            self.reports[report["id"]] = report
        
        while self.reports != {}:
            for reportId in list(self.reports.keys()):
                report = self.targetETL.getReportStatus(reportId=reportId)
                if report["status"] == "CANCELLED":
                    print(f"Report {reportId} was cancelled.")
                    del self.reports[reportId]
                elif report["status"] == "COMPLETE":
                    print(f"Report {reportId} is completed.")
                    dataframe = self.targetETL.getReportData(report=report)
                    self.bigQueryHandler.Loader.loadDataframeToBigQuery(table_name=report["type"].lower(), dataset_name="target_test", dataframe=dataframe)
                    del self.reports[reportId]
                else:
                    print(f"Report {reportId} is still processing. Status: {report['status']}")
            time.sleep(10)
        

if __name__ == "__main__":
    target = TargetETL()
    fetchAndImportTargetData = FetchAndImportTargetData()
    fetchAndImportTargetData.run()