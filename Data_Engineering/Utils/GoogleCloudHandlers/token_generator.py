import google.auth.transport.requests
from google.oauth2 import service_account

def getAccessToken(service_account_file):
        credentials = service_account.Credentials.from_service_account_info(
            service_account_file,
            scopes=['https://www.googleapis.com/auth/cloud-platform'])
        request = google.auth.transport.requests.Request()
        credentials.refresh(request)
        return credentials.token