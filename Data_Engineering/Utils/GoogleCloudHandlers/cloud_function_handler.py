import requests
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account
import traceback

class CloudFunctionHandler:
    def __init__(self, service_account_file):
        self.service_account_file = service_account_file
        self.project_id = service_account_file['project_id']
        self.credentials = self.getCredentials()
        self.ACCESS_TOKEN = self.credentials.token    
    
    def getCredentials(self):
        credentials = service_account.Credentials.from_service_account_info(self.service_account_file,
                                                                            scopes=['https://www.googleapis.com/auth/cloud-platform'])
        request = google.auth.transport.requests.Request()
        credentials.refresh(request)
        return credentials

    def getCloudLocations(self):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.get(url, headers=headers)
        return response.json()

    def getCloudOperations(self):
        url = f"https://cloudfunctions.googleapis.com/v1/operations"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.get(url, headers=headers)
        return response.json()

    def getCloudOperation(self, operation_name):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/operations/{operation_name}"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.get(url, headers=headers)
        return response.json()

    def callCloudFunction(self, location, function_name, data = None):
        if data is None:
            data = str({})
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions/{function_name}:call"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        body = {"data": data}
        response = requests.post(url, headers=headers, json=body)
        return response.json()
    
    def getCloudFunction(self, location, function_name):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions/{function_name}"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.get(url, headers=headers)
        return response.json()
    
    def listCloudFunctions(self, location):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        try:
            response = requests.get(url, headers=headers)
            responseCode = response.status_code
            if responseCode == 200:
                return response.json(), responseCode
            else:
                return "", responseCode
        except:
            return "", 500
        
    def createCloudFunction(self, location, data):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        data['name'] = f"projects/{self.project_id}/locations/{location}/functions/{data['name']}"
        response = requests.post(url, headers=headers, json=data)
        return response.json()
    
    def updateCloudFunction(self, location, function_name, data):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions/{function_name}"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.patch(url, headers=headers, json=data)
        return response.json()
    
    def deleteCloudFunction(self, location, function_name):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions/{function_name}"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.delete(url, headers=headers)
        return response.json()
    
    def generateUploadUrl(self, location,kmsKeyName):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions:generateUploadUrl"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}',
                   'Content-Type' : 'application/zip'}
        params = {'kmsKeyName' : kmsKeyName}
        response = requests.post(url, headers=headers, params=params)
        try:
            return response.json()['uploadUrl']
        except Exception as e:
            print(e)
            print(response.json())
            traceback.print_exc()
            return None

    def generateDownloadUrl(self, location):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions:generateDownloadUrl"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.get(url, headers=headers)
        return response.json()
    
    def createFunction(self, name : str, entryPoint : str, runtime : str,  description : str = None,
                        availableMemoryMb : int = 256,timeout : str = '60s', eventTrigger : dict = None, httpsTrigger : dict = None, labels : dict = None, environmentVariables : dict = None,
                        buildEnvironmentVariables : dict = None, maxInstances : int = None, minInstance : int = None, vpcConnector : str = None,
                        serviceAccountEmail : str = None, vpcConncectorEgressSettings : list = None, ingressSettings : str = None,
                        kmsKeyname : str = None, buildWorkerPool : str = None, secretEnvironmentVariables : dict = None,
                        secretVolumes : dict = None, sourceToken : str = None, dockerRepository : str = None, dockerRegistry : list = None,
                        buildServiceAccount : str = None,sourceArchiveUrl : str = None, sourceRepository : str = None, sourceUploadUrl : str = None,
                        automaticUpdatePolicy : dict = None, onDeployUpdatePolicy : dict = None):
        """Creates a dictionary object for a cloud function. Use the createCloudFunction method to create the function in the cloud.
        * Can only have one of sourceArchiveUrl, sourceRepository, or sourceUploadUrl.
        * Can only have one of automaticUpdatePolicy or onDeployUpdatePolicy.
        * Can only have either eventTrigger or httpsTrigger.
        Args:
            name (str): A user-defined name of the function. Function names must be unique globally and match pattern projects/*/locations/*/functions/*
            description (str): The description of the function
            entryPoint (str): The name of the function (as defined in source code) that will be executed. Defaults to the resource name suffix (ID of the function), if not specified.
            runtime (str): The runtime in which to run the function. Required when deploying a new function, optional when updating an existing function. For a complete list of possible choices, see the gcloud reference guides.
            availableMemoryMb (int): The amount of memory in MB available for a function. Defaults to 256MB.
            timeout (str): The function execution timeout. Execution is considered failed and can be terminated if the function is not completed at the end of the timeout period. Defaults to 60 seconds.
            eventTrigger (dict): A source that fires events in response to a condition in another service.
            httpsTrigger (dict): An HTTPS endpoint type of source that can be triggered via URL.
            labels (dict): Labels associated with this Cloud Function.
            environmentVariables (dict): Environment variables that shall be available during function execution.
            buildEnvironmentVariables (dict): Environment variables that shall be available during build and test execution.
            maxInstances (int): The maximum number of instances that your function can scale up to. Default is 1000.
            minInstance (int): The minimum number of instances that your function can scale down to. Default is 0.
            vpcConnector (str): The VPC Network Connector that this cloud function can connect to. It can be either the fully-qualified URI, or the short name of the network connector resource.
            serviceAccountEmail (str): The email of the service account that will be used to call the function if the function is triggered by an event from a different service.
            vpcConncectorEgressSettings (list): The egress settings for the connector, controlling what traffic is diverted through it.
            ingressSettings (str): The ingress settings for the function, controlling what traffic can reach it.
            kmsKeyname (str): The name of the Cloud KMS key that will be used to encrypt the function's environment variables. The format is projects/*/locations/*/keyRings/*/cryptoKeys/*.
            buildWorkerPool (str): The name of the Cloud Build worker pool to use for function deployment. If not specified, the function will use the default worker pool.
            secretEnvironmentVariables (dict): Environment variables that are encrypted using a Cloud Key Management Service crypto key. These values must be base64 encoded strings.
            secretVolumes (dict): A list of secret volumes to mount. In order to use this feature, the service account making the API request must have the roles/secretmanager.secretAccessor IAM role.
            sourceToken (str): The source token is a unique token to be specified in the Source token field of the Source Repository.
            dockerRepository (str): The name of the Docker container repository that the function will be deployed to.
            dockerRegistry (list): The URL to the hosted repository where the function is stored. This value is returned in the projects.locations.functions.sourceRepository URL.
            buildServiceAccount (str): The service account that will be authorized to make pull requests on the repository. This service account needs to be in the same project as the function.
            sourceArchiveUrl (str): The URL to the hosted zip file where the function is stored.
            sourceRepository (str): The URL to the hosted repository where the function is stored.
            sourceUploadUrl (str): The URL to the hosted repository where the function is stored.
            automaticUpdatePolicy (dict): The update policy for the function. Note that the default value is NEVER.
            onDeployUpdatePolicy (dict): The update policy for the function. Note that the default value is NEVER.
        Returns:
            dict: A dictionary object for a cloud function
        """
        try:
            if automaticUpdatePolicy is not None and onDeployUpdatePolicy is not None:
                raise ValueError("Cannot have both automaticUpdatePolicy and onDeployUpdatePolicy")
            if eventTrigger is not None and httpsTrigger is not None:
                raise ValueError("Must have either eventTrigger or httpsTrigger")
            if (sourceArchiveUrl is not None) + (sourceRepository is not None) + (sourceUploadUrl is not None) > 1:
                raise ValueError("Must have only one of sourceArchiveUrl, sourceRepository, or sourceUploadUrl")

            function = {
                "name" : name,
                "description" : description,
                "entryPoint" : entryPoint,
                "runtime" : runtime,
                "timeout" : timeout,
                "availableMemoryMb" : availableMemoryMb,
                "eventTrigger" : eventTrigger,
                "httpsTrigger" : httpsTrigger,
                "labels" : labels,
                "environmentVariables" : environmentVariables,
                "buildEnvironmentVariables" : buildEnvironmentVariables,
                "maxInstances" : maxInstances,
                "minInstance" : minInstance,
                "vpcConnector" : vpcConnector,
                "serviceAccountEmail" : serviceAccountEmail,
                "vpcConncectorEgressSettings" : vpcConncectorEgressSettings,
                "ingressSettings" : ingressSettings,
                "kmsKeyname" : kmsKeyname,
                "buildWorkerPool" : buildWorkerPool,
                "secretEnvironmentVariables[]" : secretEnvironmentVariables,
                "secretVolumes[]" : secretVolumes,
                "sourceToken" : sourceToken,
                "dockerRepository" : dockerRepository,
                "dockerRegistry" : dockerRegistry,
                "buildServiceAccount" : buildServiceAccount,
                "sourceArchiveUrl" : sourceArchiveUrl,
                "sourceRepository" : sourceRepository,
                "sourceUploadUrl" : sourceUploadUrl,
                "automaticUpdatePolicy" : automaticUpdatePolicy,
                "onDeployUpdatePolicy" : onDeployUpdatePolicy
            }

            function = {k: v for k, v in function.items() if v is not None}

            return function
        except ValueError as e:
            print(e)
            return None

    def uploadFunctionToStorage(self, uploadUrl, zippedFunctionPath):
        try:
            with open(zippedFunctionPath, 'rb') as f:
                header = {'Content-Type': 'application/zip',
                          'x-goog-content-length-range': '0,104857600'}
                response = requests.put(uploadUrl,headers=header, data=f)
                return response.content
        except Exception as e:
            print(e)
            traceback.print_exc()
            return None
        
