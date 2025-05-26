from GoogleCloudHandlers.cloud_function_handler import CloudFunctionHandler
from createETLFunctionFolder import FunctionFolderMaker
from GoogleCloudHandlers.pub_sub_handler import PubSubHandler
import json


project_id = "{{project_id}}"
platform = "{{platform}}"
with open(f'./src/var/login_credentials/{project_id}/admin_service_account.json') as f:
    service_account_file = json.load(f)
    cloudFunctionHandler = CloudFunctionHandler(service_account_file)
    pubSubHandler = PubSubHandler(service_account_file)
    functionFolderMaker = FunctionFolderMaker()
    functionFolderMaker.createFunctionFolder(project_id,platform)
    uploadUrl = cloudFunctionHandler.generateUploadUrl('northamerica-northeast1',f'projects/{project_id}/locations/northamerica-northeast1/keyRings/storage_kms_key/cryptoKeys/bs_storage')
    print(uploadUrl)
    proj = project_id.replace('-','_')
    zippedFunctionPath = f'./src/temp/{proj}_{platform}.zip'
    print(cloudFunctionHandler.uploadFunctionToStorage(uploadUrl, zippedFunctionPath))

    newFunction = cloudFunctionHandler.createFunction(name = 'elt-bin-ads',
                                                    entryPoint = 'main',
                                                    runtime = 'python311',
                                                    availableMemoryMb = 512,
                                                    timeout = '540s',
                                                    sourceUploadUrl = uploadUrl,
                                                    eventTrigger = {"eventType":"google.pubsub.topic.publish",
                                                                    "resource":f"projects/{project_id}/topics/etl"}) 
    print(cloudFunctionHandler.createCloudFunction('northamerica-northeast1',newFunction))