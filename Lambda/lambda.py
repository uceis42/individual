import os
import io
import boto3
import json
import numpy as np

# grab environment variables here
ENDPOINT_NAME = "xgboost-2022-04-23-14-09-27-392"
runtime= boto3.client('runtime.sagemaker')

# np2csv fucntion takes array to csv format
def np2csv(arr):

    csv = io.BytesIO()

    np.savetxt(csv, arr, delimiter=",", fmt="%g")

    return csv.getvalue().decode().rstrip()
# lambda handler to recieve event and invoke endpoint, then return the prediction outcome
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    
    data = json.loads(json.dumps(event))
    payload = data["data"]
    #payload = np2csv(payload)
    print(payload)
# invoke endpoint here 
    response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                       Body=np2csv(payload),ContentType="text/csv")
    print(response)
    result = json.loads(response['Body'].read().decode())
    print(result)
    
    return result
    
