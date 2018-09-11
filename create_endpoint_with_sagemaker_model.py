""" Programatically takes a trained SageMaker model and creates a live HTTP 
    endpoint for real-time inferences.
    Author: Shiraz Hazrat (shiraz@shirazhazrat.com)
    
    Also updates Google Firebase database to allow us to view model status from 
    our dashboards.

    ***Designed to be run as an AWS Lambda function.***

    Requires:
        pip install python-firebase
"""

import boto3
from firebase import firebase


def getFirebaseApp(URL):
    return firebase.FirebaseApplication(URL, None)


def get_job_name(fba, model_id):
    print("Getting model information from Firebase...")
    job_name = fba.get('/crypto/models/{}/metadata/job_name'.format(model_id), None)
    return job_name


def getSagemakerClient():
    return boto3.client('sagemaker')


def get_model_metadata(client, job_name):
    try:
        response = client.describe_training_job(TrainingJobName = job_name)
        return response
    except:
        print("Could not find SageMaker job with name:", job_name)


def create_model(client, model_id, model_metadata):
    paginator = client.get_paginator('list_models')
    response = paginator.paginate(NameContains=model_id)
    for page in response:
        if page['Models']:
            if page['Models'][0]['ModelName'] == model_id:
                print("SageMaker model already exists.")
                return True
    print("Creating SageMaker model...")
    s3_location = model_metadata['ModelArtifacts']['S3ModelArtifacts']
    image = model_metadata['AlgorithmSpecification']['TrainingImage']
    arn = model_metadata['RoleArn']
    response = client.create_model(
        ModelName = model_id,
        PrimaryContainer = {
            'ContainerHostname': "shirazisawesome",
            'Image': image,
            'ModelDataUrl': s3_location
        },
        ExecutionRoleArn = arn)
    return response


def create_endpoint_configuration(client, model_id):
    paginator = client.get_paginator('list_endpoint_configs')
    response = paginator.paginate(NameContains=model_id)
    for page in response:
        if page['EndpointConfigs']:
            if page['EndpointConfigs'][0]['EndpointConfigName'] == model_id:
                print("SageMaker endpoint configuration already exists.")
                return True
    print("Creating SageMaker endpoint configuration...")
    response = client.create_endpoint_config(
            EndpointConfigName = model_id,
            ProductionVariants = [
                {
                    'VariantName': 'variant-1',
                    'ModelName': model_id,
                    'InitialInstanceCount': 1,
                    'InstanceType': 'ml.t2.medium',
                    'InitialVariantWeight': 1
                }])
    return response


def create_endpoint(client, model_id):
    existing_endpoints = client.list_endpoints()
    for endpoint in existing_endpoints['Endpoints']:
        if endpoint['EndpointName'] == model_id:
            print("SageMaker endpoint already exists. Model is online.")
            return True
    print("Creating SageMaker endpoint... model will be online in 10 minutes.")
    response = client.create_endpoint(
        EndpointName = model_id,
        EndpointConfigName = model_id
        )
    print(response)
    return response


def updateFirebase(fba, model_id):
    return fba.put('/crypto/models/{}/'.format(model_id), 'status', "enabled")


def getEndpointStatus(client, model_id):
    response = client.describe_endpoint(EndpointName = model_id)
    return response['EndpointStatus']


def lambda_handler(event, context):
    model_id = event['model_id']
    print("Enabling model:", model_id)
    fba = getFirebaseApp(FIREBASE_URL)
    job_name = get_job_name(fba, model_id)
    client = getSagemakerClient()
    model_metadata = get_model_metadata(client, job_name)
    response = create_model(client, model_id, model_metadata)
    response = create_endpoint_configuration(client, model_id)
    response = create_endpoint(client, model_id)
    response = updateFirebase(fba, model_id)
    status = getEndpointStatus(client, model_id)
    return status