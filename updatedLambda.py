import json
import boto3
import os
import sys
import uuid
import botocore
import re

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        key=key.replace('+'," ")
    
        print("key=",key)
        x = key.split('/')
        
        fileName=str(x[1])
        fileName=fileName.replace('+'," ")
        print("fileName="+fileName)
        
        GlueJobName= str(x[1]).strip(".py").strip()
        
        print("GlueJobName=",GlueJobName)
        region= (bucket.split('prod-')[1])
        #prefix=key.strip(fileName)
        
        #print("prefix=",prefix)
        
        tempDir=("s3://"+bucket+'/'+x[0])
        print("TempDir=",tempDir)
        scriptLocation=(tempDir+'/'+fileName)
        print("scriptLocation=",scriptLocation)
        
        glueClient = boto3.client('glue',region)
        try:
            response = glueClient.start_job_run(
            JobName=GlueJobName
            )
            
        except botocore.exceptions.ClientError as e:
        #    print("Unexpected error: %s" % e)

            regionConnection =  {
                "us-east-1": "a205451-aurora-datasource",
                "CA-central-1": "a205451-Aurora-datasource",
                "ap-southeast-2": "a205451-Aurora-datasource",
                "sa-east-1": "a205451-aurora-sa",
                "eu-west-2":"a205451-Aurora-datasource"
                }
            
            print("Glue Job does not exists in the current account for the region:" +region+" thus tryinng to create one")
            try:    
                connection =regionConnection[region]
                print(connection)
                response = glueClient.get_connection(
                Name=connection
                )
            except botocore.exceptions.ClientError as e:
                print("connection not avaiable in the region, kindly create a connection with the name", connection)

            #Kindly Update here, if future needs mandate the IAM role used for exeuting your Glue Job changes        
            Role='a205451-IamRoles-roles-GlueServiceRole-59JG0YG1IODO'

            try:
                IAMClient = boto3.client('iam',region)
                response = IAMClient.get_role(
                RoleName= Role
                )
                #import pprint; pprint.pprint(response)
            except botocore.exceptions.ClientError as e:
                print("IAM Role not available in the account, please create the following role which will be used by Glue ETL Execution", Role)  
                
            # CreateGlueJob 
            #temporaryDir=("s3://"+bucket+'/'+prefix)
            #print("temporaryDir=",temporaryDir)
            response = glueClient.create_job(
            Name=GlueJobName,
            Description='Created From DetectAndTriggerGlueJob Lambda Function',
            
             
            Role=Role,
            ExecutionProperty={
                'MaxConcurrentRuns': 123
            },
            Command={
                'Name': 'glueetl',
                'ScriptLocation': scriptLocation,
                'PythonVersion': '3'
            },
             DefaultArguments={
            '--job-language': 'python',
            '--TempDir': tempDir
             },
            # NonOverridableArguments={
            #     'string': 'string'
            # },
            Connections={
                'Connections': [
                    connection,
                ]
            },
            MaxRetries=123,
            #AllocatedCapacity=10,
            Timeout=123,
            #MaxCapacity=10.0,
            #If you want Glue to encrypt the S3 result with specific KMS Key, kindly create a Security Configuration and pass the value herein
            # SecurityConfiguration='string',
            #If you want to tag your respective Glue job, kindly un-comment the below field
            # Tags={
            #     'string': 'string'
            # },
            GlueVersion='2.0',
            NumberOfWorkers=10,
            WorkerType='G.1X'
            )
            #SNS Notification to notify that a new Glue Job has been created 
            

            