import boto3
from datetime import date
import time
import pprint
 
# Function to update the respective Jobs with Security Configuration
def updateJobFunc(jobName,region,securityConfiguration):
    client = boto3.client('glue', region_name=region)
    response3 = client.get_job(
        JobName= jobName
    )
    try:
        Description = response3['Job']['Description']
        Role =response3['Job']['Role']
        ExecutionProperty =response3['Job']['ExecutionProperty']
        Command=response3['Job']['Command']
        DefaultArguments=response3['Job']['DefaultArguments']
        NonOverridableArguments=response3['Job']['NonOverridableArguments']
        #print(NonOverridableArguments)
        Connections=response3['Job']['Connections']
        #print(Connections)
        MaxRetries=response3['Job']['MaxRetries']
        AllocatedCapacity=response3['Job']['AllocatedCapacity']
        print(AllocatedCapacity)
        AllocatedCapacity = AllocatedCapacity-1
        Timeout=response3['Job']['Timeout']
        MaxCapacity=response3['Job']['MaxCapacity']
        #print(MaxCapacity)
        GlueVersion=response3['Job']['GlueVersion']
        #print(GlueVersion)
        
        print("Updating Job For Spark ETL:"+jobName)
        
        response = client.update_job(
        JobName=jobName,
        JobUpdate={
            'Description': Description,
            'LogUri': '',
            'Role': Role,
            'ExecutionProperty': ExecutionProperty,
            'Command': Command,
            'DefaultArguments': DefaultArguments,
            'NonOverridableArguments': NonOverridableArguments,
            'Connections': Connections,
            'MaxRetries': MaxRetries,
            'Timeout': Timeout,
            'WorkerType': 'Standard',
            'NumberOfWorkers': AllocatedCapacity,
            'SecurityConfiguration': securityConfiguration,
            'GlueVersion': GlueVersion
            }
        )
        
    except:
 
        print("Updating Job For Python-Shell Job: "+jobName)
        Description = response3['Job']['Description']
        Role =response3['Job']['Role']
        ExecutionProperty =response3['Job']['ExecutionProperty']
        Command=response3['Job']['Command']
        DefaultArguments=response3['Job']['DefaultArguments']
        NonOverridableArguments=response3['Job']['NonOverridableArguments']
        #print(NonOverridableArguments)
        MaxRetries=response3['Job']['MaxRetries']
        AllocatedCapacity=response3['Job']['AllocatedCapacity']
        Timeout=response3['Job']['Timeout']
        MaxCapacity=response3['Job']['MaxCapacity']
        #print(MaxCapacity)
        GlueVersion=response3['Job']['GlueVersion']
 
        response = client.update_job(
        JobName=jobName,
            JobUpdate={
                'Description': Description,
                'LogUri': '',
                'Role': Role,
                'ExecutionProperty': ExecutionProperty,
                'Command': Command,
                'DefaultArguments': DefaultArguments,
                'NonOverridableArguments': NonOverridableArguments,
                'MaxRetries': MaxRetries,
                'Timeout': Timeout,
                'MaxCapacity': MaxCapacity,
                'SecurityConfiguration': securityConfiguration,
                'GlueVersion': GlueVersion
            }
        )
# Function to update the respective Crawler with Security Configuration
       
def updateCrawler(crawlerName,region,securityConfiguration):
    client = boto3.client('glue', region_name=region)
    Crawler = client.get_crawler(
        Name=crawlerName
    )
 
    #pprint.pprint(Crawler)
 
    Name = Crawler['Crawler']['Name']
    Role = Crawler['Crawler']['Role']
    DatabaseName = Crawler['Crawler']['DatabaseName']
    Description = Crawler['Crawler']['Description']
    Targets = Crawler['Crawler']['Targets']
    Classifiers = Crawler['Crawler']['Classifiers']
    TablePrefix = Crawler['Crawler']['TablePrefix']
    SchemaChangePolicy = Crawler['Crawler']['SchemaChangePolicy']
 
    #print(Targets)
 
    response = client.update_crawler(
        Name=Name,
        Role=Role,
        DatabaseName=DatabaseName,
        Description=Description,
        Targets=Targets,
        Classifiers=Classifiers,
        TablePrefix=TablePrefix,
        SchemaChangePolicy=SchemaChangePolicy,
        CrawlerSecurityConfiguration=securityConfiguration
    )        
                
#Provide the Database Snapshot Name for which Security Configuration needs to be provided  
def addSecurityConfig(workFlowName,region,securityConfiguration):
    client = boto3.client('glue', region_name=region)
    response = client.get_workflow(
        Name=workFlowName,
        IncludeGraph=True
    )
    try:
 
        #If the Workflow was previously run
        a = response['Workflow']['LastRun']['Graph']['Nodes'][0]['TriggerDetails']['Trigger']
 
        print("The WorkFlow has been previously run")
    
        #Getting the Crawler that will be used in the Discovery state
        crawlerName= a['Predicate']['Conditions'][0]['CrawlerName']
        print("updating crawler name : " +crawlerName)
        #call the updateCrawlerFunc
        updateCrawler(crawlerName,region,securityConfiguration)
        # Listing the total Nos of Jobs without the Crawler discovery Failed and Stopped
        length =(len(a['Predicate']['Conditions']))
        # Getting the Jobs that will run if the Crawler Succeeds
        for i in range(1,length):
            jobName = a['Predicate']['Conditions'][i]['JobName']
            updateJobFunc(jobName,region,securityConfiguration)
 
        jobForCrawlerDiscoveryFailure = response['Workflow']['LastRun']['Graph']['Nodes'][1]['TriggerDetails']['Trigger']['Actions'][0]['JobName']
        #print(jobForCrawlerDiscoveryFailure)
        updateJobFunc(jobForCrawlerDiscoveryFailure,region,securityConfiguration)
 
        jobForCrawlerDiscoveryCancelled = response['Workflow']['LastRun']['Graph']['Nodes'][0]['TriggerDetails']['Trigger']['Actions'][0]['JobName']
        #print(jobForCrawlerDiscoveryCancelled)
        updateJobFunc(jobForCrawlerDiscoveryCancelled,region,securityConfiguration)
 
 
 
    except:
 
        print('The WorkFlow has not been previously run')
        a = response['Workflow']['Graph']['Nodes'][0]['TriggerDetails']['Trigger']
 
        #Getting the Crawler that will be used in the Discovery state
        crawlerName= a['Predicate']['Conditions'][0]['CrawlerName']
        print("updating crawler name : " +crawlerName)
        #call the updateCrawlerFunc
        updateCrawler(crawlerName,region,securityConfiguration)
        
        # Listing the total Nos of Jobs without the Crawler discovery Failed and Stopped
        length =(len(a['Predicate']['Conditions']))
        # Getting the Jobs that will run if the Crawler Succeeds
        for i in range(1,length):
            jobName = a['Predicate']['Conditions'][i]['JobName']
            updateJobFunc(jobName,region,securityConfiguration)
 
        jobForCrawlerDiscoveryFailure = response['Workflow']['Graph']['Nodes'][1]['TriggerDetails']['Trigger']['Actions'][0]['JobName']
        #print(jobForCrawlerDiscoveryFailure)
        updateJobFunc(jobForCrawlerDiscoveryFailure,region,securityConfiguration)
 
        jobForCrawlerDiscoveryCancelled = response['Workflow']['Graph']['Nodes'][0]['TriggerDetails']['Trigger']['Actions'][0]['JobName']
        #print(jobForCrawlerDiscoveryCancelled)
        updateJobFunc(jobForCrawlerDiscoveryCancelled,region,securityConfiguration)
 
 
 
# Parameters required : WorkFlowName, Region, SecurityConfiguration
addSecurityConfig('concurrency_1','us-east-1','neww')   