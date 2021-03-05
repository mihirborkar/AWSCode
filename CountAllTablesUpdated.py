import boto3
import time

#Athena queries will be launched in the us-east-2 region, change accordingly
client = boto3.client('athena', region_name='us-east-1')

#Make Sure the S3 bucket that you create to Store your Athena Query is in same region as your Glue Tables whose count you intend to calculate
S3PartitionTable1 = "s3://<YOUR S3 Bucket>/<PREFIX FOR STORING THE LIST OF ALL TABLES>/"

totalTables = str("select table_schema, table_name FROM information_schema.tables where table_schema  <> 'information_schema'")
#print(totalTables)

response1 = client.start_query_execution(
    QueryString = totalTables,
    #ClientRequestToken='',
    QueryExecutionContext={
    'Database': 'information_schema'
    },
    ResultConfiguration={
    'OutputLocation': S3PartitionTable1,
    }
)

#print(response1) 

time.sleep(15) 
response2 = client.get_query_results(
    QueryExecutionId=response1['QueryExecutionId']
)

#print(response2)


#Calculating Total Number of Tables Present in the Entire Glue Catalog/Data Lake
res4=response2['ResultSet']['Rows']
res5=(len(res4) -1)



def countAthenaQuery(i):
    
    data = response2['ResultSet']['Rows'][i]['Data']
    tableSchema = response2['ResultSet']['Rows'][i]['Data'][0]['VarCharValue']
    tableName = response2['ResultSet']['Rows'][i]['Data'][1]['VarCharValue']   

    #Specify the Output Directory where the count of all the tables will be stored 
    S3PartitionTable1 = "s3://chiragbhai123/hhh/op1"
    
    countQuery = "select '" + '{}'.format(tableName) + "' as tableName, count (*) as count from "+ tableSchema+"."+tableName 
    #print(countQuery)
   
    response4 = client.start_query_execution(
       QueryString = countQuery,
       #ClientRequestToken='',
       QueryExecutionContext={
       'Database': tableSchema
       },
       ResultConfiguration={
       'OutputLocation': S3PartitionTable1,
       }
    )
    time.sleep(15)

    response5 = client.get_query_results(
        QueryExecutionId=response4['QueryExecutionId']
    ) 

#Iterating over all the tables 
i = 1
while i <= res5:
   
    try:
        countAthenaQuery(i)
    except :
      tableName = response2['ResultSet']['Rows'][i]['Data'][1]['VarCharValue'] 
      print("Query Execution FAILED or STILL RUNNING for the table: " + tableName+ "\n"+"Proceeding for the next table")
    
    i += 1



