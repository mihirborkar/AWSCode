#######################################################################################################################################################################################################
# Arguments that are  needed to be passed to the Function "CreatePartition" : TargetBucketName, SourceTable,DatabaseName,PartitionKey                                                                 #
# The script creates a partitioned table in a new folder such as date=<yyyy-mm-dd> at an S3 location  :s3://<YOURBUCKET>/<YOUR TABLE NAME>/                                                           #
# The new Folder will be overwritten for any consecutive re-runs for the same day, however will not delete previous date partitions                                                                   #
# The Created Partitioned table will always be pointing at the latest S3 location containing todays date                                                                                              #
# All the partitioned tables created by the script will have the suffix _ctas_partitioned as  "<YOUR TABLE>_ctas_partitioned"                                                                         #                                                                                                                                                  #
#######################################################################################################################################################################################################

import boto3
from datetime import date
import time


def CreatePartition(bucket,table,database,partition_key):
    today = date.today()
    print(today)

    a = str(today)
    print(a)

    #Dropping the table from Athena/droping the meta-data 
    client = boto3.client('athena', region_name='us-west-2')
    S3PartitionTable1 = str('s3://'+bucket+'/'+table+'/date='+a+'/') 
    dropString = str("drop table if EXISTS " + table +"_ctas_partitioned")
    print(dropString)
    response1 = client.start_query_execution(
        QueryString = dropString,
        #ClientRequestToken='',
        QueryExecutionContext={
        'Database': database
        },
        ResultConfiguration={
        'OutputLocation': S3PartitionTable1,
        }
    )
    
    
    time.sleep(15) 
    
    # Deleting the partitions in S3 corresponding to today's date
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    prefix=str(table+'/date='+a+'/')
    bucket.objects.filter(Prefix=prefix).delete()

    time.sleep(10) 

    # Using the get-tables api to get all the columns of your table
    client = boto3.client('glue', region_name='us-west-2')
    response = client.get_table(
    DatabaseName= database,
    Name= table
    )
    
    #a = response['Table']
    e = response['Table']['StorageDescriptor']['Columns']

    columns = []
    #updated_columns = []

    for i in e:
        b = dict(i)
        val_list = list(b.values())
        columns.append(val_list[0])

    columns.remove(partition_key)
    columns.append(partition_key)
    columns = str(columns)
    columns=(columns.replace("'", "").strip('[]'))
    print(columns)
    
    #Building the CTAS Query 
    QueryString1= "CREATE TABLE " + table+"_ctas_partitioned " +"WITH (format = 'PARQUET', external_location = '"+ S3PartitionTable1+"', partitioned_by = ARRAY['" + partition_key + "']) AS SELECT " + columns + " FROM "+table

    print(QueryString1) 

    # Running the CTAS Query to create the partitioned table 
    client3 = boto3.client('athena', region_name='us-west-2')
    response2 = client3.start_query_execution(
        QueryString = QueryString1,
        QueryExecutionContext={
            'Database': database
            },
        ResultConfiguration={
            'OutputLocation': S3PartitionTable1,

            }
        )
    


#Calling the CreatePartition function to generate the corresponding partitioned table 
#Arguments Needed: TargetBucketName, SourceTable,DatabaseName,PartitionKey     

# Invoke the function for multiple tables
CreatePartition("testingdate","testmihir123","mihir","instructor")
CreatePartition("testingdate","jeinkyun","mihir","cid")


# Note:
# Athena only allows 100 partitions in a table when that table is generated using CTAS query. If you have more that 100 unique values for your partition key in your table, then it will throw an error: "HIVE_TOO_MANY_OPEN_PARTITIONS".
# Having said that, if you want to create CTAS query with partition, you need to use the column which has <100 unique row values in the table and it will run perfectly.
# Thus, while considering the option of using CTAS to generate your partitioned tables, refer to the limitations of CTAS :- 
# https://docs.aws.amazon.com/athena/latest/ug/considerations-ctas.html

# Regarding implementing the same using Spark-SQL, refer the below document which talks about how you can use spark-sql in an AWS Glue ETL.:- 
# - https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-data-catalog-hive.html 
# - https://spark.apache.org/docs/1.1.0/sql-programming-guide.html 
# However, If you want to implement CTAS in a less resource intensive way, then recommend you to opt for Glue Python Shell job rather than Spark-SQL.

# Difference between using CTAS for Presto vs Spark :- 
# Presto is an open source distributed SQL query engine for running interactive analytic queries against data sources of all sizes ranging from gigabytes to petabytes.
# Wherein, Spark is a fast and general processing engine compatible with Hadoop data. It can run in Hadoop clusters through YARN or Spark's standalone mode, and it can process data in HDFS, HBase, Cassandra, Hive, and any Hadoop InputFormat. It is designed to perform both batch processing (similar to MapReduce) and new workloads like streaming, interactive queries and machine learning.
# Running CTAS queries using Athena will be faster because you will not have to consider optimizing the resources(as Athena is a managed service) that will be needed based on the size of your table, however, running CTAS over spark-sql will not have the limitation of 100 partitions.
