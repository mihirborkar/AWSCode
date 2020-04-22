import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def CreatePartition(bucket,table,database,partition_key):

	#S3PartitionTable1 = str('s3://'+bucket+'/'+table+'/'+"date="+a+'/')
    S3PartitionTable1 = str('s3://'+bucket+'/'+table+'/')
	datasource0 = glueContext.create_dynamic_frame.from_catalog(database = database, table_name = table, transformation_ctx = "datasource0")
	df1 = datasource0.toDF()
	df1.write.mode("Overwrite").partitionBy(partition_key).parquet(S3PartitionTable1)

CreatePartition("dwhs-sparketl-poc","jdbctest_hrdw_dbo_dimcompany","lakeformation_tutorial","companycode")
CreatePartition("dwhs-sparketl-poc","dwhs_hrdwstaging_dbo_monthly_manual_div_chg_lkup","hrdwstaging","year_month")

S3TargetForCrawler = str('s3://dwhs-sparketl-poc/')
client = boto3.client('glue', region_name='us-west-2')

# update_crawler is Optional
response = client.update_crawler(
    Name='SuperCrawler',
    Targets={
        'S3Targets': [
            {
                'Path': S3TargetForCrawler
            },
        ]
    }
)

response2 = client.start_crawler(
    Name='SuperCrawler'
)


job.commit()

# Scope of automation: Load individual S3PartitionTable1 into another variable 'a' and then read from the variable 'a' to update the crawler path

