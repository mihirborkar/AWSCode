import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Reading the Tables from Glue Catalog as Dynamic Frames
ds_document = glueContext.create_dynamic_frame.from_catalog(database = "a205451-onvio-us", table_name = "bm_akkadia_public_v_file_metadata_parse_json", transformation_ctx = "ds_document")
am_company = glueContext.create_dynamic_frame.from_catalog(database = "a205451-onvio-us", table_name = "prod_us_company", transformation_ctx = "ds_document")

#Converting the Dynamic Frames to Data Frames
ds_document_dataframe= ds_document.toDF()
am_company_dataframe= am_company.toDF()

## @type: ApplyMapping
## @args: [mapping = [("accountid", "string", "file_metadata_accountid", "string"), ("file_type", "string", "file_type", "string"), ("source_type", "string", "source_type", "string"), ("file_extension", "string", "file_extension", "string"), ("create_date", "timestamp", "file_metadata_create_date", "timestamp"), ("parent_type", "string", "parent_type", "string"), ("file_size", "string", "file_size", "string"), ("fileid", "string", "file_metadata_fileid", "string"), ("status", "string", "file_metadata_status", "string"), ("file_category", "string", "file_category", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
##am_document = ApplyMapping.apply(frame = ds_document, mappings = [("accountid", "string", "file_metadata_accountid", "string"), ("file_type", "string", "file_type", "string"), ("source_type", "string", "source_type", "string")
## , ("file_extension", "string", "file_extension", "string"), ("create_date", "timestamp", "file_metadata_create_date", "timestamp"), ("parent_type", "string", "parent_type", "string"), ("file_size", "string", "file_size", "string")
##  , ("fileid", "string", "file_metadata_fileid", "string"), ("status", "string", "file_metadata_status", "string"), ("file_category", "string", "file_category", "string")], transformation_ctx = "am_document")
##am_company = ApplyMapping.apply(frame = ds_company, mappings = [("owner__contact_id", "string", "owner__contact_id", "string"), ("company", "string", "company", "string"), ("company_id", "string", "company_id", "string")
##, ("test_firm", "boolean", "test_firm", "boolean")], transformation_ctx = "am_company")

# Column Renaming, Changed from Apply Mappings to DataFrame method of "withColumnRenamed"
am_document = ds_document_dataframe.withColumnRenamed ('accountid','file_metadata_accountid').withColumnRenamed ('create_date','file_metadata_create_date').withColumnRenamed ('fileid','file_metadata_fileid').withColumnRenamed ('status','file_metadata_status')
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
##rc_document = ResolveChoice.apply(frame = am_document, choice = "make_cols", transformation_ctx = "rc_document")
##rc_company = ResolveChoice.apply(frame = am_company, choice = "make_cols", transformation_ctx = "rc_company")
#join and drop fields after they have been used for filtering
#join_document = Join.apply(rc_company,rc_document, 'company_id','file_metadata_accountid' ).drop_fields(['file_metadata_accountid'])

#join, drop fields after they have been used for filtering and drop Null Fields (i.e. drop rows if all fields are NULL) 
join_document= am_company_dataframe.join(ds_document_dataframe, ['company_id','file_metadata_accountid']).drop('file_metadata_accountid').na.drop("all")

print ("join_document Count: ", join_document.count())
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
#dn_document = DropNullFields.apply(frame = join_document, transformation_ctx = "dn_document")
#convert to dataframe
#df_document = dn_document.toDF()
## @type: DataSink
## @args: [catalog_connection = "PRODOnvioGluepg-destination", connection_options = {"dbtable": "bm_akkadia_public_v_file_metadata_parse_json", "database": "public"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
#dw_document = join_document.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").csv("s3://a205451-onvio-glue-prod-us-east-1/Test POC/")

dw_document = join_document.write.mode("overwrite").parquet("s3://a205451-onvio-glue-prod-us-east-1/Test POC/")
job.commit()