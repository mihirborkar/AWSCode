#using multithreading in pyspark. Please kindly edit the code as per your usecase.

from pyspark.sql import SparkSession
import os
import boto3
from multiprocessing.pool import ThreadPool
import logging


spark = SparkSession.builder.appName("RobertThreadpool150").getOrCreate()

def preprocess(dirs):
    path = "s3://pycharmexmple/" + dirs + "/*.json"
    path1 = "s3://pycharmexmple/output/" + dirs
    df = spark.read.option("multiline","true").json(path)
    df.coalesce(1).write.save(path1)

if __name__ == "__main__":

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('pycharmexmple')
    bucket_files = [x.key for x in bucket.objects.all()]
    txts = list({"/".join(file.split("/")[:-1]) for file in bucket_files if "json" in file.split("/")[-1]})
    print(txts)
    pool = ThreadPool(150)
    pool.map(preprocess, txts)
    pool.close()
    pool.join()
    spark.stop()
