from pyspark.sql import SparkSession
import boto3
import os
import logging
logging.basicConfig(level=logging.INFO)

def read_and_write_to_parquet(spark, s3_in_path, s3_out_path):
    
    df = spark.read.load(s3_in_path, format="json")
    df.select("score", "ups", "downs", "subreddit")
    df.write \
        .mode("overwrite") \
        .parquet(s3_out_path, compression="snappy")
    
    
def get_s3_client():
    return boto3.client("s3")  

def get_spark_session():
	spark = SparkSession.builder \
		.appName("bz2-to-parquet") \
		.config("spark.executor.instances", "14") \
		.config("spark.executor.cores", "4") \
		.config("spark.executor.memory", "8G") \
		.config("spark.driver.memory", "4G") \
		.config("spark.default.parallelism", "224") \
		.config("spark.sql.shuffle.partitions", "224") \
		.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
		.config("spark.io.compression.codec", "snappy") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
		.getOrCreate()
    
	return spark

def main():
    logging.info("ATTENTION: STARTING DRIVER")
    bucket_name = "reddit-comments-444449"
    in_bucket_prefix = "raw/"

    out_bucket_prefix = "raw-parquet"


    logging.info("Setting up Spark Session...")
    spark = get_spark_session()


    logging.info("Setting up S3 Client...")
    s3 = get_s3_client()



    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=in_bucket_prefix, Delimiter='/')
    folders = [content['Prefix'] for content in response.get('CommonPrefixes', [])]
    
    for folder in folders:

        paginator = s3.get_paginator('list_objects_v2')

        year = int(folder[4:8])
        logging.info(f"Moving to {year} directory...")

        
        for page in paginator.paginate(Bucket=bucket_name, Prefix=folder):
            for obj in page.get('Contents', []):

                filename = obj['Key']
                month = filename[-6:-4]

                logging.info(f"Processing {filename}...")

                s3_in_path = f"s3a://{bucket_name}/{filename}"
                s3_out_path = f"s3a://{bucket_name}/{out_bucket_prefix}/year={year}/month={month}/comments.parquet"

                print(f"--------- in: {s3_in_path} ; out: {s3_out_path}")

                read_and_write_to_parquet(spark, s3_in_path, s3_out_path)
if __name__ == "__main__":
     main()
            
            


            