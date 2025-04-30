from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, regexp_replace
from pyspark.ml.feature import NGram
import boto3
import logging
import os
import argparse

logging.basicConfig(level=logging.INFO)

app_name="reddit-comments-444449-batch"
bucket_name = "reddit-comments-444449"
bucket_prefix = "raw/"
output_dir = "word_freqs/"
n_gram_nums = [1]


def get_spark_session():
    try:
        spark = SparkSession.builder \
            .config("spark.executor.memory", "7g") \
            .config("spark.executor.cores", "4") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "160") \
            .config("spark.default.parallelism", "160") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.speculation", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .getOrCreate()
    
    except Exception as error:
        logging.error("Error setting up Spark session")
        raise error
    return spark



def get_s3_client():
    try:
        s3 = boto3.client('s3')
    except Exception as error:
        logging.error(f"Failed to establish S3 client. Make sure your credentials are configured correctly.")
        raise error
    return s3

def load_file_to_spark_df(spark, s3_path):
    try:
        df = spark.read.load(s3_path, format="json")
        return df
    except Exception as error:
        logging.error(f"Failed to load {s3_path} to SparkSession.")
        raise error

def write_df_to_s3(df, s3_path):
    try:
        df.write.mode("overwrite").parquet(s3_path)
    except Exception as error:
        logging.error(f"Error writing DataFrame to {s3_path} as a Parquet file.")
        raise error



def clean_punctuation(df):
    df = df.withColumn("words", split(regexp_replace(lower(col("body")), "[^\w\s]", ""), "\s+"))
    return df
    

def get_ngram_counter(df, n):
    
    if n > 1:
        ngram = NGram(n=n, inputCol="words", outputCol="bigrams")
        df_ngrams = ngram.transform(df).select("bigrams")
        ngrams_df = df_ngrams.select(explode(col("bigrams")).alias("word"))

    elif n == 1: 
        ngrams_df = df.select(explode(col("words")).alias("word"))
        
        
    ngram_counts = ngrams_df.groupBy("word").count().orderBy(col("count").desc())

    total_ngrams = ngram_counts.agg({"count": "sum"}).collect()[0][0]

    ngram_counts = ngram_counts.withColumn("frequency", col("count") / total_ngrams)

    return ngram_counts


def __main__():
    
    parser = argparse.ArgumentParser(description='Process Reddit comment data')
    parser.add_argument('--start-year', type=int, required=True, help='Start year (e.g., 2007)')
    parser.add_argument('--end-year', type=int, required=True, help='End year (e.g., 2015)')

    args = parser.parse_args()
    start_year = int(args.start_year)
    end_year = int(args.end_year)
    
    logging.info("Setting up SparkSession...")
    spark = get_spark_session()
    
    logging.info("Setting up S3 Client...")
    s3 = get_s3_client()
    
    
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=bucket_prefix, Delimiter='/')
    folders = [content['Prefix'] for content in response.get('CommonPrefixes', [])]
       
    
    for folder in folders:

        paginator = s3.get_paginator('list_objects_v2')
        
        
        year = int(folder[4:8])
        logging.info(f"Moving to {year} directory...")
        
        if year < start_year or year > end_year:
            continue

        for page in paginator.paginate(Bucket=bucket_name, Prefix=folder):
            for obj in page.get('Contents', []):
                
                filename = obj['Key']
                month = filename[-6:-4]
                
                
                logging.info(f"Processing {filename}...")
                
                s3_file_path = f"s3a://{bucket_name}/{filename}"
                df = load_file_to_spark_df(spark, s3_file_path)
                   
                
                
                df = clean_punctuation(df)
                
                 
                
                for n_gram_num in n_gram_nums:
                    
                    n_gram_counter = get_ngram_counter(df, n_gram_num)
                    
                    s3_out_path = f"s3a://{bucket_name}/{output_dir}year={year}/month={month}/{n_gram_num}_gram_counts.parquet"
                    write_df_to_s3(n_gram_counter, s3_out_path)
                    
                
    
if __name__ == "__main__":
    __main__()