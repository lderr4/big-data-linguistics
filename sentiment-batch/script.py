# nltk, boto3 , pandas , pyarrow
# !pip install nltk boto3 pandas pyarrow 
import nltk
nltk.data.path.append('/tmp/nltk_data')  
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
        col, 
        split, 
        regexp_replace, 
        lower, 
        size, 
        explode, 
        avg, 
        stddev, 
        pandas_udf,
        )
    
from pyspark.sql.types import ArrayType, StringType
from pandas import read_csv
import os
import argparse
import logging
logging.basicConfig(level=logging.INFO)


vader = SentimentIntensityAnalyzer()
bucket_name = "reddit-comments-444449"
bucket_prefix = "raw/"
output_dir = "sentiment/"



def clean_text(df):
    df = df.select("body") # only care about the text body
    df = df.withColumn("words", split(regexp_replace(lower(col("body")), "[^\w\s]", ""), "\s+"))
    return df

def match_word_bank_udf(spark, df, words):
    
    words = set(words)
    broadcast_words = spark.sparkContext.broadcast(words)
    
    @pandas_udf(ArrayType(StringType()))
    def matched_words(words_series):
        return words_series.apply(
            lambda body: [word for word in body if word in broadcast_words.value]
        )
    
    df = df.withColumn("matched", matched_words(col("words")))
    df = df.filter(size(col("matched")) > 0)
    
    return df


def get_sentiment_df(df):

    @pandas_udf('double')
    def vader_sentiment(body_series):
        return body_series.apply(lambda b: vader.polarity_scores(b)["compound"])
    
    df = df.withColumn("sentiment", vader_sentiment(col("body")))
    
    df = df \
        .select(explode(col("matched")).alias("word"), 
        col("sentiment"))
    
    df = df.repartition("word") # repartition before group by

    df = df.groupBy("word").agg(
        avg("sentiment").alias("avg_sentiment"),
        stddev("sentiment").alias("stddev_sentiment"),
    )
                
    return df
    
    
def get_s3_client():
    return boto3.client("s3")

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
    

# done
def get_slang_words(spark, path="s3://reddit-comments-444449/urbandict-word-defs.csv"):
    try:
        df = spark.read.csv(path, header=True, inferSchema=True, mode="PERMISSIVE")
        
        slang_words = (
            df.withColumn("word_count", size(split(col("word"), " ")))
            .filter(col("word_count") == 1)
            .select(lower(col("word")).alias("word"))
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        
        return set(slang_words)
    
    except Exception as error:
        logging.error(f"Error loading slang word dataset: {error}.")
        raise error

    
def main():
    parser = argparse.ArgumentParser(description='Process Reddit comment data')
    parser.add_argument('--start-year', type=int, required=True, help='Start year (e.g., 2007)')
    parser.add_argument('--end-year', type=int, required=True, help='End year (e.g., 2015)')

    args = parser.parse_args()
    start_year = int(args.start_year)
    end_year = int(args.end_year)
    
    
    logging.info("Setting up Spark Session...")
    spark = get_spark_session()
    
    
    logging.info("Loading Slang Dataset...")
    words = get_slang_words(spark)
    
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
            
                df = clean_text(df)

                df = match_word_bank_udf(spark, df, words)

                df = get_sentiment_df(df)
                
                s3_out_path = f"s3a://{bucket_name}/{output_dir}year={year}/month={month}/sentiment.parquet"
                write_df_to_s3(df, s3_out_path)
                logging.info(f"Wrote new file to {s3_out_path}")
        
if __name__ == "__main__":
    main()