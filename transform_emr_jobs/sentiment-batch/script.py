# nltk, boto3 , pandas , pyarrow
# !pip install nltk boto3 pandas pyarrow 
import nltk
nltk.data.path.append('/tmp/nltk_data')  
from nltk.sentiment.vader import SentimentIntensityAnalyzer
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
input_dir = "raw-parquet"
output_dir = "sentiment"

def sample_df(df, sample_size=5_000_000):
    # I'm not convinced that there is a benefit beyond calculating
    # sentiment for more than 5 million comments per month
    # so in the interest of time (and $), I'm only going to sample down the dataset
    total_rows = df.count()
    fraction = sample_size / total_rows

    if fraction < 1:
        df = df.sample(withReplacement=False, fraction=fraction)
    return df

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
    
    


def get_spark_session():
    try:
        spark = SparkSession.getOrCreate()
    except Exception as error:
        logging.error("Error setting up Spark session")
        raise error
    return spark
 
def load_file_to_spark_df(spark, s3_path):
    try:
        df = spark.read.parquet(s3_path)
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
    


def get_slang_words(spark, 
                    slang_path="s3://reddit-comments-444449/urbandict-word-defs.csv",
                    stop_words_path="s3://reddit-comments-444449/stopwords.txt"):
    try:
        df = spark.read.csv(slang_path ,header=True, inferSchema=True) #mode="PERMISSIVE")
        
        slang_words = (
            df.withColumn("word_count", size(split(col("word"), " ")))
            .filter(col("word_count") == 1)
            .select(lower(col("word")).alias("word"))
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        slang_words = set(slang_words)
        stopwords = spark.read.text(stop_words_path)

        stopwords = stopwords.withColumnRenamed("value", "word")
        stopwords = stopwords.select(lower(col("word")).alias("word")).rdd.flatMap(lambda x: x)
        stopwords = set(stopwords.collect())
        
        slang_words = slang_words - stopwords
        
        
        return slang_words
    
    except Exception as error:
        logging.error(f"Error loading slang word dataset: {error}.")
        raise error

    
def main():
    parser = argparse.ArgumentParser(description='Process Reddit comment data')
    parser.add_argument('--start-year', type=str, required=True, help='Start year (e.g., 2007)')
    parser.add_argument('--end-year', type=int, required=True, help='End year (e.g., 2015)')

    args = parser.parse_args()
    start_year, start_month = tuple(str(args.start_year).split("."))
    end_year = args.end_year
    
    start_year, start_month =  int(start_year), int(start_month)
    
    logging.info("Setting up Spark Session...")
    spark = get_spark_session()
    
    
    logging.info("Loading Slang Dataset...")
    words = get_slang_words(spark)
    
    for year in range(start_year, end_year+1):
        for month in range(1,13):
            if year == start_year and month < start_month:
                continue

            if len(str(month)) == 1:
                month = "0" + str(month)

            s3_in_path = f"s3a://{bucket_name}/{input_dir}/year={year}/month={month}/comments.parquet"
            s3_out_path = f"s3a://{bucket_name}/{output_dir}/year={year}/month={month}/sentiment.parquet"

            logging.info(f"Processing {s3_in_path}")

            
            df = load_file_to_spark_df(spark, s3_in_path)
            df = sample_df(df)
            df = clean_text(df)

            df = match_word_bank_udf(spark, df, words)

            df = get_sentiment_df(df)
            
            write_df_to_s3(df, s3_out_path)
            logging.info(f"Wrote new file to {s3_out_path}")
        
if __name__ == "__main__":
    main()