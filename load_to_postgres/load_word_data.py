import pandas as pd
import pyarrow.parquet as pq
from s3fs import S3FileSystem
import boto3
import sqlalchemy
import os
import gc
import logging
logging.basicConfig(
    level=logging.INFO,                # Show INFO and above
    format="%(asctime)s [%(levelname)s] %(message)s",  # Add timestamps
)
BATCH_SIZE = 100_000
AWS_REGION = "us-east-1"

def remove_stopwords(df, stopwords):
    df["valid_word"] = df["word"].apply(lambda word: not (word in stopwords))
    df = df[df["valid_word"]]
    df = df.drop('valid_word', axis=1)
    return df

def get_stopwords(s3):
    bucket_name = 'reddit-comments-444449'
    key = 'stopwords.txt'

    response = s3.get_object(Bucket=bucket_name, Key=key)

    content = response['Body'].read().decode('utf-8')

    stopwords = set(content.strip().split('\n'))
    
    return stopwords
    
def get_s3_client():
    return boto3.client('s3')

def get_postgres_engine():
    
    engine = sqlalchemy.create_engine(f'postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}@{os.environ["POSTGRES_HOSTNAME"]}:{os.environ["POSTGRES_PORT"]}/{os.environ["POSTGRES_DB"]}')
    return engine

def get_row_count(engine, table_name="reddit_linguistics.words"):

    with engine.connect() as connection:
        query = sqlalchemy.text(f"SELECT COUNT(*) FROM {table_name}")
        result = connection.execute(query)
        count = result.scalar()  # fetches the single value
    return count


def main():

    start_year = int(os.environ["START_YEAR"])
    start_month = int(os.environ["START_MONTH"])
    end_year = int(os.environ["END_YEAR"])
    
    
    logging.info("Setting up S3 Client...")
    s3 = get_s3_client()

    logging.info("Retrieving Stop Words...")
    stopwords = get_stopwords(s3)

    logging.info("Getting SQL Engine")
    engine = get_postgres_engine()
    
    s3_s3fs = S3FileSystem(
        key=os.getenv('AWS_ACCESS_KEY_ID'),
        secret=os.getenv('AWS_SECRET_ACCESS_KEY'))  # e.g., "us-east-1"

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            
            if year == 2007 and month < 10:
                continue
            if year == 2015 and month > 5:
                break

            if len(str(month)) == 1: # 1 --> 01; 2 --> 02, etc
                month = "0" + str(month)

            logging.info(f"Processing year={year} and month={month}...")
            word_freq_path = f"s3://reddit-comments-444449/word_freqs/year={year}/month={month}/1_gram_counts.parquet/"
            sentiment_path = f"s3://reddit-comments-444449/sentiment/year={year}/month={month}/sentiment.parquet"

            sentiment = pd.read_parquet(sentiment_path , engine="pyarrow")
            sentiment = remove_stopwords(sentiment, stopwords)



            all_files = [
                f for f in s3_s3fs.ls(word_freq_path) if f.endswith('.parquet')
            ]
        
            for file_path in sorted(all_files): 
                
                # Open Parquet file with streaming
                parquet_file = pq.ParquetFile(file_path, filesystem=s3_s3fs)
                
                # Process in batches
                for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
                    batch = batch.to_pandas()

                    df = pd.merge(batch, sentiment, on='word', how='inner')

                    df["year_month"] = f"{year}-{month}-01"
                    try:
                        df.to_sql('words', con=engine, schema='reddit_linguistics', if_exists='append', index=False)
                        logging.info(f"Inserted {len(df)} rows into words")
                    except Exception as error:
                        logging.error(f"Error inserting: {error}.")
            del df
            del sentiment
            gc.collect()
            
if __name__ == "__main__":
    main()