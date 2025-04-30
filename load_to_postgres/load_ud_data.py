import pandas as pd
import sqlalchemy
import os
import logging

logging.basicConfig(
    level=logging.INFO,                # Show INFO and above
    format="%(asctime)s [%(levelname)s] %(message)s",  # Add timestamps
)
S3_URI = "s3://reddit-comments-444449/urbandict-word-defs.csv"

def get_postgres_engine():
    engine = sqlalchemy.create_engine(f'postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}@{os.environ["POSTGRES_HOSTNAME"]}:{os.environ["POSTGRES_PORT"]}/{os.environ["POSTGRES_DB"]}')
    return engine

def main():


    logging.info("Getting SQL Engine")
    engine = get_postgres_engine()
    

    logging.info("Reading CSV from S3...")
    df = pd.read_csv(S3_URI , on_bad_lines="skip")


    logging.info("Cleaning DataFrame...")
    df = (
    df[df['word'].str.split().str.len() == 1]       
      .assign(word=lambda x: x['word'].str.lower())                                              
)
    df = df.sort_values('up_votes', ascending=False).drop_duplicates(subset='word', keep='first')
    df = df[['word', 'definition']]

    logging.info("Writing to Postgres...")
    df.to_sql(
        'word_definitions',
        con=engine,
        schema='reddit_linguistics',
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=10000)

    
if __name__ == "__main__":
    main()