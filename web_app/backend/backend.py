from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from constants import POSTGRES_DB,POSTGRES_HOSTNAME,POSTGRES_PASSWORD,POSTGRES_PORT,POSTGRES_USER, dates
import pandas as pd
import plotly.express as px
from typing import List
import logging
logging.basicConfig(
    level=logging.INFO,                # Show INFO and above
    format="%(asctime)s [%(levelname)s] %(message)s",  # Add timestamps
)
app = FastAPI()

database_url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOSTNAME}:{POSTGRES_PORT}/{POSTGRES_DB}'
engine = create_engine(database_url)
logging.info(f"DB URL: {database_url}; DB ENGINE: {engine}")

# Allow CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

fill_in_df = pd.DataFrame({
    'year_month': pd.to_datetime(dates),  # Convert to datetime type
    'count': 0,                     # Default integer 0
    'frequency': 0.0,               # Default float 0.0
    'avg_sentiment': 0.0,           # Default float 0.0
    'stddev_sentiment': 0.0         # Default float 0.0
})

@app.get("/word-data")
def get_word_data(word: str):

    word_analytics_query = text("""
        SELECT word, count, frequency, avg_sentiment, stddev_sentiment, year_month
        FROM reddit_linguistics.words
        WHERE word = :word
        ORDER BY year_month ASC
    """)
    word_definition_query = text("""
        SELECT word, definition
        FROM reddit_linguistics.word_definitions
        WHERE word = :word
    """)
    try:
        df = pd.read_sql(word_analytics_query, engine, params={"word": word})
        df_definition = pd.read_sql(word_definition_query, engine, params={"word": word})
        if df.empty or df_definition.empty:
            raise HTTPException(status_code=404, detail="Word not found")
        
        df['stddev_sentiment'] = df['stddev_sentiment'].fillna(0.0)
        df['year_month'] = pd.to_datetime(df['year_month'])  # Ensure year_month is datetime

        missing_months_df = fill_in_df[~fill_in_df['year_month'].isin(df['year_month'])]
        missing_months_df['word'] = word

        df = pd.concat([df, missing_months_df], ignore_index=True)

        df.sort_values(by='year_month', inplace=True)

        df = df.astype({
            'count': 'int',
            'frequency': 'float',
            'avg_sentiment': 'float',
            'stddev_sentiment': 'float',
        })

        avg_avg_sentiment = df['avg_sentiment'].mean()
        total_count = df['count'].sum()

        return {"df": df.to_dict(orient="records"),"definition":str(df_definition.definition.iloc[0]), "total_count": int(total_count), "avg_avg_sentiment": float(avg_avg_sentiment)}
    except Exception as e:
        logging.error(f"exception with read_sql: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    

    

