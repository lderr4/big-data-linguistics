from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from constants import POSTGRES_DB,POSTGRES_HOSTNAME,POSTGRES_PASSWORD,POSTGRES_PORT,POSTGRES_USER, dates
import pandas as pd
from datetime import datetime
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
        response = {}
        words = word.split(",")
        for word in words:
            if not len(word):
                continue
            word = word.strip()
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
            response[word] = {"df": df.to_dict(orient="records"),"definition":str(df_definition.definition.iloc[0]), "total_count": int(total_count), "avg_avg_sentiment": float(avg_avg_sentiment)}

    except Exception as e:
        logging.error(f"exception with read_sql: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    return response


@app.get("/get-yearly-words")
def get_year_stats(year: int):

    year_words = text("""
    select 
        n.year, 
                    
        n.word as neg_word, 
        n.sentiment_spike_year as negative_sentiment_spike, 
                    
        p.word as pos_word, 
        p.sentiment_spike_year as positive_sentiment_spike,
                    
        tw.word as top_spike,
        tw.frequency_spike as top_frequency_spike,
                    
        tsy.word as top_sentiment_word,
        tsy.avg_sentiment as top_sentiment,
                    
        bsy.word as bottom_sentiment_word,
        bsy.avg_sentiment as bottom_sentiment,
        
        bwy.word as bottom_spike,
        bwy.frequency_spike as bottom_frequency_spike,
                    
        n.rank as rank
    from reddit_linguistics.negative_sentiment_spikes_year n
    join reddit_linguistics.positive_sentiment_spikes_year p on p.year = n.year and p.rank = n.rank
    join reddit_linguistics.top_words_by_year_spike tw on tw.year=n.year and tw.rank = n.rank
    join reddit_linguistics.top_sentiment_year tsy on tsy.year=n.year and tsy.rank=n.rank
    join reddit_linguistics.bottom_sentiment_year bsy on bsy.year=n.year and bsy.rank=n.rank
    join reddit_linguistics.bottom_words_by_year_spike bwy on bwy.year=n.year and bwy.rank=n.rank
    WHERE EXTRACT(YEAR FROM n."year") = :year
    """)
    try:
        df = pd.read_sql(year_words, engine, params={"year": year})
        response = {"df": df.to_dict(orient="records")}
        return response
    except Exception as e:
            logging.error(f"exception with read_sql: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    

