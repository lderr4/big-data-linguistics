import streamlit as st
import pandas as pd
import plotly.express as px
from constants import API_URL
import requests
import logging
logging.basicConfig(
    level=logging.INFO,                # Show INFO and above
    format="%(asctime)s [%(levelname)s] %(message)s",  # Add timestamps
)

def load_word_data(word):
    
    response = requests.get(f"{API_URL}/word-data", params={"word": word})

    if response.status_code == 200:
        df = pd.DataFrame(response.json()["df"])
        total_count = response.json()["total_count"]
        avg_avg_sentiment = response.json()["avg_avg_sentiment"]
        definition = response.json()["definition"]
        return df, total_count, avg_avg_sentiment, definition
    else:
        logging.error(f"API error: {response.status_code}, {response.text}")
        return pd.DataFrame(), None, None  # Return empty DataFrame to avoid crash


st.title("Slang Analyics")
page = st.selectbox("Select a page", ["Home", "Interesting Stats", "About"])


if page == "Home":
    search_word = st.text_input("Enter a word to explore:").lower()

    if search_word:
        df, total_count,avg_avg_sentiment, definition = load_word_data(search_word)
        if df.empty:
            st.warning(f"No data found for '{search_word}'")
        else:
            st.subheader(f"Word: '{search_word}'")

            st.caption(f'Definition: *"{definition.split(".")[0]}."*')
            col1, col2 = st.columns([1, 1])
            with col1:
                st.metric(label="Average Sentiment", value = f"{avg_avg_sentiment:,.2f}")
            with col2:
                st.metric(label="Total Count", value=f"{total_count:,}")

            fig_freq = px.line(
                df, x='year_month', y='frequency',
                title='Monthly Word Frequency'
            )
            st.plotly_chart(fig_freq, use_container_width=True)

            fig_sentiment = px.line(
                df, x='year_month', y='avg_sentiment',
                title='Average Sentiment Over Time'
            )
            st.plotly_chart(fig_sentiment, use_container_width=True)

elif page == "Interesting Stats":
    st.title("📊 Interesting Stats")

    # Load precomputed stats from your API or local file
    @st.cache_data
    def load_stats():
        response = requests.get(f"{API_URL}/interesting-stats")
        if response.status_code == 200:
            return response.json()  # Assume it's a dict with different stats
        else:
            st.error("Failed to load interesting stats.")
            return {}

    stats = load_stats()

    if stats:
        st.header("Most Frequent Words")
        df_freq = pd.DataFrame(stats["most_frequent_words"])
        st.dataframe(df_freq.head(10))

        fig = px.bar(df_freq.head(10), x="word", y="count", title="Top 10 Most Frequent Words")
        st.plotly_chart(fig, use_container_width=True)

        st.header("Most Positive Words")
        df_pos = pd.DataFrame(stats["most_positive_words"])
        st.dataframe(df_pos.head(10))

        fig2 = px.bar(df_pos.head(10), x="word", y="avg_sentiment", title="Top 10 Positive Words")
        st.plotly_chart(fig2, use_container_width=True)

elif page == "About":
    st.header("About")
    st.write("""
        This is a web application for analyzing slang words and their usage over time.
        You can search for a specific word to see its frequency and sentiment analysis.
        The app also provides interesting statistics about the most frequent and positive words.
    """)