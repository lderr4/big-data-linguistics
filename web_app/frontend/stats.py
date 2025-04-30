import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from constants import API_URL

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

    # Add more plots/tables here as needed
