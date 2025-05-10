import streamlit as st
import pandas as pd
import plotly.express as px
import altair as alt
from constants import API_URL, BAD_WORDS, WORD_COLS
import requests
import logging

logging.basicConfig(
    level=logging.INFO,                                # Show INFO and above
    format="%(asctime)s [%(levelname)s] %(message)s",  # Add timestamps
)

st.title("Reddit Linguistics Explorer")
st.set_page_config(page_title="Reddit Linguistics Explorer")
page = st.selectbox("Select a page", ["Word Explorer", "Yearly Word Trends", "About"])


if page == "Word Explorer":
    def get_concise_definition(definition):

        split = definition.split(".")
        if len(split[0]) > 3:
            return split[0]
        else:
            return split[0] + split[1]


    def load_word_data(words):
        response = requests.get(f"{API_URL}/word-data", params={"word": words})

        if response.status_code == 200:
            
            data = response.json()
            rows = []
            word_summaries = {}

            for word, word_data in data.items():
                word_df = pd.DataFrame(word_data["df"])
                word_df["word"] = word  # Add column to distinguish in combined DataFrame
                rows.append(word_df)

                word_summaries[word] = {
                    "total_count": word_data["total_count"],
                    "avg_avg_sentiment": word_data["avg_avg_sentiment"],
                    "definition": word_data["definition"],
                }

            combined_df = pd.concat(rows, ignore_index=True)
            return combined_df, word_summaries
        else:
            logging.error(f"API error: {response.status_code}, {response.text}")
            return pd.DataFrame(), {}  # Safe fallback


    st.header("🔍 Word Explorer")
    search_word = st.text_input("Enter a word or a comma separated list of words to explore:").lower()

    if search_word:
        word_list = [w.strip() for w in search_word.split(",") if w.strip()]
        df, summaries = load_word_data(",".join(word_list))

        if df.empty:
            st.warning("No data found.")
        else:
            for word in word_list:


                if word in summaries:
                    summary = summaries[word]

                    definition = get_concise_definition(summary["definition"])  

                    st.subheader(f"Word: '{word}'")
                    st.caption(f'Definition: *"{definition}."*')
                    col1, col2 = st.columns([1, 1])
                    
                    with col1:
                        st.metric(label="Average Sentiment", value=f"{summary['avg_avg_sentiment']:,.2f}")
                    with col2:
                        st.metric(label="Total Count", value=f"{summary['total_count']:,}")
                else:
                    st.warning(f"No data for '{word}'")

            fig_freq = px.line(
                df,
                x='year_month',
                y='frequency',
                color='word',
                title='Monthly Word Frequency',
                labels={
                    'year_month': 'Year-Month',
                    'frequency': 'Frequency',
                    'word': 'Word'
                }
            )
            fig_freq.update_layout(xaxis_title='Date', yaxis_title='Word Frequency')
            st.plotly_chart(fig_freq, use_container_width=True)

            # Sentiment Chart
            fig_sentiment = px.line(
                df,
                x='year_month',
                y='avg_sentiment',
                color='word',
                title='Monthly Average Sentiment of Comments Including Word',
                labels={
                    'year_month': 'Year-Month',
                    'avg_sentiment': 'Average Sentiment',
                    'word': 'Word'
                }
            )
            fig_sentiment.update_layout(xaxis_title='Date', yaxis_title='Average Sentiment Score')
            st.plotly_chart(fig_sentiment, use_container_width=True)


elif page == "Yearly Word Trends":
    st.header("📊 Interesting Stats")

    year = st.selectbox("Select a year", [i for i in range(2011, 2015)], index=0)

    @st.cache_data
    def get_year_words(year):
        response = requests.get(f"{API_URL}/get-yearly-words", params={"year": year})
        if response.status_code == 200:
            df = pd.DataFrame(response.json()["df"])
            return df  # Should be list of dicts as from your SQL result
        else:
            st.error("Failed to load interesting stats.")
            return []

    def clean_bad_words(df):
        def censor_word(x):
            if x in BAD_WORDS:
                return f"{x[0]}{'*' * (len(x) - 1)}"
            else:
                return x
        
        

        for col in WORD_COLS:
            df[col] = df[col].apply(censor_word)
        return df
    

    if year:
        df = get_year_words(year)
        df = clean_bad_words(df)
        if len(df):
            def plot_top(df, word_col, score_col, title, color, sort_desc=True):
                chart_df = df[[word_col, score_col]].sort_values(by=score_col, ascending=not sort_desc).head(10)

                y_axis = alt.Y(
                    f"{word_col}:N",
                    sort=chart_df[word_col].tolist(),
                    title="Word"
                )

                bars = alt.Chart(chart_df).mark_bar(color=color).encode(
                    x=alt.X(score_col, title="Score"),
                    y=y_axis,
                    tooltip=[word_col, score_col]
                )

                text = alt.Chart(chart_df).mark_text(
                    align='left',
                    baseline='middle',
                    dx=3
                ).encode(
                    x=alt.X(score_col),
                    y=y_axis,
                )

                return (bars + text).properties(title=title, height=500)



            col1, col2, col3 = st.columns(3)

            with col1:
                st.altair_chart(plot_top(df, 'top_spike', 'top_frequency_spike', "Top Frequency Spike Words", "#4e79a7", True), use_container_width=True)
                st.altair_chart(plot_top(df, 'bottom_spike', 'bottom_frequency_spike', "Bottom Frequency Spike Words", "#9c9ca1", False), use_container_width=True)

            with col2:
                st.altair_chart(plot_top(df, 'pos_word', 'positive_sentiment_spike', "Positive Sentiment Spike Words", "#59a14f", True), use_container_width=True)
                st.altair_chart(plot_top(df, 'top_sentiment_word', 'top_sentiment', "Top Sentiment Words Words", "#76b7b2", True), use_container_width=True)

            with col3:
                st.altair_chart(plot_top(df, 'neg_word', 'negative_sentiment_spike', "Negative Sentiment Spike Words", "#e15759", False), use_container_width=True)
                st.altair_chart(plot_top(df, 'bottom_sentiment_word', 'bottom_sentiment', "Bottom Sentiment Words", "#b07aa1", False), use_container_width=True)


        
            with st.expander("ℹ️ What do these charts show?"):
                st.markdown("""
                - **Top Frequency Spike Words**: Words that saw the biggest increase in usage frequency.
                - **Positive Sentiment Spike Words**: Words that saw the biggest increase in sentiment.
                - **Negative Sentiment Spike Words**: Words that saw biggest decrease in sentiment.
                - **Bottom Frequency Spike Words**: Words that saw the biggest decrease in usage frequency.
                - **Top Sentiment Word Words**: Words that had the highest average sentiment score.
                - **Bottom Sentiment Word Words**: Words that had the lowest average sentiment score.        
                
                These metrics are computed by comparing each word’s yearly change to historical trends.
                """)
            
        
        else:
            st.warning(f"⚠️ Error retrieving data for {year}.")
    

elif page == "About":
    st.header("📚 About")
    st.write("""
        This is a web application that analyzes word usage and sentiment scores over time.
        It uses data from over 1 billion Reddit comments spanning from 2007 to 2015. The words 
        and definitions are sourced from [Urban Dictionary](https://www.urbandictionary.com/)

        By tracking how frequently a word appears and how its sentiment evolves, 
        the app provides insight into cultural shifts, slang adoption, and changing 
        public attitudes. Whether you're a linguist, data scientist, or just curious,
        this tool offers an interactive way to explore
        language trends on one of the internet’s largest social platforms.
             
        If you're interested in the code and tech stack, check out the [GitHub repository](https://github.com/lderr4/big-data-linguistics).

        **Connect with me:**  
        Website: [lucasrderr.com](https://www.lucasrderr.com)  <br>
        LinkedIn: [linkedin.com/in/lucas-derr-81455b1aa](https://www.linkedin.com/in/lucas-derr-81455b1aa/)  <br>
        Medium: [medium.com/@lucasrderr](https://medium.com/@lucasrderr)  <br>
        GitHub: [github.com/lderr4](https://github.com/lderr4)  <br>  
        Feel free to reach out for questions, feedback, or collaboration opportunities.  
        Email: lucasrderr@gmail.com
        """, unsafe_allow_html=True)