CREATE TABLE reddit_linguistics.negative_sentiment_spikes_year as
with base AS (
    SELECT 
    	w.word, 
    	date_trunc('year', year_month) as year, 
    	sum(count) as yearly_count, 
    	avg(avg_sentiment) as avg_sentiment
    	
    from reddit_linguistics.words w 
    group by w.word, year

),
year_by_year_sentiment as (
	select 
		word, 
		"year",
		yearly_count,
		avg_sentiment,
		avg_sentiment - lag(avg_sentiment) over(partition by word order by year) +
		avg_sentiment - lead(avg_sentiment) over(partition by word order by year) as sentiment_spike_year
		
	from base
	where yearly_count > 100000
),
ranked_words AS (
    SELECT 
        word,
        year,
        yearly_count,
        sentiment_spike_year,
        RANK() OVER (PARTITION BY year ORDER BY sentiment_spike_year ASC) AS "rank"
    FROM year_by_year_sentiment
    WHERE sentiment_spike_year IS NOT null
) 
SELECT *
FROM ranked_words
WHERE rank <= 100
ORDER BY year ASC, rank asc;