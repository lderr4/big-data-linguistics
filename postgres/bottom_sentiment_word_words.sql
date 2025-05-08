CREATE TABLE reddit_linguistics.bottom_sentiment_year as
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
		avg_sentiment
	from base
	where yearly_count > 100000
	order by avg_sentiment desc
),
ranked_words AS (
    SELECT 
        word,
        year,
        avg_sentiment,
        RANK() OVER (PARTITION BY year ORDER BY avg_sentiment ASC) AS "rank"
    FROM year_by_year_sentiment
    WHERE avg_sentiment IS NOT null
) 
select * 
from ranked_words
where rank <= 100