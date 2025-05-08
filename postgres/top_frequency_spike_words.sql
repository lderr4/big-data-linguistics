CREATE TABLE reddit_linguistics.top_words_by_year_spike AS
with base AS (
    SELECT w.word, date_trunc('year', year_month) as year, sum(count) as yearly_count, avg(frequency) as avg_frequency
    from reddit_linguistics.words w 
    group by w.word, year

),
year_by_year_frequency as (
	select 
		word, 
		year,
		yearly_count,
		avg_frequency,
		avg_frequency - lag(avg_frequency) over(partition by word order by year)
		+ avg_frequency - lead(avg_frequency) over(partition by word order by year) as frequency_spike
	from base
),
ranked_words AS (
    SELECT 
        word,
        year,
        yearly_count,
        avg_frequency,
        frequency_spike,
        RANK() OVER (PARTITION BY year ORDER BY frequency_spike DESC) AS rank
    FROM year_by_year_frequency
    WHERE frequency_spike IS NOT NULL
) 
SELECT *
FROM ranked_words
WHERE rank <= 100
ORDER BY year ASC, rank asc;