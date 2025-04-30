-- This is a script to make sure every year_month and every word have a row
-- Previously, words that had a count of 0 were missing.
-- this script fills them in and gives the row
INSERT INTO all_months (month)
VALUES 
  ('2010-01-01'), ('2010-02-01'), ('2010-03-01'), ('2010-04-01'),
  ('2010-05-01'), ('2010-06-01'), ('2010-07-01'), ('2010-08-01'),
  ('2010-09-01'), ('2010-10-01'), ('2010-11-01'), ('2010-12-01'),
  ('2011-01-01'), ('2011-02-01'), ('2011-03-01'), ('2011-04-01'),
  ('2011-05-01'), ('2011-06-01'), ('2011-07-01'), ('2011-08-01'),
  ('2011-09-01'), ('2011-10-01'), ('2011-11-01'), ('2011-12-01'),
  ('2012-01-01'), ('2012-02-01'), ('2012-03-01'), ('2012-04-01'),
  ('2012-05-01'), ('2012-06-01'), ('2012-07-01'), ('2012-08-01'),
  ('2012-09-01'), ('2012-10-01'), ('2012-11-01'), ('2012-12-01'),
  ('2013-01-01'), ('2013-02-01'), ('2013-03-01'), ('2013-04-01');

-- 2. Create a temporary table of all unique words
CREATE TEMP TABLE all_words (word TEXT);
INSERT INTO all_words (word)
SELECT DISTINCT word FROM reddit_linguistics.words;

INSERT INTO reddit_linguistics.words (word, year_month, count, frequency, avg_sentiment, stddev_sentiment)
SELECT 
  w.word,
  m.month,
  0 AS count,
  0.0 AS frequency,
  0.0 AS avg_sentiment,
  0.0 AS stddev_sentiment
FROM all_words w
CROSS JOIN all_months m
LEFT JOIN reddit_linguistics.words rlw ON rlw.word = w.word AND rlw.year_month = m.month
WHERE rlw.word IS NULL;
