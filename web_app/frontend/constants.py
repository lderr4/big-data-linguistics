import os
API_URL = os.environ["API_URL"]

BAD_WORDS = []
with open('/opt/bad_words.txt', 'r') as file:
    BAD_WORDS = [word.strip().lower() for word in file.readlines()]

BAD_WORDS.append("rape")
BAD_WORDS.append("rapes")
BAD_WORDS.append("raped")
BAD_WORDS.append("raping")
BAD_WORDS.append("rapist")
BAD_WORDS.append("fagt")


BAD_WORDS = set(BAD_WORDS)


WORD_COLS = [
            'neg_word',
            'pos_word',
            'top_spike',
            'top_sentiment_word',
            'bottom_sentiment_word',
            'bottom_spike'
        ]