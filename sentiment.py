## Method from https://medium.com/nerd-for-tech/wallstreetbets-sentiment-analysis-on-stock-prices-using-natural-language-processing-ed1e9e109a37

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize, RegexpTokenizer
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2.extras as extras
import nltk
import re

## custom word values
# extra slang taken from https://infinityinvesting.com/wallstreetbets-slang-meaning/
CUSTOM_WORDS = {
    'moon': 4.0,
    'long': 2.0,
    'short': -2.0,
    'call': 4.0,
    'calls': 4.0,    
    'put': -4.0,
    'puts': -4.0,
    'overvalued': -3.0,
    'undervalued': 3.0,
    'bullish': 3.0,
    'bearish': -3.0,
    'bull': 3.0,
    'bear': -3.0,
    'stonk': 3.0,
    'stonks': 3.0,
    'diamond': 2.0,
    'tendie': 3.0,
    'tendies':3.0,
    'HODL': 1.0,
    'Amazon': 0.0,
    'amazon': 0.0,
    'monster': 2.0
}
nltk.download('vader_lexicon')
nltk.download('punkt')
SENT_OBJ  = SentimentIntensityAnalyzer()
SENT_OBJ.lexicon.update(CUSTOM_WORDS)

def sentiment_analysis(sentence):
    sentence = re.sub(r'[^\w\s]','',sentence) # remove punctuation
    tokenized_sentence = nltk.word_tokenize(sentence)

    pos_word_list=[]
    neu_word_list=[]
    neg_word_list=[]

    for word in tokenized_sentence:
        if (SENT_OBJ.polarity_scores(word)['compound']) >= 0.1:
            pos_word_list.append(word)
        elif (SENT_OBJ.polarity_scores(word)['compound']) <= -0.1:
            neg_word_list.append(word)
        else:
            neu_word_list.append(word)  


    sentiment_dict = SENT_OBJ.polarity_scores(sentence)

    return sentiment_dict['pos'], sentiment_dict['neu'], sentiment_dict['neg'], sentiment_dict['compound'], ", ".join(pos_word_list), ", ".join(neu_word_list), ", ".join(neg_word_list)


def main():
    conn = psycopg2.connect(database="dfqt9vfoh18uko", 
                                     user='kdlnydflpyjhnx',
                                     password='877fc89efc05c0c0f3bc52fbe87ae0b4db0c044cd83c0e14107b42fddb032901',
                                     host='ec2-3-216-167-65.compute-1.amazonaws.com',
                                     port='5432')

    sql = "select * from setup.reddit_commentary;"
    reddit_df = sqlio.read_sql_query(sql, conn)


    reddit_df["positive_score"], reddit_df['neutral_score'], reddit_df['negative_score'], reddit_df['compound_score'], reddit_df['positive_words'], reddit_df['neutral_words'], reddit_df['negative_words'] = zip(*reddit_df['comments'].map(sentiment_analysis))

    sentiment_results_df = reddit_df[['id', 'positive_score', 'neutral_score', 'negative_score', 'compound_score', 'positive_words', 'neutral_words', 'negative_words']].copy()
    sentiment_results_df.rename(columns={'id': 'comment_id'}, inplace=True)

    sentiment_results_df.to_csv("test.csv")

    tuples = [tuple(x) for x in sentiment_results_df.to_numpy()]
  
    cols = ','.join(list(sentiment_results_df.columns))
  
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % ('mlresults.sentiment', cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    conn.close()

if __name__=='__main__':
    main()