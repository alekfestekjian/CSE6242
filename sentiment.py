## Method from https://medium.com/nerd-for-tech/wallstreetbets-sentiment-analysis-on-stock-prices-using-natural-language-processing-ed1e9e109a37

from nltk.sentiment.vader import SentimentIntensityAnalyzer
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2.extras as extras
import nltk

## custom word values
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
}

def sentiment_analysis(sentence):

    sent_obj  = SentimentIntensityAnalyzer()
    sent_obj.lexicon.update(CUSTOM_WORDS)

    sentiment_dict = sent_obj.polarity_scores(sentence)

    return sentiment_dict['compound']


def main():
    nltk.download('vader_lexicon')

    conn = psycopg2.connect(database="dfqt9vfoh18uko", 
                                     user='kdlnydflpyjhnx',
                                     password='877fc89efc05c0c0f3bc52fbe87ae0b4db0c044cd83c0e14107b42fddb032901',
                                     host='ec2-3-216-167-65.compute-1.amazonaws.com',
                                     port='5432')

    sql = "select * from setup.reddit_commentary;"
    reddit_df = sqlio.read_sql_query(sql, conn)

    sent_obj  = SentimentIntensityAnalyzer()
    sent_obj.lexicon.update(CUSTOM_WORDS)

    reddit_df["sentiment"] = reddit_df["comments"].apply(sentiment_analysis)

    sentiment_results_df = reddit_df[['id', 'sentiment']].copy()
    sentiment_results_df.rename(columns={'id': 'comment_id'}, inplace=True)

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