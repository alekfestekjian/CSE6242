# template taken from https://medium.com/swlh/how-to-scrape-large-amounts-of-reddit-data-using-pushshift-1d33bde9286#:~:text=Under%20the%20hood%2C%20pmaw%20makes,of%2060%20requests%20per%20minute.

import datetime as dt
import pandas as pd
from pmaw import PushshiftAPI

subreddits = ['wallstreetbets', 'stockmarket', 'investing', 'trading', 'stocks', 'finance', 'robinhood']
stocks = ["AMZN","GME","TSLA","AMC","AAPL","META","MSFT","NFLX","JPM","GOOG","DIS","SNAP","NOK","BB","AAP","BTC","PFE","HD","KO","MMM","PLTR","V","PG","JNJ","DJIA","GSPC","SHOP","SPY","GOOGL","BABA","WISH","FB","DB","OPEN"]

after = int(dt.datetime(2016,1,1,0,0).timestamp())
before = int(dt.datetime(2022,9,30,0,0).timestamp())


api = PushshiftAPI()

subreddit_dfs = []
for s in subreddits:
    comments = api.search_comments(after=after,before=before, subreddit=s)
    comments_df = pd.DataFrame(comments)
    comments_df = comments_df[['author', 'author_fullname', 'body', 'parent_id', 'score', 'subreddit']].replace('\n','', regex=True).replace(',','', regex=True) # keep only certain columns and remove line breaks and commas
    subreddit_dfs.append(comments_df)

reddit_data = pd.concat(subreddit_dfs)

for stock in stocks:
    stock_df = reddit_data[reddit_data['body'].str.contains(stock)]
    csv_string = f'./reddit/comments/{stock}_comments.csv'
    stock_df.to_csv(csv_string, header=True, index=False, columns=list(stock_df.axes[1]))
