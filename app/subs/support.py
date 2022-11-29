import pandas as pd
import functools as ft
import numpy as np
from datetime import datetime
from app.config import db

class Benchmark:
    def __init__(self):
        self.snp='^GSPC'
        self.dji='^DJI'
    
    def StockList(self):
        return pd.read_sql(f"select distinct ticker From setup.equity_pricing where ticker != 'OPEN' order by 1", db)['ticker'].values

    def StockPrices(self, ticker, from_dt, to_dt):
        predict_to_dt = (datetime.strptime(to_dt,'%Y-%m-%d') - pd.tseries.offsets.CustomBusinessDay(n=-7)).strftime('%Y-%m-%d')

        stockdata=pd.read_sql(f"select businessdate, ticker, adjclose From setup.equity_pricing where ticker='{ticker}' and businessdate between '{from_dt}' and '{to_dt}' order by 1", db, index_col='businessdate')
        predict=pd.read_sql(f"select businessdate, ticker, price From mlresults.ticker_response where ticker='{ticker}' and businessdate between '{from_dt}' and '{predict_to_dt}' order by 1", db, index_col='businessdate')
        snpdata=pd.read_sql(f"select businessdate, ticker, close From setup.index_pricing where ticker='{self.snp}' and businessdate between '{from_dt}' and '{to_dt}' order by 1", db, index_col='businessdate')
        djidata=pd.read_sql(f"select businessdate, ticker, close From setup.index_pricing where ticker='{self.dji}' and businessdate between '{from_dt}' and '{to_dt}' order by 1", db, index_col='businessdate')
        
        stockdata['ticker_stdclose']=stockdata.loc[:,'adjclose'].apply(lambda e : (e - stockdata['adjclose'].mean()) / stockdata['adjclose'].std())
        stockdata=stockdata.rename(columns={"ticker": "ticker", "adjclose": "ticker_close"})

        predict['predict_stdclose']=predict.loc[:,'price'].apply(lambda e : (e - predict['price'].mean()) / predict['price'].std())
        predict=predict.rename(columns={"ticker": "ticker", "price": "ticker_close"})

        snpdata['snp_stdclose']=snpdata.loc[:,'close'].apply(lambda i : (i - snpdata['close'].mean()) / snpdata['close'].std())
        snpdata=snpdata.rename(columns={"ticker": "snp_ticker", "close": "snp_close"})

        djidata['dji_stdclose']=djidata.loc[:,'close'].apply(lambda i : (i - djidata['close'].mean()) / djidata['close'].std())
        djidata=djidata.rename(columns={"ticker": "dji_ticker", "close": "dji_close"})

        stck_dfs = [stockdata, snpdata, djidata, predict]
        #inner join on market dates, excluding weekends/holidays
        df_final = ft.reduce(lambda left, right: pd.merge(left, right, on='businessdate'), stck_dfs) 

        #append the next 6 predicted dates based on user selected dates + 6 days (predictions are agnostic of market business dates)
        predicted_dict=predict[-7:].reset_index().to_dict(orient='list')

        df_final=df_final[['predict_stdclose','ticker_stdclose','snp_stdclose','dji_stdclose']].reset_index().to_dict(orient='list')
        df_final['businessdate'] += predicted_dict['businessdate']
        df_final['predict_stdclose'] += predicted_dict['predict_stdclose']
        
        #dictionary containing the predicted prices only after the stock pricing end date
        predict_dict=predict[-7:].reset_index().to_dict(orient='list')
        predict_dict['ticker_close'] = [f'{x:,.2f}' for x in predict_dict['ticker_close']]

        #calculate the market betas of stock X with the S&P500 and Dow Jones
        dji_bench_returns = djidata['dji_close'].pct_change()[1:]
        snp_bench_returns = snpdata['snp_close'].pct_change()[1:]
        stock_returns = stockdata['ticker_close'].pct_change()[1:]

        stck = np.array(stock_returns)
        dji = np.array(dji_bench_returns)
        snp = np.array(snp_bench_returns)

        dji_betas = 1 / np.dot(dji, dji.transpose()) * np.dot(dji.transpose(), stck)
        snp_betas = 1 / np.dot(snp, snp.transpose()) * np.dot(snp.transpose(), stck)

        return predict_dict, df_final, round(dji_betas,4), round(snp_betas,4)

    def SentimentData(self, ticker, from_dt, to_dt):
        sentimentdata=pd.read_sql(f"""Select com.ticker, to_char(com.created_dt, 'YYYY Month') traded_dt, com.title, com.selftext, com.comments, sent.compound_score sentiment
                                        From setup.reddit_commentary com
                                            inner join mlresults.sentiment sent
                                            on com.id=sent.comment_id
                                        Where com.ticker='{ticker}'
                                        And com.created_dt between '{from_dt}' and '{to_dt}' 
                                        order by 2""", db)

        sentimentdata['merged_comments']=sentimentdata[['title','selftext','comments']].stack().groupby(level=0).apply(' '.join)

        if len(sentimentdata) > 0:
            #convert our nltk -1 to 1 output into 5 separate buckets for 
            ratings=['Strong Sell', 'Sell', 'Hold', 'Buy', 'Strong Buy']
            bins=[-1.0, -0.6, -0.2, 0.2, 0.6, 1.0]
            sentimentdata['category'] = pd.cut(sentimentdata.sentiment, bins=bins, labels=ratings, precision=4)

            #simple bar chart reflecting the breakout by user sentiment for the above time frame
            sentiment_ratings = (
                sentimentdata.groupby('category')
                .agg(category_cnts=('category','count'))
                .reset_index()
            )

            return sentimentdata[['ticker','traded_dt','merged_comments']].to_dict(orient='list'), sentiment_ratings.to_dict(orient='list')

        else:
            return None