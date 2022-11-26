import pandas as pd
import functools as ft
from app.config import db

##Use this for matching our stocks to their appropriate benchmark?
class Benchmark:
    def __init__(self):
        self.snp='^GSPC'
        self.dji='^DJI'

    def StockList(self):
        return pd.read_sql(f"select distinct ticker From setup.equity_pricing order by 1", db)['ticker'].values

    def StockPrices(self, ticker, from_dt, to_dt):
        stockdata=pd.read_sql(f"select businessdate, ticker, close From setup.equity_pricing where ticker='{ticker}' and businessdate between '{from_dt}' and '{to_dt}' order by 1", db, index_col='businessdate')
        snpdata=pd.read_sql(f"select businessdate, ticker, close From setup.index_pricing where ticker='{self.snp}' and businessdate between '{from_dt}' and '{to_dt}' order by 1", db, index_col='businessdate')
        djidata=pd.read_sql(f"select businessdate, ticker, close From setup.index_pricing where ticker='{self.dji}' and businessdate between '{from_dt}' and '{to_dt}' order by 1", db, index_col='businessdate')
        
        stockdata['ticker_stdclose']=stockdata.loc[:,'close'].apply(lambda e : (e - stockdata['close'].mean()) / stockdata['close'].std())
        stockdata=stockdata.rename(columns={"ticker": "ticker", "close": "ticker_close"})

        snpdata['snp_stdclose']=snpdata.loc[:,'close'].apply(lambda i : (i - snpdata['close'].mean()) / snpdata['close'].std())
        snpdata=snpdata.rename(columns={"ticker": "snp_ticker", "close": "snp_close"})

        djidata['dji_stdclose']=djidata.loc[:,'close'].apply(lambda i : (i - djidata['close'].mean()) / djidata['close'].std())
        djidata=djidata.rename(columns={"ticker": "dji_ticker", "close": "dji_close"})

        stck_dfs = [stockdata, snpdata, djidata]
        #inner join on market dates, excluding weekends/holidays
        df_final = ft.reduce(lambda left, right: pd.merge(left, right, on='businessdate'), stck_dfs) 

        return df_final.reset_index().to_dict(orient='list')

    def SentimentData(self, ticker, from_dt, to_dt):
        sentimentdata=pd.read_sql(f"""select ticker, created_dt traded_dt, title, selftext, comments
                                    from setup.reddit_commentary 
                                    where ticker='{ticker}' 
                                    and created_dt between '{from_dt}' and '{to_dt}' 
                                    order by created_dt desc""", db)

        sentimentdata['merged_comments']=sentimentdata[['title','selftext','comments']].stack().groupby(level=0).apply(' '.join)

        return sentimentdata[['ticker','traded_dt','merged_comments']].to_dict(orient='list')
