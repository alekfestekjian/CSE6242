from app import app
from app.config import db
from app.subs.support import Benchmark

import pandas as pd
import functools as ft
from flask import render_template, request, jsonify

 
@app.route('/', methods=["GET"])
@app.route('/main', methods=["GET"])
def index():
    headline="CSE 6242 Social Media Sentiment Analysis"
    stocklist = pd.read_sql(f"select distinct ticker From setup.equity_pricing order by 1", db)
    return render_template("main.html", stocklist=stocklist['ticker'].values, headline=headline)

@app.route("/getstock", methods=['POST'])
def getstock():
    stockNm=request.get_json()
    stockdata=pd.read_sql(f"select businessdate, ticker, close From setup.equity_pricing where ticker='{stockNm['stockchoice']}' and businessdate between '{stockNm['from_date']}' and '{stockNm['to_date']}' order by 1", db, index_col='businessdate')
    snpdata=pd.read_sql(f"select businessdate, ticker, close From setup.index_pricing where ticker='^GSPC' and businessdate between '{stockNm['from_date']}' and '{stockNm['to_date']}' order by 1", db, index_col='businessdate')
    djidata=pd.read_sql(f"select businessdate, ticker, close From setup.index_pricing where ticker='^DJI' and businessdate between '{stockNm['from_date']}' and '{stockNm['to_date']}' order by 1", db, index_col='businessdate')
    
    stockdata['ticker_stdclose']=stockdata.loc[:,'close'].apply(lambda e : (e - stockdata['close'].mean()) / stockdata['close'].std())
    stockdata=stockdata.rename(columns={"ticker": "ticker", "close": "ticker_close"})

    snpdata['snp_stdclose']=snpdata.loc[:,'close'].apply(lambda i : (i - snpdata['close'].mean()) / snpdata['close'].std())
    snpdata=snpdata.rename(columns={"ticker": "snp_ticker", "close": "snp_close"})

    djidata['dji_stdclose']=djidata.loc[:,'close'].apply(lambda i : (i - djidata['close'].mean()) / djidata['close'].std())
    djidata=djidata.rename(columns={"ticker": "dji_ticker", "close": "dji_close"})

    stck_dfs = [stockdata, snpdata, djidata]
    #inner join on market dates, excluding weekends/holidays
    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on='businessdate'), stck_dfs) 
    
    return jsonify({"stockdata": df_final.reset_index().to_dict(orient='list')})


@app.route("/getsentiment", methods=['POST'])
def getsentiment():
    s=request.get_json()

    sentimentdata=pd.read_sql(f"""select ticker, created_dt traded_dt, title, selftext, comments
                                    from setup.reddit_commentary 
                                    where ticker='{s['ticker']}' 
                                    and created_dt between '{s['from_date']}' and '{s['to_date']}' 
                                    order by created_dt desc""", db)

    sentimentdata['merged_comments']=sentimentdata[['title','selftext','comments']].stack().groupby(level=0).apply(' '.join)

    return jsonify({"sentiment": sentimentdata[['ticker','traded_dt','merged_comments']].to_dict(orient='list')})




