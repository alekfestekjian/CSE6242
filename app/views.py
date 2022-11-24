from app import app
from app.config import db
from app.subs.support import Benchmark

import pandas as pd
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
    stockdata=pd.read_sql(f"select businessdate, ticker, close From setup.equity_pricing where ticker='{stockNm['stockchoice']}' and businessdate >= '2018-01-01' order by 1", db)
    snpdata=pd.read_sql(f"select businessdate, ticker, close From setup.index_pricing where ticker='^GSPC' and businessdate >= '2018-01-01' order by 1", db)
    djidata=pd.read_sql(f"select businessdate, ticker, close From setup.index_pricing where ticker='^DJI' and businessdate >= '2018-01-01' order by 1", db)
    
    stockdata['stdclose']=stockdata.loc[:,'close'].apply(lambda e : (e - stockdata['close'].mean()) / stockdata['close'].std())
    snpdata['stdclose']=snpdata.loc[:,'close'].apply(lambda i : (i - snpdata['close'].mean()) / snpdata['close'].std())
    djidata['stdclose']=djidata.loc[:,'close'].apply(lambda i : (i - djidata['close'].mean()) / djidata['close'].std())

    return jsonify({"stockdata": stockdata.to_dict(orient='list'), 
                    "snpdata": snpdata.to_dict(orient='list'), 
                    "djidata": djidata.to_dict(orient='list')})





