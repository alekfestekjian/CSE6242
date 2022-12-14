from app import app
from app.subs.support import Benchmark
from flask import render_template, request, jsonify

bench=Benchmark()

@app.route('/', methods=["GET"])
@app.route('/main', methods=["GET"])
def index():
    headline="CSE 6242 Social Media Sentiment Analysis"
    return render_template("main.html", stocklist=bench.StockList(), headline=headline)

@app.route("/getstock", methods=['POST'])
def getstock():
    stockNm=request.get_json()
    predict, pricing_data, dji_beta, snp_beta = bench.StockPrices(stockNm['stockchoice'], stockNm['from_date'], stockNm['to_date'])
    return jsonify({"stockdata": pricing_data, 'prediction':predict, 'dji_beta': dji_beta, 'snp_beta':snp_beta})

@app.route("/getsentiment", methods=['POST'])
def getsentiment():
    sent=request.get_json()
    sentiment, categorical = bench.SentimentData(sent['ticker'], sent['from_date'], sent['to_date'])
    return jsonify({"sentiment": sentiment, 'categorical':categorical, 'ticker': sent['ticker'], 'from_date': sent['from_date'], 'to_date': sent['to_date'] })




