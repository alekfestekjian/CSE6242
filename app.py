from flask import Flask
from flask import render_template
from datetime import time
from flask_sqlalchemy import SQLAlchemy
import psycopg2

app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://qcqgpbdohgijqd:f00c9aed2fb5f6482b98796671ff2382e2516067e4f2cdb91c2a94aa38f67139@ec2-3-220-207-90.compute-1.amazonaws.com:5432/d3skeejujfn0f0"

db = SQLAlchemy(app)

conn = psycopg2.connect(database="d3skeejujfn0f0",
                                     user='qcqgpbdohgijqd',
                                     password='f00c9aed2fb5f6482b98796671ff2382e2516067e4f2cdb91c2a94aa38f67139',
                                     host='ec2-3-220-207-90.compute-1.amazonaws.com',
                                     port='5432')
cursor = conn.cursor()

# Stock data schema: "setup"
# Stock data table: "equity_pricing"
# Columns: id, ticker, businessdate, close, open, high, low, adjclose, volume

@app.route("/", methods=['post', 'get'])
def test():  
     cursor.execute("select distinct ticker from setup.equity_pricing")
     tickers = cursor.fetchall()

     data = {}
     for t in tickers: 
        cursor.execute(f"select close from setup.equity_pricing where ticker = '{t[0]}'")
        data[str(t[0])] = cursor.fetchall()

     cursor.execute(f"select businessdate from setup.equity_pricing where ticker = 'AAPL'")
     time = cursor.fetchall()

     return render_template("all_stock_prices.html", data = data, labels = time)

if __name__ == "__main__":
    app.run(debug=True)
