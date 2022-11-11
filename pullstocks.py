#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 10 17:10:51 2022

@author: ashleyroakes
"""

import pandas as pd
from yahoo_fin.stock_info import get_data

# Import stock data from Yahoo finance

# Filter the data for the time period 
start = "01/01/2016"
end = "09/30/2022"

tickers = ["AMZN", "GME", "TSLA", "AMC", "AAPL",  "META", "MSFT", "NFLX", "JPM", "GOOG", "DIS",  
           "SNAP", "NOK", "BB",  "AAP", "BTC", "PFE", "HD", "KO", "MMM", "PLTR", "V", "PG", "JNJ", 
           "DJIA","SHOP", "SPY", "GOOGL", "BABA", "WISH", "DB",  "OPE", "^IXIC", "^DJI", "^GSPC"]

stck_data = {}
for tick in tickers:
    stck_data[tick.lower()] = get_data(tick.lower(), start_date= start, end_date= end)

# Export Stock Data into csv
stck_data = pd.concat(stck_data.values(), ignore_index= False)
#stck_data.to_csv('/Users/ashleyroakes/Desktop/Stock Data/Stock_Data.csv')