import pandas as pd
from yahoo_fin.stock_info import get_data

# Import stock data from Yahoo finance

# Filter the data for the time period 
start = "01/01/2016"
end = "09/30/2022"

# Pull the data for the selected stocks
tickers = ["AMZN", "GME", "TSLA", "AMC", "AAPL",  "META", "MSFT", "NFLX", "JPM", "GOOG", "DIS",  
           "SNAP", "NOK", "BB",  "AAP", "BTC-USD", "PFE", "HD", "KO", "MMM", "PLTR", "V", "PG", 
           "JNJ", "SHOP", "SPY", "GOOGL", "BABA", "WISH", "DB",  "OPEN"]

stck_data = {}
for tick in tickers:
    stck_data[tick.lower()] = get_data(tick.lower(), start_date= start, end_date= end)

# Export Stock Data into csv
stck_data = pd.concat(stck_data.values(), ignore_index= False)
stck_data.to_csv('~/data/Stock_Data.csv')

# Pull the data for the stock indexes
indices = ["^IXIC", "^DJI", "^GSPC"]

idx_data = {}
for ind in indices:
    idx_data[ind.lower()] = get_data(ind.lower(), start_date= start, end_date= end)

idx_data = pd.concat(idx_data.values(), ignore_index= False)
idx_data.to_csv('~/data/Index_Data.csv')

