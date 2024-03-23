# -*- coding: utf-8 -*-
"""
Created in 2024

@author: Quant Galore
"""

import requests
import pandas as pd
import numpy as np
import mysql.connector
import sqlalchemy
import pytz

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")
date = calendar.schedule(start_date = (datetime.today()-timedelta(days=10)), end_date = datetime.today()).index.strftime("%Y-%m-%d").values[-1]

tz = pytz.timezone("GMT")

all_stocks_1 = requests.get(f"https://api.polygon.io/v3/reference/tickers?type=CS&market=stocks&active=true&order=asc&sort=ticker&limit=1000&apiKey={polygon_api_key}").json()
all_stocks_2 = requests.get(f"{all_stocks_1['next_url']}&apikey={polygon_api_key}").json()
all_stocks_3 = requests.get(f"{all_stocks_2['next_url']}&apikey={polygon_api_key}").json()
all_stocks_4 = requests.get(f"{all_stocks_3['next_url']}&apikey={polygon_api_key}").json()
all_stocks_5 = requests.get(f"{all_stocks_4['next_url']}&apikey={polygon_api_key}").json()
all_stocks_6 = requests.get(f"{all_stocks_5['next_url']}&apikey={polygon_api_key}").json()

stock_list = [all_stocks_1, all_stocks_2, all_stocks_3, all_stocks_4, all_stocks_5, all_stocks_6]
available_stock_list = []

for stock_data in stock_list:
    available_stocks  = pd.json_normalize(stock_data["results"])
    available_stock_list.append(available_stocks)
    
total_available_stocks = pd.concat(available_stock_list)

tickers = total_available_stocks["ticker"].values
valid_tickers = []
times = []

for ticker in tickers:
    start_time = datetime.now()
    ticker_call_contracts = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={ticker}&contract_type=call&as_of={date}&expired=false&limit=1000&apiKey={polygon_api_key}").json()["results"])
    
    if len(ticker_call_contracts) < 1:
        continue
    
    expiration_dates = ticker_call_contracts["expiration_date"].drop_duplicates().values
    
    if len(expiration_dates) < 3:
        continue
    
    days_between = (pd.to_datetime(expiration_dates[2]) -pd.to_datetime(expiration_dates[1])).days
    
    if days_between < 5:
        continue
    elif days_between >= 5:
        valid_tickers.append(ticker)
        
    end_time = datetime.now()
    seconds_to_complete = (end_time - start_time).total_seconds()
    times.append(seconds_to_complete)
    iteration = round((np.where(tickers==ticker)[0][0]/len(tickers))*100,2)
    iterations_remaining = len(tickers) - np.where(tickers==ticker)[0][0]
    average_time_to_complete = np.mean(times)
    estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
    time_remaining = estimated_completion_time - datetime.now()
            
    print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")
        
complete_valid_tickers = pd.DataFrame({"tickers":valid_tickers})

engine = sqlalchemy.create_engine('mysql+mysqlconnector://username:password@database-host-name:3306/database-name')
complete_valid_tickers.to_sql("weekly_option_tickers", con = engine)