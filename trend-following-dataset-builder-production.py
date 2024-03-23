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
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

engine = sqlalchemy.create_engine('mysql+mysqlconnector://username:password@database-host-name:3306/database-name')
tickers_original = pd.read_sql("weekly_option_tickers", con = engine)["tickers"].values
addl_tickers = np.array(["USO", "GLD", "SLV", "UNG", "WEAT", "SPY", "QQQ"])

tickers = np.append(tickers_original, addl_tickers)
##

trading_dates = calendar.schedule(start_date = "2020-01-01", end_date = (datetime.today())).index.strftime("%Y-%m-%d").values

ticker_trends_list = []
times = []

for ticker in tickers:
    try:
        
        start_time = datetime.now()
    
        ticker_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{trading_dates[0]}/{trading_dates[-1]}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
        ticker_data.index = pd.to_datetime(ticker_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        
        ticker_data["20_mov_avg"] = ticker_data["c"].rolling(window=20).mean()
        ticker_data["3_mo_avg"] = ticker_data["c"].rolling(window=63).mean()
        ticker_data["6_mo_avg"] = ticker_data["c"].rolling(window=126).mean()
        ticker_data["12_mo_avg"] = ticker_data["c"].rolling(window=252).mean()
        ticker_data["ticker"] = ticker
        ticker_data = ticker_data[['v', 'vw', 'o', 'c', 'h', 'l', 'n', '20_mov_avg','3_mo_avg' ,'6_mo_avg','12_mo_avg','ticker']].dropna()
    
        ticker_trends_list.append(ticker_data)
        
        end_time = datetime.now()
        seconds_to_complete = (end_time - start_time).total_seconds()
        times.append(seconds_to_complete)
        iteration = round((np.where(tickers==ticker)[0][0]/len(tickers))*100,2)
        iterations_remaining = len(tickers) - np.where(tickers==ticker)[0][0]
        average_time_to_complete = np.mean(times)
        estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
        time_remaining = estimated_completion_time - datetime.now()
        
        print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")
    except Exception as error:
        print(ticker, error)
        continue

full_ticker_data = pd.concat(ticker_trends_list).reset_index()

engine = sqlalchemy.create_engine('mysql+mysqlconnector://username:password@database-host-name:3306/database-name')
full_ticker_data.to_sql("production_trend_dataset", con = engine, if_exists = "replace", chunksize = 5000)
