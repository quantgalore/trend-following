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
trend_dataset = pd.read_sql("production_trend_dataset", con = engine).set_index("t")
tickers = trend_dataset["ticker"].drop_duplicates().values

regime_list = []

for ticker in tickers:
    
    ticker_data = trend_dataset[trend_dataset["ticker"] == ticker].copy()
    
    ticker_data['regime'] = ticker_data.apply(lambda row: 1 if (row['3_mo_avg'] > row['6_mo_avg']) else 0, axis=1)
    ticker_data['regime_change'] = ticker_data['regime'].diff().ne(0)
    
    current_regime_data = ticker_data.tail(1)
    current_regime = current_regime_data["regime"].iloc[0]
    current_regime_history = ticker_data[ticker_data.index >= (ticker_data[ticker_data["regime_change"] == 1].index[-1])].copy()
    
    performance_since_regime_start = round(((current_regime_history["c"].iloc[-1] - current_regime_history["c"].iloc[0]) / current_regime_history["c"].iloc[0]) * 100, 2)
    days_since_regime = (current_regime_history.index[-1] - current_regime_history.index[0]).days
    
    production_regime_data = pd.DataFrame([{"date": current_regime_data.index[0], "regime": current_regime, "performance_since": performance_since_regime_start,
                                            "days_since": days_since_regime, "ticker": ticker}])
    
        
    regime_list.append(production_regime_data)
    
full_regime_dataset = pd.concat(regime_list).sort_values(by="date", ascending = True)
addl_tickers = np.array(["USO", "GLD", "SLV", "UNG", "WEAT"])

commodity_regimes = full_regime_dataset[full_regime_dataset["ticker"].isin(addl_tickers)]

selected_ticker = "AAPL"
selected_ticker_data = trend_dataset[(trend_dataset["ticker"] == selected_ticker) & (trend_dataset.index >= "2023-01-01")].copy()
selected_ticker_trend_data = full_regime_dataset[(full_regime_dataset["ticker"] == selected_ticker)].copy()

plt.figure(dpi=200)
plt.xticks(rotation = 45)
plt.xlabel("Date")
plt.ylabel("Price")
plt.title(f"{selected_ticker}")
plt.suptitle(f"Days since regime: {selected_ticker_trend_data['days_since'].iloc[0]}")
plt.plot(selected_ticker_data.index, selected_ticker_data["3_mo_avg"])
plt.plot(selected_ticker_data.index, selected_ticker_data["6_mo_avg"])
plt.plot(selected_ticker_data.index, selected_ticker_data["c"])
plt.legend([ "3m", "6m", "price"])
plt.show()

positive_regimes = full_regime_dataset[full_regime_dataset["regime"] == 1].copy().sort_values(by="date", ascending=True)
negative_regimes = full_regime_dataset[full_regime_dataset["regime"] == 0].copy().sort_values(by="date", ascending=True)