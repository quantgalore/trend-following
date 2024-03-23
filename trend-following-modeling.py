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
import seaborn as sns

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

engine = sqlalchemy.create_engine('mysql+mysqlconnector://username:password@database-host-name:3306/database-name')
trend_dataset = pd.read_sql("SELECT * FROM trend_dataset", con = engine).set_index("t")
tickers = trend_dataset["ticker"].drop_duplicates().values

# If you want to just model commodities, uncomment the below line
# tickers = np.array(["USO", "GLD", "SLV", "UNG", "WEAT"])

regime_return_list = []

for ticker in tickers:
    
    ticker_data = trend_dataset[trend_dataset["ticker"] == ticker].copy()
    
    plt.figure(dpi=200)
    plt.xticks(rotation = 45)
    plt.plot(ticker_data.index, ticker_data["3_mo_avg"])
    plt.plot(ticker_data.index, ticker_data["6_mo_avg"])
    plt.plot(ticker_data.index, ticker_data["c"])
    plt.legend([ "3m", "6m", "price"])
    plt.title(f"{ticker}")
    plt.show()
    
    ticker_data['regime'] = ticker_data.apply(lambda row: 1 if (row['3_mo_avg'] > row['6_mo_avg']) else 0, axis=1)
    ticker_data['regime_change'] = ticker_data['regime'].diff().ne(0)
    
    regime_change_dates = ticker_data[ticker_data['regime_change']].index.values
    
    regime_data_list = []
    
    for regime_change in regime_change_dates:
        
        if regime_change == regime_change_dates[-1]:
            regime_data = ticker_data[(ticker_data.index >= regime_change)].copy()
        else:
            regime_data = ticker_data[(ticker_data.index >= regime_change) & (ticker_data.index <= regime_change_dates[np.where(regime_change_dates==regime_change)[0][0]+1])].copy()
        regime_data["pct_change"] = round(regime_data["c"].pct_change()*100, 2)
        
        regime = regime_data["regime"].iloc[0]
        
        total_return = round(((regime_data["c"].iloc[-1] - regime_data["c"].iloc[0]) / regime_data["c"].iloc[0]) * 100, 2)
        
        if regime == 1:
            position_return = round(((regime_data["c"].iloc[-1] - regime_data["c"].iloc[0]) / regime_data["c"].iloc[0]) * 100, 2)
            dollar_return = regime_data["c"].iloc[-1] - regime_data["c"].iloc[0]
        elif regime == 0:
            position_return = round(((regime_data["c"].iloc[0] - regime_data["c"].iloc[-1]) / regime_data["c"].iloc[0]) * 100, 2)
            dollar_return = regime_data["c"].iloc[0] - regime_data["c"].iloc[-1]
        
        if len(regime_data) < 2:
            return_data = pd.DataFrame([{"date": regime_data.index[0], "regime": regime, "return": total_return,"position_return": position_return,
                                         "dollar_return": dollar_return, "days_of_regime": (regime_data.index[-1] - regime_data.index[0]).days}])
        
        else: 
            return_data = pd.DataFrame([{"date": regime_data.index[0], "regime": regime, "return": total_return,"position_return": position_return,
                                     "dollar_return": dollar_return, "days_of_regime": (regime_data.index[-2] - regime_data.index[0]).days}])
        regime_data_list.append(return_data)
        
    full_regime_data = pd.concat(regime_data_list)
    full_regime_data["ticker"] = ticker
        
    plt.figure(dpi=200)
    plt.xticks(rotation = 45)
    plt.xlabel("Date")
    plt.ylabel("% Gain")
    plt.title(f"{ticker} - Trend Following Performance")
    plt.plot(full_regime_data["date"].values, full_regime_data["position_return"].cumsum())   
    plt.show()
    
    regime_return_list.append(full_regime_data)
    
full_regime_dataset = pd.concat(regime_return_list).sort_values(by="date", ascending = True)

positive_regimes = full_regime_dataset[full_regime_dataset["regime"] == 1].copy().sort_values(by="date", ascending=True)
negative_regimes = full_regime_dataset[full_regime_dataset["regime"] == 0].copy().sort_values(by="date", ascending=True)

plt.figure(dpi = 800)
sns.boxplot(x='regime', y='return', data=full_regime_dataset)
plt.title('Returns of Regimes')
plt.xlabel('Regime')
plt.ylabel('Return')
plt.xticks([0, 1], ['Negative Regime', 'Positive Regime'])
plt.show()