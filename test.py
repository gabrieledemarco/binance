import seaborn as sns
import matplotlib.pyplot as plt  # Compute the correlation matrix
import pandas as pd
import numpy as np
from BinanceService import BinanceService
import DateFunction as dt
from DbService import DbService
from CommonTable import CommonTable



""" 
def get_Corr_HeatMap(data: pd.DataFrame):
    corr = df.corr()  # Generate a mask for the upper triangle
    mask = np.zeros_like(corr, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True  # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(10, 10))  # Draw the heatmap with the mask and correct aspect ratio
    sns.heatmap(corr, annot=True, fmt='.4f', mask=mask, center=0, square=True, linewidths=.5)


bins = BinanceService(api_key='2mYr1HH1a9O3LR3ogAoO9SowRD0DwFX9nLZRUnGifIPmGfmznoVemAqRVc8JKMoC',
                      api_secret='LvUCcMAe3FecFpY9KVQMOquD8UpYHJfCY1y9EbzMgbSCwhHBmB4CruhsBUzKYsa5')

tickers = {'BTC', 'ETH', 'SOL', 'FTM'}
df = pd.DataFrame()

for tick in tickers:
    df.insert(2, tick,
              bins.get_price_historical_kline(symbol=tick,
                                              interval='1d',
                                              start_date=dt.get_past_date(dt.now_date(), month=6),
                                              end_date=dt.now_date()), True)
print(df)
"""

