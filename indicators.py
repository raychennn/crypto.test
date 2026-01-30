import pandas as pd
import numpy as np
from scipy.stats import linregress

def calculate_ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def calculate_atr(df, period=20):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

def get_slope(series, window=48):
    # 使用 numpy polyfit 計算線性回歸斜率，處理 NaN
    def _slope(y):
        if np.isnan(y).any(): return np.nan
        x = np.arange(len(y))
        slope, _, _, _, _ = linregress(x, y)
        return slope
    
    return series.rolling(window=window).apply(_slope, raw=True)

def rolling_percentile(series, window=60):
    # 計算當前值在過去 window 內的百分位排名
    # 注意：計算量大，優化使用 pandas rank
    return series.rolling(window=window).rank(pct=True)
