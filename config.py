# config.py

# 系統設定
TIMEZONE = "Asia/Taipei"  # 排程依據的時區
MAX_OUTPUT = 20           # 最終清單最大數量

# 1. Universe 過濾
EXCLUDE_SYMBOLS = [
    'BTCUSDT', 'USDCUSDT', 'BTCDOMUSDT', 'ALLUSDT', 
    'XAUUSDT', 'XAGUSDT', 'TSLAUSDT', 'EURUSDT', 'GBPUSDT'
]
MIN_HISTORY_DAYS = 60     # 至少要有 60 天資料

# 2. 噪音門檻 (Noise Gate)
ATR_PERCENTILE_THRESHOLD = 0.95  # 排除 ATR% > P95 (極端波動)
VOL_MEDIAN_DAYS = 7
VOL_PERCENTILE_THRESHOLD = 0.10  # 排除成交量 < P10 (流動性差)

# 4. RS 模組 (BTC Benchmark)
RS_WINDOW_SHORT = 72    # 3 Days (hours)
RS_WINDOW_MID = 168     # 7 Days
RS_WINDOW_LONG = 336    # 14 Days
RS_WEIGHTS = {
    '14D': 0.50,
    '7D': 0.30,
    '3D': 0.20
}
RS_HARD_THRESHOLD = 0.70  # RS_Rank 必須 > P70 (除了 Turning)

# 5. Setup 模組
# VCP
VCP_IMPULSE_WINDOW = 72 # 3D
VCP_IMPULSE_RANK = 0.80 # P80
VCP_LOOKBACK_MIN = 24
VCP_LOOKBACK_MAX = 168
VCP_WIDTH_HISTORY_RANK = 0.20 # 寬度需在自身歷史 P20 以下
VCP_POS_THRESHOLD = 0.66      # 收盤價在區間 > 2/3

# PowerPlay
PP_LOOKBACK_BREAKOUT = 48
PP_TR_HISTORY_RANK = 0.90     # Breakout TR > P90
PP_CLOSE_POS_RANK = 0.75      # 收盤在 K 棒上方 P75
PP_FLAG_WIDTH_RANK = 0.20     # 旗形寬度 < P20

# 評分權重
SCORE_WEIGHTS = {
    'RS': 0.6,
    'Setup': 0.2,
    'Trend': 0.2
}
