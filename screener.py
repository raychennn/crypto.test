import pandas as pd
import numpy as np
from indicators import calculate_ema, calculate_atr, get_slope, rolling_percentile
import config
import logging

logger = logging.getLogger(__name__)

class CryptoScreener:
    def __init__(self, data_map, btc_df):
        self.data = data_map
        self.btc = btc_df
        self.results = []

    def run(self):
        logger.info("Starting screening pipeline...")
        
        # 0. 準備全市場數據以計算 ATR/Vol 分位數
        universe_metrics = []
        
        # 預處理每個幣種
        processed_data = {}
        
        for symbol, df in self.data.items():
            if symbol == 'BTCUSDT': continue # Benchmark 不參與篩選但參與計算
            
            # --- 指標計算 ---
            df = df.copy()
            df['atr'] = calculate_atr(df)
            df['atr_pct'] = df['atr'] / df['close']
            df['vol_value'] = df['volume'] * df['close']
            
            # 存下最後一根 (未收盤的當下) 用於全市場比較
            last_row = df.iloc[-1]
            universe_metrics.append({
                'symbol': symbol,
                'atr_pct': last_row['atr_pct'],
                'med_vol': df['vol_value'].rolling(24*7).median().iloc[-1]
            })
            processed_data[symbol] = df

        # --- Step 2: Noise Gate (全市場分位數) ---
        u_df = pd.DataFrame(universe_metrics)
        if u_df.empty: return []

        atr_threshold = u_df['atr_pct'].quantile(config.ATR_PERCENTILE_THRESHOLD)
        vol_threshold = u_df['med_vol'].quantile(config.VOL_PERCENTILE_THRESHOLD)

        valid_symbols = u_df[
            (u_df['atr_pct'] <= atr_threshold) & 
            (u_df['med_vol'] >= vol_threshold)
        ]['symbol'].tolist()

        logger.info(f"Noise Gate passed: {len(valid_symbols)} / {len(u_df)}")

        # --- Step 4 Preparation: Calculate RS Scores for ALL valid symbols first ---
        # 為了計算 RS Rank，需要所有合格幣種的 RS Score
        rs_scores = []
        
        for symbol in valid_symbols:
            df = processed_data[symbol]
            
            # [修正點] 使用 .ffill() 取代舊版 .fillna(method='ffill')
            btc_aligned = self.btc.reindex(df.index).ffill()
            
            # Log Return Calculation
            df['log_close'] = np.log(df['close'])
            btc_log = np.log(btc_aligned['close'])
            
            rel_rocs = {}
            score = 0
            
            for period_name, period_len in [('3D', config.RS_WINDOW_SHORT), 
                                          ('7D', config.RS_WINDOW_MID), 
                                          ('14D', config.RS_WINDOW_LONG)]:
                
                # RelROC
                coin_ret = df['log_close'].diff(period_len)
                btc_ret = btc_log.diff(period_len)
                rel_roc = coin_ret - btc_ret
                
                weight = config.RS_WEIGHTS[period_name]
                # 取最後一根的值
                val = rel_roc.iloc[-1]
                if np.isnan(val): val = 0
                
                rel_rocs[period_name] = val
                score += val * weight
            
            # RS Trend Logic
            rs_line = df['close'] / btc_aligned['close']
            rs_ma20 = calculate_ema(rs_line, 20)
            rs_slope = get_slope(rs_ma20)
            
            rs_trend_ok = (rs_line.iloc[-1] > rs_ma20.iloc[-1]) and (rs_slope.iloc[-1] > 0)

            rs_scores.append({
                'symbol': symbol,
                'rs_score': score,
                'rs_trend_ok': rs_trend_ok,
                'rs_slope': rs_slope.iloc[-1],
                'rel_rocs': rel_rocs
            })

        if not rs_scores: return []
        
        # Calculate RS Ranks (0-100)
        rs_df = pd.DataFrame(rs_scores)
        rs_df['rs_rank'] = rs_df['rs_score'].rank(pct=True) * 100
        
        # --- Main Loop: Trend & Setup ---
        final_candidates = []

        for idx, row in rs_df.iterrows():
            symbol = row['symbol']
            df = processed_data[symbol]
            rs_rank = row['rs_rank']
            
            # --- Step 3: Trend Gate ---
            # 1H Check
            ema50_1h = calculate_ema(df['close'], 50)
            ema200_1h = calculate_ema(df['close'], 200)
            ema20_1h = calculate_ema(df['close'], 20) # for PP
            
            close_now = df['close'].iloc[-1]
            
            # Resample to 4H for Trend Check
            df_4h = df.resample('4h').agg({'open':'first', 'high':'max', 'low':'min', 'close':'last'}).dropna()
            if len(df_4h) < 55: continue
            
            ema20_4h = calculate_ema(df_4h['close'], 20)
            ema45_4h = calculate_ema(df_4h['close'], 45)
            ema50_4h = calculate_ema(df_4h['close'], 50)
            
            # Slope of EMA50 4H (Check last vs 10 bars ago)
            ema50_slope_pos = ema50_4h.iloc[-1] > ema50_4h.iloc[-3] # 稍微寬鬆一點，用3根4H
            
            trend_gate_4h = (df_4h['close'].iloc[-1] > ema20_4h.iloc[-1] and 
                             ema20_4h.iloc[-1] > ema45_4h.iloc[-1] and 
                             ema50_slope_pos)
            
            trend_gate_1h = (close_now > ema50_1h.iloc[-1] and 
                             ema50_1h.iloc[-1] > ema200_1h.iloc[-1])
            
            if not (trend_gate_4h and trend_gate_1h):
                continue

            # --- Step 5: Setup Module ---
            setup_vcp = False
            setup_pp = False
            setup_score = 0
            
            # 5A: VCP-Base
            # Impulse Check: 3D return percentile in own history
            ret_3d = np.log(df['close']).diff(config.VCP_IMPULSE_WINDOW)
            current_ret = ret_3d.iloc[-1]
            # 過去 60 天的 3D returns 分布
            hist_rets = ret_3d.iloc[-24*60:] 
            impulse_ok = current_ret > hist_rets.quantile(config.VCP_IMPULSE_RANK)
            
            # Base Tightness & Position
            # 掃描 Lookback [24, 168]
            best_width_rank = 1.0 # Lower is better
            
            for L in [24, 48, 72, 96, 168]: # 簡化：檢查幾個關鍵長度
                window_high = df['high'].rolling(L).max()
                window_low = df['low'].rolling(L).min()
                
                # Current metrics
                hh = window_high.iloc[-1]
                ll = window_low.iloc[-1]
                if hh == ll: continue
                
                width = (hh - ll) / df['atr'].iloc[-1]
                pos = (close_now - ll) / (hh - ll)
                
                # Self-History Width Rank
                # 計算過去 60 天，同樣長度 L 的 width 分布
                past_widths = (df['high'].rolling(L).max() - df['low'].rolling(L).min()) / df['atr']
                # 簡單做法：計算當前 width 在過去 series 中的 percentile
                width_pct = (past_widths.iloc[-24*60:] < width).mean()
                
                if (width_pct < config.VCP_WIDTH_HISTORY_RANK and 
                    pos > config.VCP_POS_THRESHOLD):
                    
                    # MA Support Check (Approx)
                    if df['close'].iloc[-L:].min() > ema200_1h.iloc[-1]: # 簡化: 不破長均線
                         setup_vcp = True
                         best_width_rank = min(best_width_rank, width_pct)

            # 5B: Power Play
            # Breakout Check (Max 10D High)
            hh_10d = df['high'].rolling(240).max()
            # 假設 breakout 發生在過去 48 小時內
            breakout_idx = -1
            recent_highs = df['close'].iloc[-48:] > hh_10d.shift(1).iloc[-48:]
            
            if recent_highs.any():
                # 檢查 Breakout Bar 品質
                # 這裡簡化：只要最近結構符合 PowerPlay 特徵
                # 1. 最近 48H 有創新高
                # 2. 目前盤整旗形極窄
                flag_lookback = 24
                flag_width = (df['high'].iloc[-flag_lookback:].max() - df['low'].iloc[-flag_lookback:].min()) / df['atr'].iloc[-1]
                
                # Self-history flag width
                past_flag_widths = (df['high'].rolling(flag_lookback).max() - df['low'].rolling(flag_lookback).min()) / df['atr']
                flag_rank = (past_flag_widths.iloc[-24*60:] < flag_width).mean()
                
                if flag_rank < config.PP_FLAG_WIDTH_RANK and close_now > ema20_1h.iloc[-1]:
                    setup_pp = True
            
            if not (setup_vcp or setup_pp):
                continue
                
            # --- Step 6: Bucket & Filtering ---
            bucket = "None"
            if rs_rank > 85 and row['rs_trend_ok'] and (setup_vcp or setup_pp):
                bucket = "Leader"
            elif rs_rank > 80 and setup_pp:
                bucket = "PowerPlay"
            elif rs_rank > 60 and row['rs_trend_ok']: # Turning
                bucket = "Turning"
            
            # RS Hard Threshold Check (Turning allows lower)
            if bucket == "None":
                if rs_rank < config.RS_HARD_THRESHOLD * 100: continue
            
            # --- Step 7: Scoring ---
            # Normalize setup quality (lower width rank is better -> higher score)
            setup_q_score = (1.0 - best_width_rank) * 100
            trend_score = 100 if trend_gate_4h else 50
            
            total_score = (config.SCORE_WEIGHTS['RS'] * rs_rank + 
                           config.SCORE_WEIGHTS['Setup'] * setup_q_score + 
                           config.SCORE_WEIGHTS['Trend'] * trend_score)
            
            final_candidates.append({
                'symbol': symbol,
                'bucket': bucket,
                'rs_rank': round(rs_rank, 1),
                'score': round(total_score, 1),
                'price': close_now,
                'setup': 'PP' if setup_pp else 'VCP'
            })

        # Sort and Limit
        final_candidates.sort(key=lambda x: x['score'], reverse=True)
        return final_candidates[:config.MAX_OUTPUT]
