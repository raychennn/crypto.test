import os
import ccxt.async_support as ccxt
import pandas as pd
import asyncio
import logging
from config import MIN_HISTORY_DAYS, EXCLUDE_SYMBOLS

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        # 1. 從環境變數讀取 Key (Zeabur Variables)
        api_key = os.getenv('BINANCE_API_KEY')
        secret_key = os.getenv('BINANCE_SECRET_KEY')

        # 2. 設定 CCXT 基礎參數
        exchange_config = {
            'enableRateLimit': True,  # 必開，自動管理請求間隔
            'options': {
                'defaultType': 'future',        # 鎖定 USDT 合約市場
                'adjustForTimeDifference': True # 自動校正時間差，防止 Zeabur 報錯
            }
        }

        # 3. 判斷是否啟用驗證模式
        if api_key and secret_key:
            exchange_config['apiKey'] = api_key
            exchange_config['secret'] = secret_key
            logger.info("Using Authenticated Binance API (Higher Rate Limits).")
        else:
            logger.warning("⚠️ No API Keys found. Using Public API (Lower Rate Limits).")

        self.exchange = ccxt.binance(exchange_config)

    async def fetch_markets(self):
        """抓取所有可交易的 USDT 永續合約"""
        try:
            markets = await self.exchange.load_markets()
            symbols = []
            for symbol, info in markets.items():
                # 嚴格篩選條件
                if (info['quote'] == 'USDT' and 
                    info['contract'] and 
                    info['type'] == 'swap' and 
                    info['active'] and 
                    symbol not in EXCLUDE_SYMBOLS):
                    symbols.append(symbol)
            return symbols
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []

    async def fetch_ohlcv(self, symbol, timeframe='1h', limit=1500):
        """
        抓取單一幣種 K 線
        limit=1500 約等於 62 天 (24*62=1488)，滿足 60 天需求
        """
        try:
            # 異步抓取
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            if not ohlcv:
                return None
            
            # 轉換為 DataFrame
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # 資料長度檢查
            if len(df) < MIN_HISTORY_DAYS * 24:
                return None
                
            return df
        except Exception as e:
            # 捕捉錯誤但不中斷程式，只記錄 Warning
            logger.warning(f"Error fetching {symbol}: {e}")
            return None

    async def get_all_data(self):
        """主流程：並發抓取所有幣種數據"""
        try:
            symbols = await self.fetch_markets()
            
            # 確保 BTCUSDT 一定在清單中 (作為 Benchmark)
            # CCXT 的 symbol 格式通常是 'BTC/USDT:USDT'
            btc_found = False
            for s in symbols:
                if 'BTC/USDT' in s:
                    btc_found = True
                    break
            
            # 如果清單沒掃到 BTC (極少見)，手動加回去以防萬一
            if not btc_found:
                logger.info("Adding BTC/USDT:USDT to symbols manually for benchmark.")
                symbols.append('BTC/USDT:USDT')

            logger.info(f"Fetching data for {len(symbols)} symbols...")
            
            # 建立異步任務清單
            tasks = {symbol: self.fetch_ohlcv(symbol) for symbol in symbols}
            
            # 並發執行 (Gather)
            results = await asyncio.gather(*tasks.values(), return_exceptions=True)
            
            data_map = {}
            btc_data = None
            
            for symbol, result in zip(tasks.keys(), results):
                if isinstance(result, pd.DataFrame):
                    # 正規化 Symbol 名稱 (移除 CCXT 的後綴，變成 BTCUSDT 格式)
                    clean_sym = symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
                    
                    data_map[clean_sym] = result
                    
                    if clean_sym == 'BTCUSDT':
                        btc_data = result

            if btc_data is None:
                raise Exception("Critical: BTCUSDT data not found. Cannot run benchmark.")

            logger.info(f"Successfully loaded data for {len(data_map)} symbols.")
            return data_map, btc_data

        finally:
            # 確保連線關閉
            await self.exchange.close()
