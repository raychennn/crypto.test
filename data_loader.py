import os
import ccxt.async_support as ccxt
import pandas as pd
import asyncio
import logging
from config import MIN_HISTORY_DAYS, EXCLUDE_SYMBOLS

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        # 1. 從環境變數讀取 Key
        api_key = os.getenv('BINANCE_API_KEY')
        secret_key = os.getenv('BINANCE_SECRET_KEY')

        # 2. 設定 CCXT 基礎參數
        exchange_config = {
            'enableRateLimit': True,  
            'options': {
                'defaultType': 'future',
                'adjustForTimeDifference': True
            }
        }

        if api_key and secret_key:
            exchange_config['apiKey'] = api_key
            exchange_config['secret'] = secret_key
        else:
            logger.warning("⚠️ No API Keys found. Using Public API.")

        self.exchange = ccxt.binance(exchange_config)

    async def fetch_markets(self):
        """抓取市場清單 (加入防禦性 .get)"""
        try:
            markets = await self.exchange.load_markets()
            symbols = []
            for symbol, info in markets.items():
                # 使用 .get() 避免 KeyError
                quote = info.get('quote')
                is_contract = info.get('contract')
                type_ = info.get('type')
                is_active = info.get('active')

                if (quote == 'USDT' and 
                    is_contract and 
                    type_ == 'swap' and 
                    is_active and 
                    symbol not in EXCLUDE_SYMBOLS):
                    symbols.append(symbol)
            return symbols
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []

    async def fetch_ohlcv(self, symbol, semaphore, timeframe='1h', limit=1500):
        """
        抓取 K 線 (加入 Semaphore 限制併發數量)
        並在回傳前設定 DatetimeIndex
        """
        async with semaphore:  # 限制同時連線數
            try:
                # 稍微 sleep 一下讓 event loop 有喘息空間
                await asyncio.sleep(0.05) 
                
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                if not ohlcv:
                    return None
                
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                
                # [關鍵修正] 設定 Index
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df = df.set_index('timestamp').sort_index()
                
                # 資料長度檢查
                if len(df) < MIN_HISTORY_DAYS * 24:
                    return None
                    
                return df
            except Exception as e:
                logger.warning(f"Error fetching {symbol}: {e}")
                return None

    async def get_all_data(self):
        try:
            symbols = await self.fetch_markets()
            
            # 確保 BTCUSDT
            btc_found = False
            for s in symbols:
                if 'BTC/USDT' in s:
                    btc_found = True
                    break
            
            if not btc_found:
                logger.info("Adding BTC/USDT:USDT manually.")
                symbols.append('BTC/USDT:USDT')

            logger.info(f"Fetching data for {len(symbols)} symbols with Semaphore(10)...")
            
            # [關鍵修正] 使用 Semaphore 限制併發
            # Zeabur 免費/輕量方案建議 5-10，避免瞬間記憶體爆炸
            sem = asyncio.Semaphore(10)
            
            tasks = [self.fetch_ohlcv(symbol, sem) for symbol in symbols]
            
            # 執行所有任務
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            data_map = {}
            btc_data = None
            
            # 因為 tasks 是 list，results 會按順序對應 symbols
            for symbol, result in zip(symbols, results):
                if isinstance(result, pd.DataFrame):
                    clean_sym = symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
                    data_map[clean_sym] = result
                    
                    if clean_sym == 'BTCUSDT':
                        btc_data = result

            if btc_data is None:
                raise Exception("Critical: BTCUSDT data not found.")

            logger.info(f"Successfully loaded {len(data_map)} symbols.")
            return data_map, btc_data

        finally:
            await self.exchange.close()
