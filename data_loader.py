import ccxt.async_support as ccxt
import pandas as pd
import asyncio
import logging
from config import MIN_HISTORY_DAYS, EXCLUDE_SYMBOLS

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })

    async def fetch_markets(self):
        try:
            markets = await self.exchange.load_markets()
            symbols = []
            for symbol, info in markets.items():
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
        # 1500 hours approx 62 days, covering the 60 days requirement
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            if not ohlcv:
                return None
            
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # 確保資料長度足夠
            if len(df) < MIN_HISTORY_DAYS * 24:
                return None
                
            return df
        except Exception as e:
            logger.warning(f"Error fetching {symbol}: {e}")
            return None

    async def get_all_data(self):
        symbols = await self.fetch_markets()
        # 務必抓取 BTCUSDT 作為 Benchmark
        if 'BTC/USDT:USDT' not in symbols and 'BTC/USDT' not in symbols:
            # Handle ccxt symbol naming, ensuring BTC is included
            pass 

        logger.info(f"Fetching data for {len(symbols)} symbols...")
        
        tasks = {symbol: self.fetch_ohlcv(symbol) for symbol in symbols}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        data_map = {}
        btc_data = None
        
        for symbol, result in zip(tasks.keys(), results):
            if isinstance(result, pd.DataFrame):
                # Normalize symbol name for consistency
                clean_sym = symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
                data_map[clean_sym] = result
                if clean_sym == 'BTCUSDT':
                    btc_data = result

        if btc_data is None:
            raise Exception("Critical: BTCUSDT data not found for benchmark.")

        await self.exchange.close()
        return data_map, btc_data
