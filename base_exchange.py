import asyncio
import ccxt.pro as ccxt  # Use the Pro (Async) version of CCXT
import time
from typing import Dict, Optional
from arbitrage_market_data import ArbitrageMarketData
from logger_config import setup_logger

class BaseExchange:    
    def __init__(self, exchange_id: str):
        self.exchange_id = exchange_id
        self.logger = setup_logger("Engine")
        # Initialize CCXT exchange class dynamically
        self.exchange = getattr(ccxt, exchange_id)({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'swap',
                'defaultSubType': 'linear',
                # Increase tolerance for missed pongs (default is usually 10000ms)
                'keepAlive': 30000, 
                # Some exchanges allow configuring the ping interval
                'ws': {
                    'options': {
                        'keepAlive': 30000,
                    }
                }
            }
        })
        self.latest_data: Dict[str, ArbitrageMarketData] = {}
        self.symbols = []
        self.markets = []

    async def start(self, queue: asyncio.Queue):
        self.logger.info(f"[{self.exchange_id}] Connecting to WebSocket...")
        try:
            await self.load_swap_markets()
            # Parallelize fetching ticker and funding rate
            await asyncio.gather(
                self._watch_tickers(queue),
            
            )
        except Exception as e:
            self.logger.info(f"[{self.exchange_id}] Error: {e}")
        finally:
            await self.exchange.close()
    
    async def load_swap_markets(self):
        # Load markets via REST first to get precision/contract details
        markets = await self.exchange.load_markets()
        
        # Filter for USDT Linear Perps
        self.markets = {
            symbol: m for symbol, m in markets.items()
            if m.get('contract') 
            and m.get('type') == 'swap' 
            and m.get('subType') == 'linear' 
            and m.get('quote') == 'USDT'
            and m.get('active', True) # Only active markets
        } 
        self.symbols = list(self.markets.keys())
        self.logger.info(f"[{self.exchange_id}] Loaded {len(self.symbols)} swap markets.")

    async def _watch_tickers(self, queue: asyncio.Queue):
        """Watches the standard Ticker stream (for Bid/Ask and Volume) via WebSocket."""
        while True:
            try:
                tickers = await self.exchange.watch_tickers(self.symbols)
                # Initialize or update the data structure
                for symbol, ticker in tickers.items():
                    data = ArbitrageMarketData(
                            exchange=self.exchange_id,
                            symbol=symbol,
                            bid = ticker['bid'],
                            ask = ticker['ask'],
                            index_price = ticker['indexPrice'],
                            mark_price = ticker['markPrice'],
                            base_volume = ticker['baseVolume'],
                            quote_volume = ticker['quoteVolume'],
                            funding_rate = float(ticker['info']['fundingRate']),
                            next_funding_ts = int(ticker['info']['nextFundingTime']),
                            timestamp=time.time()
                        )
                    self.latest_data[symbol] = data
                    await queue.put(self.latest_data[symbol])
            except Exception as e:
                # Use the handler in Base to force close/reset
                await self._handle_socket_error(e)
    
    async def _handle_socket_error(self, e: Exception):
        """Force closes socket to reset state on error"""
        self.logger.error(f"[{self.exchange_id}] Socket Error: {type(e).__name__} - {e}")
        self.logger.error(f"[{self.exchange_id}] Resetting connection...")
        try:
            # CRITICAL: Must close to force a fresh handshake next loop
            await self.exchange.close()
        except:
            pass
        await asyncio.sleep(5) # Backoff

    def chunk(self, l, size):
        for i in range(0, len(l), size):
            yield l[i:i+size]