import asyncio
import ccxt.pro as ccxt
import time
from arbitrage_market_data import ArbitrageMarketData
from base_exchange import BaseExchange

class BitgetExchange(BaseExchange):    
    def __init__(self):
        super().__init__('bitget')
    
    async def load_swap_markets(self):
        """
        Bitget specific market loading.
        """
        try:
            # We use the main self.exchange just for loading markets (REST API)
            markets = await self.exchange.load_markets()
            
            self.markets = {
                symbol: m for symbol, m in markets.items()
                if m.get('swap')                 
                and m.get('contract')            
                and (m.get('linear') is True or m.get('settle') == 'USDT') 
                and m.get('quote') == 'USDT'     
                and m.get('active', True)       
            } 
            self.symbols = list(self.markets.keys())
            self.logger.info(f"[{self.exchange_id}] Successfully loaded {len(self.symbols)} USDT perp markets.")
            
        except Exception as e:
            self.logger.error(f"[{self.exchange_id}] Failed to load markets: {e}")
            self.symbols = []

    async def _watch_tickers(self, queue: asyncio.Queue):
        if not self.symbols:
            await self.load_swap_markets()
            if not self.symbols:
                return

        # WORKER: Creates its OWN exchange instance for true isolation
        async def watch_chunk_isolated(chunk_symbols, chunk_id):
            # STAGGER: Wait before starting to avoid 429 rate limits
            await asyncio.sleep(chunk_id * 2.0)
            
            self.logger.info(f"[{self.exchange_id}] Chunk #{chunk_id} initializing isolated connection ({len(chunk_symbols)} symbols)...")
            
            # CRITICAL: Create a FRESH instance for this chunk
            # This forces a distinct TCP/WebSocket connection
            chunk_exchange = ccxt.bitget({
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'swap',
                    'defaultSubType': 'linear',
                    'keepAlive': 60000, # Very generous timeout (60s)
                }
            })

            try:
                while True:
                    try:
                        tickers = await chunk_exchange.watch_tickers(chunk_symbols)
                        
                        for symbol, ticker in tickers.items():
                            # We can use the global self.markets metadata
                            # provided the symbol keys match
                            if symbol not in self.markets: continue
                            
                            info = ticker.get('info', {})
                            
                            # Safe Parsing
                            data = ArbitrageMarketData(
                                exchange=self.exchange_id,
                                symbol=symbol,
                                bid=float(ticker.get('bid') or 0),
                                ask=float(ticker.get('ask') or 0),
                                index_price=float(info.get('indexPrice') or ticker.get('indexPrice') or 0),
                                mark_price=float(info.get('markPrice') or ticker.get('markPrice') or 0),
                                base_volume=float(ticker.get('baseVolume') or 0),
                                quote_volume=float(ticker.get('quoteVolume') or 0),
                                funding_rate=float(info.get('fundingRate') or ticker.get('fundingRate') or 0),
                                next_funding_ts=int(info.get('nextFundingTime') or ticker.get('nextFundingTime') or 0),
                                timestamp=time.time()
                            )
                            
                            await queue.put(data)

                    except Exception as e:
                        self.logger.error(f"[{self.exchange_id}] Chunk #{chunk_id} Error: {e}. Reconnecting isolated socket...")
                        # Close the isolated instance to reset state
                        try:
                            await chunk_exchange.close()
                        except:
                            pass
                        await asyncio.sleep(5)
            
            finally:
                # Cleanup if task is cancelled
                await chunk_exchange.close()

        # 3. Execution
        # Reduce chunk size to 40 to be safer
        CHUNK_SIZE = 50
        chunks = list(self.chunk(self.symbols, CHUNK_SIZE))
        
        self.logger.info(f"[{self.exchange_id}] Spawning {len(chunks)} ISOLATED connections...")

        tasks = []
        for i, c in enumerate(chunks):
            tasks.append(watch_chunk_isolated(c, i))

        await asyncio.gather(*tasks)