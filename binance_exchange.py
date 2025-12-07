import asyncio
import ccxt.pro as ccxt  # Use the Pro (Async) version of CCXT
import time
from typing import List
from arbitrage_market_data import ArbitrageMarketData
from base_exchange import BaseExchange

class BinanceExchange(BaseExchange):    
    def __init__(self):
        super().__init__('binanceusdm')

    async def watch_bids_asks_stream(self, queue: asyncio.Queue):
        while True:
            try:
                tickers = await self.exchange.watch_bids_asks()  # global stream
                for symbol, t in tickers.items():
                    if symbol not in self.symbols:
                        continue
                    d = self.latest_data.get(symbol)
                    if d:
                        d.bid = t['bid']
                        d.ask = t['ask']
                        d.timestamp = time.time()
                    else:
                        self.latest_data[symbol] = ArbitrageMarketData(
                            exchange = self.exchange_id,
                            symbol = symbol,
                            bid = t['bid'],
                            ask = t['ask'],
                            base_volume = None,
                            quote_volume = None,
                            funding_rate = None,
                            next_funding_ts = None,
                            mark_price = None,
                            index_price = None,
                            timestamp = time.time()
                        )
                    await queue.put(self.latest_data[symbol])
            except Exception as e:
                # Handle errors for this specific stream
                print(f"watch_bids_asks_stream error - {e}. Retrying in 5s...")
                # Use the handler in Base to force close/reset
                await self._handle_socket_error(e)

    async def watch_tickers_stream(self, queue: asyncio.Queue):
        while True:
            try:
                tickers = await self.exchange.watch_tickers()
                for symbol, t in tickers.items():
                    if symbol not in self.symbols:
                        continue
                    d = self.latest_data.get(symbol)
                    if d:
                        d.base_volume = t['baseVolume']
                        d.quote_volume = t['quoteVolume']
                        d.timestamp = time.time()
                    else:
                        self.latest_data[symbol] = ArbitrageMarketData(
                            exchange=self.exchange_id,
                            symbol=symbol,
                            bid=None,
                            ask=None,
                            base_volume = t['baseVolume'],
                            quote_volume = t['quoteVolume'],
                            funding_rate = None,
                            next_funding_ts = None,
                            mark_price = None,
                            index_price = None,
                            timestamp=time.time()
                        )
                    await queue.put(self.latest_data[symbol])
            except Exception as e:
                # Handle errors for this specific stream
                print(f"watch_tickers_stream error - {e}. Retrying in 5s...")
                # Use the handler in Base to force close/reset
                await self._handle_socket_error(e)

    async def watch_mark_prices_stream(self, queue: asyncio.Queue):
        while True:
            try:
                tickers = await self.exchange.watch_mark_prices()
                for symbol, t in tickers.items():
                    if symbol not in self.symbols:
                        continue
                    info = t['info']
                    d = self.latest_data.get(symbol)
                    if d:
                        d.funding_rate = float(info['r'])
                        d.next_funding_ts = float(info['T'])
                        d.mark_price = float(info['p'])
                        d.index_price = float(info['i'])
                        d.timestamp = time.time()
                    else:
                        self.latest_data[symbol] = ArbitrageMarketData(
                            exchange = self.exchange_id,
                            symbol = symbol,
                            bid = None,
                            ask = None,
                            base_volume = None,
                            quote_volume = None,
                            funding_rate = float(info['r']),
                            next_funding_ts = int(info['T']),
                            mark_price = float(info['p']),
                            index_price = float(info['i']),
                            timestamp = time.time()
                        )
                    await queue.put(self.latest_data[symbol])
            except Exception as e:
                # Handle errors for this specific stream
                print(f"watch_mark_prices_stream error - {e}. Retrying in 5s...")
                # Use the handler in Base to force close/reset
                await self._handle_socket_error(e)



    async def _watch_tickers(self, queue: asyncio.Queue):
        # Loop forever listening for updates
        while True:
            try:
                # Watch multiple symbols at once
                # 3. Create a list of concurrent tasks
                tasks = []
                tasks.append(self.watch_mark_prices_stream(queue))
                tasks.append(self.watch_bids_asks_stream(queue))
                tasks.append(self.watch_tickers_stream(queue))
                    
                # 4. Run all tasks concurrently
                await asyncio.gather(*tasks)
            except Exception as e:
                print(f"[{self.exchange_id}] Ticker Stream Error: {e}")
                await asyncio.sleep(5) # Backoff
