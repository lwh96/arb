import asyncio
import ccxt.pro as ccxt  # Use the Pro (Async) version of CCXT
import logging
import time
from typing import Dict, Optional
from arbitrage_market_data import ArbitrageMarketData


# --- The Strategy Engine ---
class ArbitrageEngine:
    def __init__(self):
        self.market_map: Dict[str, Dict[str, ArbitrageMarketData]] = {}

    async def process_data(self, queue: asyncio.Queue):
        print("Strategy Engine Started...")
        while True:
            data: ArbitrageMarketData = await queue.get()
            
            # 1. Update Internal State
            if data.symbol not in self.market_map:
                self.market_map[data.symbol] = {}
            self.market_map[data.symbol][data.exchange] = data

            # 2. Check for Arbitrage
            await self.find_opportunities(data.symbol)
            
            queue.task_done()