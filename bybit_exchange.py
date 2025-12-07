import asyncio
import ccxt.pro as ccxt  # Use the Pro (Async) version of CCXT
import time
from abc import abstractmethod
from typing import Dict, Optional
from arbitrage_market_data import ArbitrageMarketData
from base_exchange import BaseExchange

class BybitExchange(BaseExchange):    
    def __init__(self):
        super().__init__('bybit')
