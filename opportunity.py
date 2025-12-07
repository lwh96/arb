import asyncio
import ccxt.pro as ccxt  # Use the Pro (Async) version of CCXT
import logging
import time
from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class Opportunity:
    symbol: str
    long_exchange: str
    short_exchange: str
    gross_yield_bps: float
    fees_bps: float
    entry_spread_bps: float
    net_profit_bps: float
    liquidity_score: float
    mark_divergence_bps: float
    time_to_funding_min: float
    earliest_ts: int
    final_score: float
    ask_long: float
    bid_short: float