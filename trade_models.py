from dataclasses import dataclass, field
from typing import Optional
import time
import uuid

@dataclass
class TradeSignal:
    symbol: str
    long_exchange: str
    short_exchange: str
    entry_price_long: float
    entry_price_short: float
    target_spread: float  # The spread difference we want to capture
    funding_yield_bps: float
    score: float
    timestamp: float = field(default_factory=time.time)

@dataclass
class ActiveTrade:
    trade_id: str
    symbol: str
    long_exchange: str
    short_exchange: str
    entry_price_long: float
    entry_price_short: float
    size_amount: float     # Amount of coin (e.g. 0.1 BTC)
    entry_spread: float
    status: str            # 'OPEN', 'CLOSING', 'CLOSED'
    entry_time: float
    pnl_realized: float = 0.0
    
    # For persistence
    def to_dict(self):
        return self.__dict__
    
    @staticmethod
    def from_dict(data):
        return ActiveTrade(**data)