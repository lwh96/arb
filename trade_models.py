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
    target_spread: float 
    funding_yield_bps: float
    score: float
    timestamp: float = field(default_factory=time.time)

@dataclass
class ActiveTrade:
    trade_id: str
    symbol: str
    long_exchange: str
    short_exchange: str
    
    # Target values from Signal
    target_entry_long: float
    target_entry_short: float
    
    # Execution values (Actual)
    exec_entry_long: float = 0.0
    exec_entry_short: float = 0.0
    
    size_amount: float = 0.0
    entry_spread: float = 0.0
    
    status: str = "PENDING"  # PENDING, OPEN, CLOSING, CLOSED
    entry_time: float = 0.0
    
    # PnL Tracking
    exit_price_long: float = 0.0
    exit_price_short: float = 0.0
    entry_fees: float = 0.0
    exit_fees: float = 0.0
    pnl_realized: float = 0.0
    net_pnl: float = 0.0

    expected_yield: float = 0.0
    actual_yield: float = 0.0
    
    def to_dict(self):
        return self.__dict__
    
    @staticmethod
    def from_dict(data):
        return ActiveTrade(**data)