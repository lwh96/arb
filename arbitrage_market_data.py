from dataclasses import dataclass

@dataclass
class ArbitrageMarketData:
    exchange: str
    symbol: str
    bid: float
    ask: float
    funding_rate: float
    next_funding_ts: int
    index_price: float
    mark_price: float
    base_volume: float
    quote_volume: float
    timestamp: float
    
    def is_valid(self) -> bool:
        # Exchange / symbol must exist
        if not self.exchange or not self.symbol or not self.bid or not self.ask or not self.funding_rate \
        or not self.next_funding_ts or not self.index_price or not self.mark_price \
        or not self.base_volume or not self.quote_volume or not self.timestamp:
            return False
        
        # Prices must be positive
        if self.bid <= 0 or self.ask <= 0:
            return False
        
        # Ask should not be lower than bid
        if self.ask < self.bid:
            return False
        
        # Funding timestamp must be positive (epoch)
        if self.next_funding_ts <= 0:
            return False
        
        # Index and mark price must be valid
        if self.index_price <= 0 or self.mark_price <= 0:
            return False
        
        # Volumes should not be negative
        if self.base_volume < 0 or self.quote_volume < 0:
            return False
        
        # Timestamp must be valid
        if self.timestamp <= 0:
            return False
        
        return True
