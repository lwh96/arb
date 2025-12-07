import asyncio
import time
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
# Assuming this is imported from your file
from arbitrage_market_data import ArbitrageMarketData
from scorer import FundingArbitrageScorer
from opportunity import Opportunity
from trade_models import TradeSignal


# --- The Strategy Engine ---
class ArbitrageEngine:
    def __init__(self):
        # Map: Symbol -> Exchange -> MarketData
        self.market_map: Dict[str, Dict[str, ArbitrageMarketData]] = {}
        self.scorer = FundingArbitrageScorer()
        
        # Global Opportunities Dictionary
        # Key: "Symbol_LongEx_ShortEx" (e.g., "BTCUSDT_binance_bybit")
        # Value: Dict containing the score result
        self.opportunities: Dict[str, Opportunity] = {}

    async def process_data(self, queue: asyncio.Queue):
        print("Strategy Engine Started...")
        while True:
            data: ArbitrageMarketData = await queue.get()
            if data.is_valid():
                # 1. Update Internal State (In-Memory Database)
                if data.symbol not in self.market_map:
                    self.market_map[data.symbol] = {}
                self.market_map[data.symbol][data.exchange] = data
            
                # 2. Check for Arbitrage on this specific symbol
                await self.find_opportunities(data.symbol)
                
                # 3. Print Dashboard (Throttle this in production)
                self.print_dashboard()
                
            queue.task_done()

    async def find_opportunities(self, symbol: str):
        current_time = time.time()
        
        # Extract snapshots
        exchanges_data = list(self.market_map[symbol].values())
        
        if len(exchanges_data) >= 2:
            # Returns List[Opportunity]
            results: List[Opportunity] = self.scorer.score_opportunities(exchanges_data)

            # --- Update Global State ---
            symbol_keys = [k for k in self.opportunities.keys() if k.startswith(f"{symbol}_")]
            
            if not results:
                # Clear stale
                for k in symbol_keys: del self.opportunities[k]
            else:
                found_keys = set()
                for opp in results:
                    key = f"{opp.symbol}_{opp.long_exchange}_{opp.short_exchange}"
                    self.opportunities[key] = opp
                    found_keys.add(key)
                    
                    # CHECK FOR EXECUTION
                    if self.execution_queue and opp.final_score >= 20.0:
                         await self._trigger_execution(opp)
                
                # Cleanup outdated keys for this symbol
                for k in symbol_keys:
                    if k not in found_keys: del self.opportunities[k]
        
        # Global Cleanup: Remove expired
        keys_to_remove = [k for k, opp in self.opportunities.items() if opp.earliest_ts <= current_time]
        for k in keys_to_remove: del self.opportunities[k]

    async def _trigger_execution(self, opp: Opportunity):
        """Create a signal using Object Attributes"""
        if opp.symbol in self.cooldowns:
            if time.time() - self.cooldowns[opp.symbol] < 600: return
        
        signal = TradeSignal(
            symbol=opp.symbol,
            long_exchange=opp.long_exchange,
            short_exchange=opp.short_exchange,
            entry_price_long=opp.ask_long,   # Using the field we added
            entry_price_short=opp.bid_short, # Using the field we added
            target_spread=opp.entry_spread_bps,
            funding_yield_bps=opp.gross_yield_bps,
            score=opp.final_score
        )
        
        print(f"🚀 SENDING SIGNAL: {signal.symbol} Score {signal.score}")
        await self.execution_queue.put(signal)
        self.cooldowns[opp.symbol] = time.time()

    def print_dashboard(self):
        # Sort by .final_score attribute
        sorted_opps = sorted(
            self.opportunities.values(), 
            key=lambda x: x.final_score, 
            reverse=True
        )

        print(f"\n--- ⚡ LIVE DELTA NEUTRAL OPPORTUNITIES (Top 20 of {len(sorted_opps)}) ---")
        if not sorted_opps:
            print("No opportunities meeting criteria...")
            return

        print(f"{'SYM':<12} {'PAIR':<12} {'SCORE':<6} {'NET BPS':<8} {'SPREAD':<8} {'LIQ':<4} {'TIME':<6}")
        print("-" * 75)
        
        for opp in sorted_opps[:20]:
            pair_str = f"{opp.long_exchange[0:3].upper()}/{opp.short_exchange[0:3].upper()}"
            spread_str = f"{opp.entry_spread_bps:+.1f}"
            
            print(
                f"{opp.symbol:<12} "
                f"{pair_str:<12} "
                f"{opp.final_score:<6.1f} "
                f"{opp.net_profit_bps:<8.1f} "
                f"{spread_str:<8} "
                f"{opp.liquidity_score:<4.2f} "
                f"{opp.time_to_funding_min:<6.1f}m"
            )
        print("-" * 75)