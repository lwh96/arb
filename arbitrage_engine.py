import asyncio
import time
from typing import Dict, List, Optional
from arbitrage_market_data import ArbitrageMarketData
from scorer import FundingArbitrageScorer
from opportunity import Opportunity
from trade_models import TradeSignal
from logger_config import setup_logger

DASHBOARD_INTERVAL = 60  # Seconds between dashboard refreshes

class ArbitrageEngine:
    def __init__(self, execution_queue: asyncio.Queue = None):
        self.logger = setup_logger("Engine")
        self.market_map: Dict[str, Dict[str, ArbitrageMarketData]] = {}
        self.scorer = FundingArbitrageScorer()
        self.opportunities: Dict[str, Opportunity] = {}
        self.execution_queue = execution_queue
        self.cooldowns = {} 

    async def process_data(self, queue: asyncio.Queue):
        self.logger.info("Strategy Engine Started...")
        while True:
            data: ArbitrageMarketData = await queue.get()
            if data.is_valid():
                if data.symbol not in self.market_map:
                    self.market_map[data.symbol] = {}
                self.market_map[data.symbol][data.exchange] = data
            
                # Await, but offload CPU work inside find_opportunities
                await self.find_opportunities(data.symbol)
                
            queue.task_done()

    async def dashboard_loop(self):
        """
        Runs in the background and prints the dashboard every X seconds.
        """
        self.logger.info("Dashboard Loop Started...")
        while True:
            await asyncio.sleep(DASHBOARD_INTERVAL)
            self.print_dashboard()

    async def find_opportunities(self, symbol: str):
        current_time = time.time()
        exchanges_data = list(self.market_map[symbol].values())
        
        if len(exchanges_data) >= 2:
            try:
                # Offload to thread to prevent blocking event loop
                results: List[Opportunity] = await asyncio.to_thread(
                    self.scorer.score_opportunities, 
                    exchanges_data
                )
            except Exception as e:
                self.logger.error(f"Scoring Error: {e}")
                return

            symbol_keys = [k for k in self.opportunities.keys() if k.startswith(f"{symbol}_")]
            
            if not results:
                for k in symbol_keys: del self.opportunities[k]
            else:
                found_keys = set()
                for opp in results:
                    key = f"{opp.symbol}_{opp.long_exchange}_{opp.short_exchange}"
                    self.opportunities[key] = opp
                    found_keys.add(key)
                    
                    if self.execution_queue and opp.final_score >= 10.0:
                         await self._trigger_execution(opp)
                
                for k in symbol_keys:
                    if k not in found_keys: del self.opportunities[k]
        
        keys_to_remove = [k for k, opp in self.opportunities.items() if opp.earliest_ts <= current_time]
        for k in keys_to_remove: del self.opportunities[k]

    async def _trigger_execution(self, opp: Opportunity):
        if opp.symbol in self.cooldowns:
            if time.time() - self.cooldowns[opp.symbol] < 600: return
        
        signal = TradeSignal(
            symbol=opp.symbol,
            long_exchange=opp.long_exchange,
            short_exchange=opp.short_exchange,
            entry_price_long=opp.ask_long,
            entry_price_short=opp.bid_short,
            target_spread=opp.entry_spread_bps,
            funding_yield_bps=opp.gross_yield_bps,
            score=opp.final_score
        )
        
        self.logger.info(f"SIGNAL: {signal.symbol} | Score {signal.score} | Yield {signal.funding_yield_bps}bps")
        await self.execution_queue.put(signal)
        self.cooldowns[opp.symbol] = time.time()

    def print_dashboard(self):
        # Sort by .final_score attribute
        sorted_opps = sorted(
            self.opportunities.values(), 
            key=lambda x: x.final_score, 
            reverse=True
        )
        if not sorted_opps:
            return
        
        print(f"\n--- ⚡ LIVE DELTA NEUTRAL OPPORTUNITIES (Top 20 of {len(sorted_opps)}) ---")
        print(f"{'SYM':<12} {'PAIR':<12} {'SCORE':<6} {'NET BPS':<8} {'SPREAD':<8} {'LIQ':<4} {'TIME':<6}")
        print("-" * 75)
        
        for opp in sorted_opps[:20]:
            pair_str = f"{opp.long_exchange[0:3].upper()}/{opp.short_exchange[0:3].upper()}"
            spread_str = f"{opp.entry_spread_bps:+.1f}"
            
            print(opp)
            print("-" * 75)