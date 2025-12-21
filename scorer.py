import asyncio
import time
from datetime import datetime
import pandas as pd
import numpy as np
from typing import Dict, List, Any
from dataclasses import asdict
from opportunity import Opportunity
from arbitrage_market_data import ArbitrageMarketData

# --- Configuration ---
EXCHANGE_TAKER_FEES = {
    "binance": 0.00046,
    "bybit": 0.00055,
    "bitget": 0.00060,
    "default": 0.00060 
}

EXCHANGE_MAKER_FEES = {
    "binance": 0.00018,
    "bybit": 0.00020,
    "bitget": 0.00020,
    "default": 0.00020
}
# Thresholds
MIN_SCORE_THRESHOLD = 5.0       # Lowered from 10.0 because Net Profit calculation is now stricter (includes Taker fees)
MIN_VOLUME_USD = 500_000        
MIN_PROFIT_BPS = 2.0            # Minimum hard profit required after ALL fees

class FundingArbitrageScorer:
    """
    Vectorized Scorer returning Typed Opportunity Objects.
    Logic: Total Return = Funding + Entry Spread - (Maker Entry + Taker Exit Fees)
    """

    def score_opportunities(self, market_data: List[ArbitrageMarketData]) -> List[Opportunity]:
        if not market_data:
            return []

        # 1. Ingest Data
        df = pd.DataFrame([asdict(d) for d in market_data])
        
        # 2. Hard Filters
        current_time = time.time()
        df = df[
            (df['timestamp'] > ((current_time - 600) * 1000)) & 
            (df['next_funding_ts'] > current_time) &
            (df['quote_volume'] > MIN_VOLUME_USD)
        ].copy()

        if df.empty: return []

        # 3. Create Pairs
        pairs = pd.merge(df, df, on='symbol', suffixes=('_L', '_S'))
        pairs = pairs[pairs['exchange_L'] != pairs['exchange_S']].copy()
        
        if pairs.empty: return []

        # 4. Effective Funding Logic
        pairs['earliest_ts'] = np.minimum(pairs['next_funding_ts_L'], pairs['next_funding_ts_S'])
        pairs['eff_FR_L'] = np.where(pairs['next_funding_ts_L'] == pairs['earliest_ts'], pairs['funding_rate_L'], 0.0)
        pairs['eff_FR_S'] = np.where(pairs['next_funding_ts_S'] == pairs['earliest_ts'], pairs['funding_rate_S'], 0.0)

        # 5. Calculate Metrics
        # Gross Yield
        pairs['gross_yield_bps'] = (pairs['eff_FR_S'] - pairs['eff_FR_L']) * 10_000
        
        # Filter: We only care about positive yield direction? 
        # Actually, if Spread is massive (+50bps) and Funding is -5bps, it's still a profitable trade.
        # But for a "Funding Bot", we usually prefer Funding > 0. 
        # Let's keep the filter lenient: Total PnL > 0 is what matters.
        
        # Fee Calculation (REALISTIC: Maker Entry + Taker Exit)
        def get_maker(ex_series):
            return ex_series.map(EXCHANGE_MAKER_FEES).fillna(EXCHANGE_MAKER_FEES['default'])
        def get_taker(ex_series):
            return ex_series.map(EXCHANGE_TAKER_FEES).fillna(EXCHANGE_TAKER_FEES['default'])
        
        # Fee = (Maker_L + Maker_S) + (Taker_L + Taker_S)
        entry_fees_bps = (get_maker(pairs['exchange_L']) + get_maker(pairs['exchange_S'])) * 10_000
        exit_fees_bps = (get_taker(pairs['exchange_L']) + get_taker(pairs['exchange_S'])) * 10_000
        pairs['fees_bps'] = entry_fees_bps + exit_fees_bps

        # Entry Spread (The "Buffer")
        # Bid_S - Ask_L. If Positive, we sell high buy low (Profit).
        pairs['entry_spread_bps'] = ((pairs['bid_S'] - pairs['ask_L']) / pairs['ask_L']) * 10_000

        # Mark Divergence (Risk)
        avg_mark = (pairs['mark_price_L'] + pairs['mark_price_S']) / 2
        pairs['mark_divergence_bps'] = ((pairs['mark_price_L'] - pairs['mark_price_S']).abs() / avg_mark) * 10_000

        # 6. TOTAL RETURN SCORING (The Fix)
        # Net Profit = Yield + Spread - All Fees
        pairs['est_net_profit_bps'] = pairs['gross_yield_bps'] + pairs['entry_spread_bps'] - pairs['fees_bps']
        
        # Strict Profit Filter
        pairs = pairs[pairs['est_net_profit_bps'] > MIN_PROFIT_BPS].copy()
        if pairs.empty: return []

        # Liquidity Score
        pairs['min_vol'] = np.minimum(pairs['quote_volume_L'], pairs['quote_volume_S'])
        vol_log = np.log10(pairs['min_vol'])
        pairs['liquidity_score'] = ((vol_log - 5.0) / 2.5).clip(0.1, 1.2)

        # Final Score Logic
        # We penalize high mark divergence (noise risk)
        # We DO NOT double-count spread (it's already in Net Profit)
        
        basis_penalty = pairs['mark_divergence_bps'] * 0.25  # Reduced penalty weight (was 0.5)
        
        raw_score = (pairs['est_net_profit_bps'] - basis_penalty) * pairs['liquidity_score']
        pairs['final_score'] = raw_score.clip(0, 100)
        
        pairs = pairs[pairs['final_score'] >= MIN_SCORE_THRESHOLD].copy()
        pairs = pairs.sort_values(by='final_score', ascending=False)

        # 7. Convert to List[Opportunity]
        results = []
        for _, row in pairs.iterrows():
            opp = Opportunity(
                symbol=row['symbol'],
                long_exchange=row['exchange_L'],
                short_exchange=row['exchange_S'],
                gross_yield_bps=round(float(row['gross_yield_bps']), 2),
                fees_bps=round(float(row['fees_bps']), 2),
                entry_spread_bps=round(float(row['entry_spread_bps']), 2),
                net_profit_bps=round(float(row['est_net_profit_bps']), 2),
                liquidity_score=round(float(row['liquidity_score']), 2),
                mark_divergence_bps=round(float(row['mark_divergence_bps']), 2),
                time_to_funding_min=round((float(row['earliest_ts']) - current_time) / 60, 1), # Fixed: was /60_000 but timestamp is seconds
                earliest_ts=int(row['earliest_ts']),
                final_score=round(float(row['final_score']), 1),
                ask_long=float(row['ask_L']),
                bid_short=float(row['bid_S'])
            )
            results.append(opp)
            
        return results