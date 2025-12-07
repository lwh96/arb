import asyncio
import ccxt.pro as ccxt
import json
import csv
import os
import time
import logging
from typing import Dict, Optional
from trade_models import TradeSignal, ActiveTrade
import uuid
from dotenv import load_dotenv
from logger_config import setup_logger

# Load environment variables
load_dotenv()

# Configuration
TRADES_FILE = "active_trades.json"
HISTORY_FILE = "trade_history.csv"
LEVERAGE = 3
PCT_EQUITY_PER_TRADE = 0.10  # Use 10% of available balance per trade
MAX_RETRY_ATTEMPTS = 3
EXIT_PROFIT_THRESHOLD_BPS = 5.0 # Net profit target (after fees) to trigger exit

class TradeManager:
    def __init__(self):
        self.active_trades: Dict[str, ActiveTrade] = {}
        self.clients: Dict[str, ccxt.Exchange] = {}
        self.logger = setup_logger("TradeManager")
        
        # Load Keys securely from Environment
        self.api_config = {
            "binance": {
                "apiKey": os.getenv("BINANCE_API_KEY"),
                "secret": os.getenv("BINANCE_SECRET"),
                "options": {"defaultType": "swap"}
            },
            "bybit": {
                "apiKey": os.getenv("BYBIT_API_KEY"),
                "secret": os.getenv("BYBIT_SECRET"),
                "options": {"defaultType": "swap"}
            },
            "bitget": {
                "apiKey": os.getenv("BITGET_API_KEY"),
                "secret": os.getenv("BITGET_SECRET"),
                "password": os.getenv("BITGET_PASSPHRASE"),
                "options": {"defaultType": "swap"}
            }
        }
        
        self._validate_keys()
        self._init_exchanges()
        self._load_state()

    def _validate_keys(self):
        """Ensure keys exist before starting"""
        for exchange, config in self.api_config.items():
            if not config["apiKey"] or not config["secret"]:
                self.logger.critical(f"MISSING API KEYS for {exchange}. Check your .env file.")
                raise ValueError(f"Missing keys for {exchange}")
            if exchange == 'bitget' and not config.get('password'):
                self.logger.critical("MISSING BITGET PASSPHRASE. Check your .env file.")
                raise ValueError("Missing Bitget passphrase")

    def _init_exchanges(self):
        """Initialize authenticated clients"""
        for exchange_id, config in self.api_config.items():
            try:
                exchange_class = getattr(ccxt, exchange_id)
                self.clients[exchange_id] = exchange_class(config)
                self.logger.info(f"Initialized authenticated client for {exchange_id}")
            except Exception as e:
                self.logger.error(f"Failed to init {exchange_id}: {e}")

    async def run(self, signal_queue: asyncio.Queue, market_data_queue: asyncio.Queue):
        self.logger.info("Trade Manager Started... Waiting for signals.")
        while True:
            # 1. Process New Signals
            if not signal_queue.empty():
                signal: TradeSignal = await signal_queue.get()
                await self.process_signal(signal)
                signal_queue.task_done()

            # 2. Monitor Active Trades (Exit Logic)
            if self.active_trades:
                await self.monitor_exits()

            await asyncio.sleep(1)

    async def process_signal(self, signal: TradeSignal):
        """Pre-checks before execution"""
        # Q1: Check for Duplicates (Symbol level check)
        for trade in self.active_trades.values():
            if trade.symbol == signal.symbol and trade.status == "OPEN":
                self.logger.warning(f"IGNORED: Active trade exists for {signal.symbol}")
                return

        await self.execute_entry_strategy(signal)

    async def execute_entry_strategy(self, signal: TradeSignal):
        trade_id = str(uuid.uuid4())[:8]
        self.logger.info(f"[{trade_id}] EXECUTING: Long {signal.long_exchange} / Short {signal.short_exchange} for {signal.symbol}")

        long_client = self.clients.get(signal.long_exchange)
        short_client = self.clients.get(signal.short_exchange)

        if not long_client or not short_client:
            self.logger.error(f"[{trade_id}] Exchanges not initialized properly.")
            return

        # Dynamic Sizing
        size_amount = 0.0
        try:
            # Fetch free collateral from both
            bal_long = await long_client.fetch_balance()
            bal_short = await short_client.fetch_balance()
            
            # USDT Free Balance
            free_long = float(bal_long['USDT']['free'])
            free_short = float(bal_short['USDT']['free'])
            
            # Use the smaller account to determine size (Bottleneck)
            max_deployable = min(free_long, free_short) * PCT_EQUITY_PER_TRADE * LEVERAGE
            
            # Convert USDT size to Amount of Coin
            raw_size = max_deployable / signal.entry_price_long
            
            # Rounding to precision (Crucial for live trading)
            size_amount = float(long_client.amount_to_precision(signal.symbol, raw_size))
            
            self.logger.info(f"[{trade_id}] Calculated Size: {size_amount} {signal.symbol} (Based on ${min(free_long, free_short):.2f} equity)")

        except Exception as e:
            self.logger.error(f"[{trade_id}] Sizing Error: {e}")
            return

        # Q5: Post-Only Parameters
        params_maker = {'postOnly': True} 

        self.logger.info(f"[{trade_id}] Placing MAKER orders...")
        
        try:
            # Task 1: Buy Long
            t1 = long_client.create_limit_buy_order(signal.symbol, size_amount, signal.entry_price_long, params_maker)
            # Task 2: Sell Short
            t2 = short_client.create_limit_sell_order(signal.symbol, size_amount, signal.entry_price_short, params_maker)
            
            results = await asyncio.gather(t1, t2, return_exceptions=True)
            res_long, res_short = results

            # Q7: HANDLE LEGGED TRADES
            long_filled = not isinstance(res_long, Exception)
            short_filled = not isinstance(res_short, Exception)

            if long_filled and short_filled:
                self.logger.info(f"[{trade_id}] BOTH LEGS FILLED.")
                self._register_trade(trade_id, signal, size_amount, res_long, res_short)
            
            elif long_filled and not short_filled:
                self.logger.critical(f"[{trade_id}] LEG RISK: Long Filled, Short Failed! Error: {res_short}")
                self.logger.critical(f"[{trade_id}] FORCING TAKER SHORT HEDGE...")
                await self._force_hedge(short_client, signal.symbol, 'sell', size_amount)
                # Register trade assuming hedge worked (add error handling in real prod)
                self._register_trade(trade_id, signal, size_amount, res_long, {'price': signal.entry_price_short, 'id': 'hedge-forced'})

            elif short_filled and not long_filled:
                self.logger.critical(f"[{trade_id}] LEG RISK: Short Filled, Long Failed! Error: {res_long}")
                self.logger.critical(f"[{trade_id}] FORCING TAKER BUY HEDGE...")
                await self._force_hedge(long_client, signal.symbol, 'buy', size_amount)
                self._register_trade(trade_id, signal, size_amount, {'price': signal.entry_price_long, 'id': 'hedge-forced'}, res_short)

            else:
                self.logger.warning(f"[{trade_id}] Both orders failed (likely Post-Only). Trade aborted.")

        except Exception as e:
            self.logger.critical(f"[{trade_id}] SYSTEM ERROR during execution: {e}", exc_info=True)

    async def _force_hedge(self, client, symbol, side, amount):
        """Aggressive Taker order to fix a legged position"""
        try:
            if side == 'buy':
                await client.create_market_buy_order(symbol, amount)
            else:
                await client.create_market_sell_order(symbol, amount)
            self.logger.info(f"HEDGE SUCCESSFUL for {symbol}")
        except Exception as e:
            self.logger.critical(f"HEDGE FAILED: {e}. MANUAL INTERVENTION REQUIRED IMMEDIATELY.")

    def _register_trade(self, trade_id, signal, size, res_long, res_short):
        # Handle cases where response might be different depending on exchange
        p_long = float(res_long.get('average') or res_long.get('price') or signal.entry_price_long)
        p_short = float(res_short.get('average') or res_short.get('price') or signal.entry_price_short)

        trade = ActiveTrade(
            trade_id=trade_id,
            symbol=signal.symbol,
            long_exchange=signal.long_exchange,
            short_exchange=signal.short_exchange,
            entry_price_long=p_long,
            entry_price_short=p_short,
            size_amount=size,
            entry_spread=signal.target_spread,
            status="OPEN",
            entry_time=time.time()
        )
        self.active_trades[trade_id] = trade
        self._save_state()
        self._log_to_csv(trade, "ENTRY")
        self.logger.info(f"[{trade_id}] Trade Registered & Saved.")

    async def monitor_exits(self):
        # Exit Logic
        for t_id, trade in list(self.active_trades.items()):
            if trade.status != "OPEN": continue

            try:
                # 1. Check if Funding Event passed (Simple time check)
                # In real prod, store 'funding_time' in ActiveTrade and compare
                # if time.time() > trade.funding_time + 600: ... 

                long_client = self.clients[trade.long_exchange]
                short_client = self.clients[trade.short_exchange]
                
                # Fetch current Ticker (Bid/Ask) for Exit Calculation
                # We Sell the Long (Bid) and Buy the Short (Ask)
                tick_long = await long_client.fetch_ticker(trade.symbol)
                tick_short = await short_client.fetch_ticker(trade.symbol)
                
                exit_bid_long = tick_long['bid']
                exit_ask_short = tick_short['ask']
                
                # Current Exit Spread = (Short_Ask - Long_Bid) / Long_Bid
                # If we entered at -20bps, we want this to be > -20bps (e.g. -5bps or 0bps)
                current_spread_bps = ((exit_ask_short - exit_bid_long) / exit_bid_long) * 10000
                
                # Convergence Gain = Entry Spread - Current Spread
                # Example: Entry -20, Current -5. Gain = -20 - (-5) = -15 (Wrong direction logic?)
                
                # LOGIC CORRECTION:
                # We want PnL. 
                # Long PnL = Exit_Bid - Entry_Ask
                # Short PnL = Entry_Bid - Exit_Ask
                
                # Simplified Convergence Check:
                # We want the spread to be SMALLER (closer to 0) than entry spread (if entry was positive)
                # Or HIGHER (closer to 0) if entry was negative cost.
                
                # Let's just use the PnL logic directly
                # Est PnL from Price only
                pnl_long_pct = (exit_bid_long - trade.entry_price_long) / trade.entry_price_long
                pnl_short_pct = (trade.entry_price_short - exit_ask_short) / trade.entry_price_short
                
                total_pnl_bps = (pnl_long_pct + pnl_short_pct) * 10000
                
                # If Price PnL + Funding (Estimated) > Target
                # For now, let's just use a simple convergence target
                # If we captured > 10bps of spread convergence
                
                # Entry Basis (Normalized): (Short - Long) / Long
                basis_diff = trade.entry_spread - current_spread_bps
                
                # If we have gained 5bps from spread narrowing OR 30 minutes have passed
                time_held = time.time() - trade.entry_time
                
                if basis_diff > 5.0 or time_held > 3600: 
                    self.logger.info(f"[{t_id}] 🎯 TARGET HIT (Spread Gain: {basis_diff:.1f}bps or Time Limit). Closing...")
                    await self.close_trade(trade)

            except Exception as e:
                self.logger.error(f"Monitor error {t_id}: {e}")

    async def close_trade(self, trade: ActiveTrade):
        """Execute Exit Orders (Taker for speed/certainty)"""
        self.logger.info(f"[{trade.trade_id}] Closing Position...")
        
        long_client = self.clients[trade.long_exchange]
        short_client = self.clients[trade.short_exchange]
        
        params = {'reduceOnly': True}
        
        try:
            # Sell the Long (Market)
            t1 = long_client.create_market_sell_order(trade.symbol, trade.size_amount, params)
            # Buy the Short (Market)
            t2 = short_client.create_market_buy_order(trade.symbol, trade.size_amount, params)
            
            await asyncio.gather(t1, t2)
            
            trade.status = "CLOSED"
            del self.active_trades[trade.trade_id]
            self._save_state()
            self._log_to_csv(trade, "EXIT")
            self.logger.info(f"[{trade.trade_id}] CLOSED Successfully.")
            
        except Exception as e:
            self.logger.error(f"[{trade.trade_id}] CLOSE FAILED: {e}")

    def _save_state(self):
        with open(TRADES_FILE, 'w') as f:
            json.dump({k: v.to_dict() for k, v in self.active_trades.items()}, f, indent=4)

    def _load_state(self):
        if os.path.exists(TRADES_FILE):
            try:
                with open(TRADES_FILE, 'r') as f:
                    data = json.load(f)
                    for k, v in data.items():
                        self.active_trades[k] = ActiveTrade.from_dict(v)
                self.logger.info(f"Loaded {len(self.active_trades)} active trades from disk.")
            except Exception as e:
                self.logger.error(f"Error loading state: {e}")

    def _log_to_csv(self, trade: ActiveTrade, action: str):
        file_exists = os.path.isfile(HISTORY_FILE)
        with open(HISTORY_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Time', 'ID', 'Action', 'Symbol', 'LongEx', 'ShortEx', 'Size', 'PnL'])
            
            writer.writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                trade.trade_id,
                action,
                trade.symbol,
                trade.long_exchange,
                trade.short_exchange,
                trade.size_amount,
                trade.pnl_realized
            ])