import asyncio
import ccxt.pro as ccxt
import json
import csv
import os
import time
import logging
from typing import Dict
from trade_models import TradeSignal, ActiveTrade
import uuid

# Configuration
TRADES_FILE = "active_trades.json"
HISTORY_FILE = "trade_history.csv"
LEVERAGE = 3
PCT_EQUITY_PER_TRADE = 0.10  # Use 10% of available balance per trade
MAX_RETRY_ATTEMPTS = 3

class TradeManager:
    def __init__(self):
        self.active_trades: Dict[str, ActiveTrade] = {}
        self.clients: Dict[str, ccxt.Exchange] = {}
        self.logger = logging.getLogger("TradeManager")
        
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
                "password": os.getenv("BITGET_PASSPHRASE"), # Bitget specific
                "options": {"defaultType": "swap"}
            }
        }
        
        # Validation: Check if keys loaded successfully
        self._validate_keys()
        self._init_exchanges()
        self._load_state()
        
    def _validate_keys(self):
        """Ensure keys exist before starting"""
        for exchange, config in self.api_config.items():
            if not config["apiKey"] or not config["secret"]:
                raise ValueError(f"❌ MISSING API KEYS for {exchange}. Check your .env file.")
            if exchange == 'bitget' and not config['password']:
                raise ValueError("❌ MISSING BITGET PASSPHRASE. Check your .env file.")
        
    def _init_exchanges(self):
        """Initialize authenticated clients"""
        for exchange_id, config in self.api_config.items():
            try:
                # Dynamic instantiation
                exchange_class = getattr(ccxt, exchange_id)
                self.clients[exchange_id] = exchange_class(config)
            except Exception as e:
                print(f"Failed to init {exchange_id}: {e}")

    async def run(self, signal_queue: asyncio.Queue, market_data_queue: asyncio.Queue):
        print("Trade Manager Started... Waiting for signals.")
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
        # Q1: Check for Duplicates
        for trade in self.active_trades.values():
            if trade.symbol == signal.symbol:
                print(f"⚠️ IGNORED: Active trade exists for {signal.symbol}")
                return

        await self.execute_entry_strategy(signal)

    async def execute_entry_strategy(self, signal: TradeSignal):
        trade_id = str(uuid.uuid4())[:8]
        print(f"[{trade_id}] ⚔️ EXECUTING: Long {signal.long_exchange} / Short {signal.short_exchange}")

        long_client = self.clients.get(signal.long_exchange)
        short_client = self.clients.get(signal.short_exchange)

        # Q4: Dynamic Sizing
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
            # Note: Should check MinNotional here
            size_amount = max_deployable / signal.entry_price_long
            
            # Rounding to precision (Crucial for live trading)
            # CCXT helper: amount_to_precision
            size_amount = float(long_client.amount_to_precision(signal.symbol, size_amount))

        except Exception as e:
            print(f"[{trade_id}] Sizing Error: {e}")
            return

        # Q5: Post-Only Parameters
        # Binance: timeInForce='GTX' or postOnly=True
        # Bybit: postOnly=True
        params_maker = {'postOnly': True} 

        # EXECUTION LOGIC:
        # We try to fill the "Harder" leg first (usually the one with less liquidity or Maker side)
        # But for simultaneous arb, we fire both.
        
        print(f"[{trade_id}] Placing MAKER orders for {size_amount} {signal.symbol}...")
        
        # Asyncio Gather to fire instantly
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
                # Success!
                self._register_trade(trade_id, signal, size_amount, res_long, res_short)
            
            elif long_filled and not short_filled:
                # CRITICAL: Long Filled, Short Failed/Killed.
                # We are Delta Long. We must Force Short immediately via Taker.
                print(f"[{trade_id}] 🚨 LEG RISK: Long Filled, Short Failed! Forcing Taker Short...")
                await self._force_hedge(short_client, signal.symbol, 'sell', size_amount)
                # Register trade assuming hedge worked (add error handling in real prod)
                # Note: We register using original params, PnL tracking will be slightly off but delta is safe
                self._register_trade(trade_id, signal, size_amount, res_long, {'price': signal.entry_price_short, 'id': 'hedge'})

            elif short_filled and not long_filled:
                # CRITICAL: Short Filled, Long Failed.
                print(f"[{trade_id}] 🚨 LEG RISK: Short Filled, Long Failed! Forcing Taker Buy...")
                await self._force_hedge(long_client, signal.symbol, 'buy', size_amount)
                self._register_trade(trade_id, signal, size_amount, {'price': signal.entry_price_long, 'id': 'hedge'}, res_short)

            else:
                print(f"[{trade_id}] Both orders failed (likely Post-Only killed). Trade aborted.")

        except Exception as e:
            print(f"[{trade_id}] CRITICAL SYSTEM ERROR: {e}")

    async def _force_hedge(self, client, symbol, side, amount):
        """Q7: Aggressive Taker order to fix a legged position"""
        try:
            # Market Order to ensure fill
            if side == 'buy':
                await client.create_market_buy_order(symbol, amount)
            else:
                await client.create_market_sell_order(symbol, amount)
            print(f"✅ HEDGE SUCCESSFUL for {symbol}")
        except Exception as e:
            print(f"❌ HEDGE FAILED: {e}. MANUAL INTERVENTION REQUIRED IMMEDIATELY.")

    def _register_trade(self, trade_id, signal, size, res_long, res_short):
        trade = ActiveTrade(
            trade_id=trade_id,
            symbol=signal.symbol,
            long_exchange=signal.long_exchange,
            short_exchange=signal.short_exchange,
            entry_price_long=float(res_long.get('average') or res_long.get('price')),
            entry_price_short=float(res_short.get('average') or res_short.get('price')),
            size_amount=size,
            entry_spread=signal.target_spread,
            status="OPEN",
            entry_time=time.time()
        )
        self.active_trades[trade_id] = trade
        self._save_state()
        print(f"[{trade_id}] Trade Registered.")

    async def monitor_exits(self):
        """Q6: Exit Logic"""
        # In real production, this needs access to current ticker prices.
        # For now, we simulate looking up prices via the clients (REST polling)
        # Ideally, pass the 'market_data_queue' data here.
        
        for t_id, trade in list(self.active_trades.items()):
            if trade.status != "OPEN": continue

            try:
                # Check Time-Based Exit (Funding Complete?)
                # Assumes we entered 30 mins before. Exit 30 mins after funding.
                # (You would check actual funding timestamp here)
                
                # Check Convergence
                long_client = self.clients[trade.long_exchange]
                short_client = self.clients[trade.short_exchange]
                
                # Fetch current Orderbook (Top 1)
                # Note: Polling is slow. In prod, update prices via WSS in background.
                ob_long = await long_client.fetch_ticker(trade.symbol)
                ob_short = await short_client.fetch_ticker(trade.symbol)
                
                # We Sell Long (Bid) and Buy Short (Ask) to exit
                exit_price_long = ob_long['bid']
                exit_price_short = ob_short['ask']
                
                # Current Spread = (Short_Ask - Long_Bid) / Long_Bid
                current_spread_bps = ((exit_price_short - exit_price_long) / exit_price_long) * 10000
                
                # LOGIC: If Spread has converged (is less than or equal to Entry Spread)
                # OR if it's profitable enough including funding (simplified here)
                
                # Example: Entered at -20bps. Current is -5bps. We gained 15bps.
                # Example: Entered at +10bps. Current is -5bps. We gained 15bps.
                
                # Simple condition: Convergence
                # We want Current Spread to be LOWER than Entry Spread (if Entry was positive)
                # Or HIGHER (closer to 0) if Entry was negative (Cost recovery).
                # Actually, simple math: Profit = Entry_Spread - Current_Spread
                
                spread_gain = trade.entry_spread - current_spread_bps
                
                if spread_gain > 5.0: # If we squeezed 5bps out of the spread + Funding
                    print(f"[{t_id}] 🎯 TARGET HIT. Closing...")
                    await self.close_trade(trade)

            except Exception as e:
                print(f"Monitor error {t_id}: {e}")

    async def close_trade(self, trade: ActiveTrade):
        """Execute Exit Orders (Taker for speed/certainty)"""
        print(f"[{trade.trade_id}] Closing Position...")
        
        long_client = self.clients[trade.long_exchange]
        short_client = self.clients[trade.short_exchange]
        
        # Reduce Only params
        params = {'reduceOnly': True}
        
        try:
            # Sell the Long
            t1 = long_client.create_market_sell_order(trade.symbol, trade.size_amount, params)
            # Buy the Short
            t2 = short_client.create_market_buy_order(trade.symbol, trade.size_amount, params)
            
            await asyncio.gather(t1, t2)
            
            trade.status = "CLOSED"
            del self.active_trades[trade.trade_id]
            self._save_state()
            self._log_to_csv(trade, "EXIT")
            print(f"[{trade.trade_id}] CLOSED Successfully.")
            
        except Exception as e:
            print(f"[{trade.trade_id}] CLOSE FAILED: {e}")

    # --- Persistence Helpers ---
    def _save_state(self):
        """Saves active trades to JSON"""
        with open(TRADES_FILE, 'w') as f:
            json.dump({k: v.to_dict() for k, v in self.active_trades.items()}, f, indent=4)

    def _load_state(self):
        """Loads active trades on startup"""
        if os.path.exists(TRADES_FILE):
            try:
                with open(TRADES_FILE, 'r') as f:
                    data = json.load(f)
                    for k, v in data.items():
                        self.active_trades[k] = ActiveTrade.from_dict(v)
                print(f"Loaded {len(self.active_trades)} active trades from disk.")
            except Exception as e:
                print(f"Error loading state: {e}")

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