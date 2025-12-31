import asyncio
import ccxt.pro as ccxt
import json
import csv
import os
import time
import logging
from typing import Dict, Optional, Tuple
from trade_models import TradeSignal, ActiveTrade
from opportunity import Opportunity
from arbitrage_engine import ArbitrageEngine
import uuid
from dotenv import load_dotenv
from logger_config import setup_logger

load_dotenv()

TRADES_FILE = "active_trades.json"
HISTORY_FILE = "trade_history.csv"
LEVERAGE = 5
PCT_EQUITY_PER_TRADE = 0.15
EXIT_NET_PROFIT_TARGET_BPS = 3.0 
ORDER_TIMEOUT_SEC = 20 
MIN_DEPLOYABLE = 20 
EXCHANGE_TAKER_FEES = {
    "binanceusdm": 0.00046,
    "bybit": 0.00055,
    "bitget": 0.00060,
    "default": 0.00060 
}

EXCHANGE_MAKER_FEES = {
    "binanceusdm": 0.00020,
    "bybit": 0.00020,
    "bitget": 0.00020,
    "default": 0.00020
}

class TradeManager:
    def __init__(self):
        self.active_trades: Dict[str, ActiveTrade] = {}
        self.clients: Dict[str, ccxt.Exchange] = {}
        self.logger = setup_logger("TradeManager")
        self.engine: ArbitrageEngine = None
        
        self.api_config = {
            "binanceusdm": {
                "apiKey": os.getenv("BINANCE_API_KEY"),
                "secret": os.getenv("BINANCE_SECRET"),
                "options": {"defaultType": "swap"}
            },
            "bybit": {
                "apiKey": os.getenv("BYBIT_API_KEY"),
                "secret": os.getenv("BYBIT_SECRET"),
                "options": {"defaultType": "swap"}
            },
            # "bitget": { ... }
        }
        
        self._validate_keys()
        self._init_exchanges()
        self._load_state()

    def _validate_keys(self):
        for exchange, config in self.api_config.items():
            if not config["apiKey"] or not config["secret"]:
                self.logger.warning(f"MISSING API KEYS for {exchange}.")
            if exchange == 'bitget' and not config.get('password'):
                 self.logger.warning("MISSING BITGET PASSPHRASE.")

    def _init_exchanges(self):
        for exchange_id, config in self.api_config.items():
            try:
                exchange_class = getattr(ccxt, exchange_id)
                self.clients[exchange_id] = exchange_class(config)
                self.logger.info(f"Initialized {exchange_id}")
            except Exception as e:
                self.logger.error(f"Failed to init {exchange_id}: {e}")

    async def run(self, signal_queue: asyncio.Queue, market_data_queue: asyncio.Queue, engine: ArbitrageEngine):
        self.logger.info("Trade Manager Started.")
        self.engine = engine
        while True:
            if not signal_queue.empty():
                signal: TradeSignal = await signal_queue.get()
                await self.process_signal(signal)
                signal_queue.task_done()

            if self.active_trades:
                await self.monitor_exits()

            await asyncio.sleep(1)

    async def process_signal(self, signal: TradeSignal):
        for trade in self.active_trades.values():
            if trade.symbol == signal.symbol and trade.status in ["PENDING", "OPEN"]:
                return 
        await self.execute_entry_strategy(signal)

    async def _place_order_with_retry(self, client, symbol, side, amount, price, params):
        """
        Attempts to place a Post-Only order. Handles both Exception-based rejections (Binance)
        and Status-based rejections (Bybit).
        Fetches fresh ticker on retry to ensure price is relevant.
        """
        retries = 2
        
        # Initial attempt uses the signal price
        current_price = price
        
        for attempt in range(retries + 1):
            try:
                order = None
                if side == 'buy':
                    order = await client.create_limit_buy_order(symbol, amount, current_price, params)
                else:
                    order = await client.create_limit_sell_order(symbol, amount, current_price, params)
                
                order = await client.fetch_order(order['id'], symbol, params={"acknowledged" : True})        
                # Check for Bybit-style immediate rejection (PostOnly kill)
                # Bybit status can be 'canceled' or 'rejected' immediately if PostOnly fails
                if order and order.get('status') in ['canceled', 'rejected', 'expired', None]:
                    # Raise a custom exception to trigger the retry block below
                    raise Exception(f"Order rejected immediately (Bybit style): {order.get('status')}")
                
                # If we get here and status is 'open' or 'closed', it succeeded
                return order

            except Exception as e:
                err_msg = str(e)
                # Check for Post-Only rejection signatures
                # Binance: "-5022", "Post Only"
                # Bybit (caught above): "rejected immediately"
                is_post_only_reject = "-5022" in err_msg or "Post Only" in err_msg or "rejected immediately" in err_msg
                
                if is_post_only_reject:
                    if attempt < retries:
                        self.logger.warning(f"Post-Only rejected for {symbol} {side} @ {current_price}. Retrying ({attempt+1}/{retries})...")
                        
                        # Fetch fresh ticker to get current Best Bid/Ask
                        try:
                            bid = self.engine.market_map[symbol][client.id].bid
                            ask = self.engine.market_map[symbol][client.id].ask
                            
                            # Get tick size for precision
                            market = client.market(symbol)
                            tick_size = market['precision']['price']
                            
                            # Adjust price based on current market to ensure Maker
                            # Buy: slightly below Best Ask (or at Best Bid)
                            # Sell: slightly above Best Bid (or at Best Ask)
                            
                            if side == 'buy':
                                # Try to match Best Bid (Maker)
                                # If spread is tight, Best Bid might still cross if market moved down
                                # Safer: Best Bid - 1 tick
                                target = bid - (tick_size * (attempt+ 1))
                                current_price = target
                            else:
                                # Sell at Best Ask
                                target = ask + (tick_size * (attempt+ 1))
                                current_price = target
                                
                            # Ensure formatted string for API
                            current_price = float(client.price_to_precision(symbol, current_price))
                            
                            self.logger.info(f"Retrying {side} with fresh market price: {current_price}")
                            continue 
                        except Exception as ticker_err:
                            self.logger.error(f"Failed to fetch ticker for retry: {ticker_err}")
                            # Fallback to simple tick adjustment on old price if ticker fetch fails
                            if side == 'buy': current_price -= 0.0001
                            else: current_price += 0.0001
                            continue

                # If non-recoverable error or retries exhausted
                self.logger.error(f"Placement Error ({symbol}): {e}")
                return None

    async def execute_entry_strategy(self, signal: TradeSignal):
        trade_id = str(uuid.uuid4())[:8]
        self.logger.info(f"[{trade_id}] PREPARING ENTRY: {signal.symbol}")

        long_client = self.clients.get(signal.long_exchange)
        short_client = self.clients.get(signal.short_exchange)
        if not long_client.markets:
            await long_client.load_markets()
            
        if not short_client.markets:
            await short_client.load_markets()

        if not long_client or not short_client:
            return
        tm_min = time.localtime().tm_min
        if tm_min < 50 or tm_min > 58:
            self.logger.info(f"[{trade_id}] SKIPPING ENTRY: {signal.symbol} EXCEED TIME WINDOW TM_MIN: {tm_min}")
            return

        size_amount = 0.0
        try:
            bal_long, bal_short = await asyncio.gather(long_client.fetch_balance(), 
                                                 short_client.fetch_balance())
            market_long = long_client.market(signal.symbol)
            market_short = short_client.market(signal.symbol)

            free_long = float(bal_long['USDT']['free'])
            free_short = float(bal_short['USDT']['free'])
            max_deployable = min(free_long, free_short) * PCT_EQUITY_PER_TRADE * LEVERAGE

            l_cost_min = market_long['limits']['cost']['min']
            l_cost_max = market_long['limits']['cost']['max']
            s_cost_min = market_short['limits']['cost']['min']
            s_cost_max = market_short['limits']['cost']['max']

            if max_deployable < MIN_DEPLOYABLE:
                self.logger.info(f"[{trade_id}] SKIPPING ENTRY: {signal.symbol} MAX DEPLOYABLE {max_deployable} < THRESHOLD: {MIN_DEPLOYABLE}")
                return
            if (l_cost_min and max_deployable < l_cost_min) or (s_cost_min and max_deployable < s_cost_min):
                self.logger.info(f"[{trade_id}] SKIPPING ENTRY: {signal.symbol} MAX DEPLOYABLE {max_deployable} < min cost")
                return
            if (l_cost_max and max_deployable > l_cost_max) or (s_cost_max and max_deployable > s_cost_max):
                self.logger.info(f"[{trade_id}] SKIPPING ENTRY: {signal.symbol} MAX DEPLOYABLE {max_deployable} > max cost")
                return            
            
            raw_size = max_deployable / signal.entry_price_long
            size_amount = float(long_client.amount_to_precision(signal.symbol, raw_size))
            l_amount_min = market_long['limits']['amount']['min']
            l_amount_max = market_long['limits']['amount']['max']
            s_amount_min = market_short['limits']['amount']['min']
            s_amount_max = market_short['limits']['amount']['max']

            if (l_amount_min and size_amount < l_amount_min)  or (s_amount_min and size_amount < s_amount_min):
                self.logger.info(f"[{trade_id}] SKIPPING ENTRY: {signal.symbol} SIZE {size_amount} < MIN amount")
                return
            if (l_amount_max and size_amount > l_amount_max) or (s_amount_max and size_amount > s_amount_max):
                self.logger.info(f"[{trade_id}] SKIPPING ENTRY: {signal.symbol} SIZE {size_amount} > MAX amount")
                return            
        except Exception as e:
            self.logger.error(f"[{trade_id}] Sizing Error: {e}")
            return

        self.logger.info(f"[{trade_id}] Placing MAKER orders for {size_amount} {signal.symbol}...")
        params_maker = {'postOnly': True}

        try:
            # Parallel placement with individual retry logic
            t1 = self._place_order_with_retry(long_client, signal.symbol, 'buy', size_amount, signal.entry_price_long, params_maker)
            t2 = self._place_order_with_retry(short_client, signal.symbol, 'sell', size_amount, signal.entry_price_short, params_maker)
            
            results = await asyncio.gather(t1, t2)
            order_long, order_short = results
            
        except Exception as e:
            self.logger.error(f"[{trade_id}] System Error during placement: {e}")
            return

        # --- RECOVERY LOGIC ---
        
        # 1. Both Failed Placement
        if order_long is None and order_short is None:
            self.logger.error(f"[{trade_id}] Both orders failed placement. Aborting.")
            return
        
        # 2. Long Failed, Short Placed (Danger)
        if order_long is None:
            self.logger.critical(f"[{trade_id}] Long failed placement, Short placed. CANCELLING SHORT immediately.")
            await self._cancel_and_hedge(short_client, order_short['id'], signal.symbol, 'sell', size_amount)
            return

        # 3. Short Failed, Long Placed (Danger)
        if order_short is None:
            self.logger.critical(f"[{trade_id}] Short failed placement, Long placed. CANCELLING LONG immediately.")
            await self._cancel_and_hedge(long_client, order_long['id'], signal.symbol, 'buy', size_amount)
            return

        # 4. Both Placed Successfully -> Wait for Fills
        self.logger.info(f"[{trade_id}] Orders placed (L:{order_long['id']} S:{order_short['id']}). Waiting 20s for fills...")
        
        # FIX: _wait_for_fills now returns independent statuses
        filled_long, filled_short = await self._wait_for_fills(
            trade_id, long_client, short_client, 
            order_long['id'], order_short['id'], signal.symbol
        )

        final_price_l = 0.0
        final_price_s = 0.0

        if filled_long and filled_short:
            self.logger.info(f"[{trade_id}] SUCCESS. Both legs filled.")
            await self._finalize_entry(trade_id, signal, size_amount, order_long['id'], order_short['id'])
        
        elif filled_long and not filled_short:
            self.logger.critical(f"[{trade_id}] LEGGED! Long filled, Short pending/canceled. Canceling Short & Hedging...")
            
            hedge_price = await self._cancel_and_hedge(short_client, order_short['id'], signal.symbol, 'sell', size_amount)
            
            o_l = await long_client.fetch_order(order_long['id'], signal.symbol, params={"acknowledged" : True})
            final_price_l = float(o_l.get('average') or o_l.get('price'))
            final_price_s = hedge_price 
            
            if final_price_s > 0:
                entry_fee = (EXCHANGE_MAKER_FEES[signal.long_exchange] + EXCHANGE_TAKER_FEES[signal.short_exchange]) * 10_000
                self._register_trade_manual(trade_id, signal, size_amount, final_price_l, final_price_s, entry_fee, "OPEN (HEDGED)")
            
        elif filled_short and not filled_long:
            self.logger.critical(f"[{trade_id}] LEGGED! Short filled, Long pending/canceled. Canceling Long & Hedging...")
            
            hedge_price = await self._cancel_and_hedge(long_client, order_long['id'], signal.symbol, 'buy', size_amount)
            
            o_s = await short_client.fetch_order(order_short['id'], signal.symbol, params={"acknowledged" : True})
            final_price_s = float(o_s.get('average') or o_s.get('price'))
            final_price_l = hedge_price 
            
            if final_price_l > 0:
                entry_fee = (EXCHANGE_TAKER_FEES[signal.long_exchange] + EXCHANGE_MAKER_FEES[signal.short_exchange]) * 10_000
                self._register_trade_manual(trade_id, signal, size_amount, final_price_l, final_price_s, entry_fee,"OPEN (HEDGED)")

        else:
            self.logger.warning(f"[{trade_id}] Timeout. Both orders unfilled (or canceled). Canceling both.")
            await self._cancel_order_safe(long_client, order_long['id'], signal.symbol)
            await self._cancel_order_safe(short_client, order_short['id'], signal.symbol)

    async def _wait_for_fills(self, trade_id, client_l, client_s, id_l, id_s, symbol) -> Tuple[bool, bool]:
        """
        Polls status. 
        Returns True if 'closed' (filled).
        Returns False if 'open', 'canceled', 'rejected', or timeout.
        Logic updated: If one cancels, we DO NOT return immediately. We wait for the other to finish or timeout.
        """
        start_ts = time.time()
        long_filled = False
        short_filled = False
        
        long_done = False # Filled OR Canceled
        short_done = False # Filled OR Canceled
        
        while time.time() - start_ts < ORDER_TIMEOUT_SEC:
            # Check Long
            if not long_done:
                try:
                    o_l = await client_l.fetch_order(id_l, symbol, params={"acknowledged" : True})
                    status = o_l['status']
                    if status == 'closed': 
                        long_filled = True
                        long_done = True
                    elif status in ['canceled', 'rejected', 'expired']:
                        long_filled = False
                        long_done = True # Stop checking this one
                except Exception as e:
                    self.logger.error(f"[{trade_id}] Error checking Long: {e}")
            
            # Check Short
            if not short_done:
                try:
                    o_s = await client_s.fetch_order(id_s, symbol, params={"acknowledged" : True})
                    status = o_s['status']
                    if status == 'closed': 
                        short_filled = True
                        short_done = True
                    elif status in ['canceled', 'rejected', 'expired']:
                        short_filled = False
                        short_done = True
                except Exception as e:
                    self.logger.error(f"[{trade_id}] Error checking Short: {e}")

            # If both are in a final state (Filled or Canceled), we can stop waiting
            if long_done and short_done:
                return (long_filled, short_filled)
            
            await asyncio.sleep(1)
            
        # Timeout reached. Return whatever positive state we captured.
        return (long_filled, short_filled)

    async def _cancel_and_hedge(self, client, order_id, symbol, side, amount) -> float:
        """
        Cancel pending order, then market execute.
        Returns the average fill price of the hedge order.
        """
        await self._cancel_order_safe(client, order_id, symbol)
        
        self.logger.warning(f"Hedging {side} {amount} {symbol}...")
        try:
            order = None
            if side == 'buy': 
                order = await client.create_market_buy_order(symbol, amount)
            else: 
                order = await client.create_market_sell_order(symbol, amount)
            id = order.get('id')
            order = await client.fetch_order(id, symbol, params={"acknowledged" : True})        
            self.logger.info("Hedge Executed.")
            
            if order:
                return float(order.get('average') or order.get('price') or 0.0)
            return 0.0
            
        except Exception as e:
            self.logger.critical(f"HEDGE FAILED: {e}")
            return 0.0

    async def _cancel_order_safe(self, client, order_id, symbol):
        try:
            await client.cancel_order(order_id, symbol)
        except Exception:
            pass 

    def _register_trade_manual(self, trade_id, signal, size, entry_l, entry_s, entry_fees_paid, status="OPEN"):
        trade = ActiveTrade(
            trade_id=trade_id,
            symbol=signal.symbol,
            long_exchange=signal.long_exchange,
            short_exchange=signal.short_exchange,
            target_entry_long=signal.entry_price_long,
            target_entry_short=signal.entry_price_short,
            exec_entry_long=entry_l,
            exec_entry_short=entry_s,
            size_amount=size,
            entry_spread=signal.target_spread,
            entry_fees=entry_fees_paid,
            expected_yield=signal.funding_yield_bps,
            status=status,
            entry_time=time.time()
        )
        self.active_trades[trade_id] = trade
        self._save_state()
        self._log_to_csv(trade, "ENTRY")

    async def _finalize_entry(self, trade_id, signal, size, id_l, id_s):
        long_client = self.clients[signal.long_exchange]
        short_client = self.clients[signal.short_exchange]
        
        try:
            o_l = await long_client.fetch_order(id_l, signal.symbol, params={"acknowledged" : True})
            o_s = await short_client.fetch_order(id_s, signal.symbol, params={"acknowledged" : True})
            
            real_entry_l = float(o_l.get('average') or o_l.get('price'))
            real_entry_s = float(o_s.get('average') or o_s.get('price'))
            
            entry_fee = (EXCHANGE_MAKER_FEES[signal.long_exchange] + EXCHANGE_MAKER_FEES[signal.short_exchange]) * 10_000
            self._register_trade_manual(trade_id, signal, size, real_entry_l, real_entry_s, entry_fee)
            
        except Exception as e:
            self.logger.error(f"[{trade_id}] Finalize Error: {e}")

    async def monitor_exits(self):
        for t_id, trade in list(self.active_trades.items()):
            if not trade.status.startswith("OPEN"): continue
            tm_min = time.localtime().tm_min
            if  5 <= tm_min < 50 :
                return
            try:
                long_client = self.clients[trade.long_exchange]
                short_client = self.clients[trade.short_exchange]
                
                
                exit_bid_long = self.engine.market_map[trade.symbol][long_client.id].bid
                exit_ask_short = self.engine.market_map[trade.symbol][short_client.id].ask
                
                pnl_long_pct = (exit_bid_long - trade.exec_entry_long) / trade.exec_entry_long
                pnl_short_pct = (trade.exec_entry_short - exit_ask_short) / trade.exec_entry_short
                gross_pnl_bps = (pnl_long_pct + pnl_short_pct) * 10000
                
                exit_fee_bps = (EXCHANGE_TAKER_FEES[long_client.id] + EXCHANGE_TAKER_FEES[short_client.id]) * 10000
                net_pnl_bps = gross_pnl_bps - exit_fee_bps
                
                time_held = time.time() - trade.entry_time
                
                if net_pnl_bps > EXIT_NET_PROFIT_TARGET_BPS:
                    self.logger.info(f"[{t_id}] TARGET HIT (Net PnL: {net_pnl_bps:.1f} bps). Closing...")
                    await self.close_trade(trade)
                
                elif time_held > 3000: 
                    self.logger.info(f"[{t_id}] TIME LIMIT. Closing...")
                    await self.close_trade(trade)

            except Exception as e:
                self.logger.error(f"Monitor error {t_id}: {e}")

    async def close_trade(self, trade: ActiveTrade):
        self.logger.info(f"[{trade.trade_id}] Closing...")
        long_client = self.clients[trade.long_exchange]
        short_client = self.clients[trade.short_exchange]
        params = {'reduceOnly': True}
        
        try:
            t1 = long_client.create_market_sell_order(trade.symbol, trade.size_amount, params)
            t2 = short_client.create_market_buy_order(trade.symbol, trade.size_amount, params)
            
            o_l, o_s = await asyncio.gather(t1, t2)
            id_l = o_l.get('id')
            id_s = o_s.get('id')
            o_l, o_s, f_l, f_s = await asyncio.gather(long_client.fetch_order(id_l, trade.symbol, params={"acknowledged" : True}),
                                             short_client.fetch_order(id_s, trade.symbol, params={"acknowledged" : True}),
                                             long_client.fetch_funding_rate_history('PLUME/USDT:USDT', None, 1),
                                             short_client.fetch_funding_rate_history('PLUME/USDT:USDT', None, 1))
            
            net_yield = abs(abs(f_l[0]['fundingRate'])- abs(f_s[0]['fundingRate'])) * 10_000
            
            trade.exit_price_long = float(o_l.get('average') or o_l.get('price'))
            trade.exit_price_short = float(o_s.get('average') or o_s.get('price'))
            
            pnl_l = (trade.exit_price_long - trade.exec_entry_long) * trade.size_amount
            pnl_s = (trade.exec_entry_short - trade.exit_price_short) * trade.size_amount
            trade.exit_fees = (EXCHANGE_TAKER_FEES[long_client.id] + EXCHANGE_TAKER_FEES[short_client.id]) * 10000
            trade.actual_yield= net_yield
            trade.pnl_realized = pnl_l + pnl_s
            trade.net_pnl = (net_yield * trade.size_amount) + trade.pnl_realized - trade.exit_fees - trade.entry_fees
            trade.status = "CLOSED"
            
            del self.active_trades[trade.trade_id]
            self._save_state()
            self._log_to_csv(trade, "EXIT")
            
            self.logger.info(f"[{trade.trade_id}] CLOSED. PnL: ${trade.pnl_realized:.5f}")
            
        except Exception as e:
            self.logger.critical(f"[{trade.trade_id}] CLOSE FAILED: {e}")

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
            except Exception: pass

    def _log_to_csv(self, trade: ActiveTrade, action: str):
        file_exists = os.path.isfile(HISTORY_FILE)
        with open(HISTORY_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Time', 'ID', 'Action', 'Symbol', 'Exp. Yield','LongEx', 'ShortEx', 'Size', 'EntryL', 'EntryS', 'EntryFee', 'ExitL', 'ExitS', 'ExitFee', 'PnL', 'Actual Yield', 'Net PNL'])
            writer.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), trade.trade_id, action, trade.symbol, trade.expected_yield, trade.long_exchange, trade.short_exchange, trade.size_amount, trade.exec_entry_long, trade.exec_entry_short, trade.entry_fees, trade.exit_price_long, trade.exit_price_short, trade.exit_fees, trade.pnl_realized, trade.actual_yield, trade.net_pnl])

async def test():
    data_queue = asyncio.Queue()
    exec_queue = asyncio.Queue()
    engine = ArbitrageEngine(execution_queue=exec_queue) 
    trader = TradeManager()  
    opp = Opportunity(
        symbol='ANIME/USDT:USDT',
        long_exchange='binanceusdm',
        short_exchange='bybit',
        gross_yield_bps=3.88, fees_bps=15.5,
        entry_spread_bps=42.29, net_profit_bps=30.67,
        liquidity_score=0.92, mark_divergence_bps=43.35,
        time_to_funding_min=2.3, 
        earliest_ts=1766667600000, 
        final_score=18.3,
        ask_long=0.008513,
        bid_short=0.008549)  
    
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
    await exec_queue.put(signal)
    await trader.run(exec_queue, data_queue, engine)

if __name__ == '__main__':
        asyncio.run(test())