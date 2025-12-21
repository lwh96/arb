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

load_dotenv()

TRADES_FILE = "active_trades.json"
HISTORY_FILE = "trade_history.csv"
LEVERAGE = 3
PCT_EQUITY_PER_TRADE = 0.10  
MAX_RETRY_ATTEMPTS = 3
EXIT_NET_PROFIT_TARGET_BPS = 2.0 

class TradeManager:
    def __init__(self):
        self.active_trades: Dict[str, ActiveTrade] = {}
        self.clients: Dict[str, ccxt.Exchange] = {}
        self.logger = setup_logger("TradeManager")
        
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
        for exchange, config in self.api_config.items():
            if not config["apiKey"] or not config["secret"]:
                self.logger.warning(f"MISSING API KEYS for {exchange}. Execution will fail.")
            if exchange == 'bitget' and not config.get('password'):
                 self.logger.warning("MISSING BITGET PASSPHRASE.")

    def _init_exchanges(self):
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
            if not signal_queue.empty():
                signal: TradeSignal = await signal_queue.get()
                await self.process_signal(signal)
                signal_queue.task_done()

            if self.active_trades:
                await self.monitor_exits()

            await asyncio.sleep(1)

    async def process_signal(self, signal: TradeSignal):
        for trade in self.active_trades.values():
            if trade.symbol == signal.symbol and trade.status == "OPEN":
                return 
        await self.execute_entry_strategy(signal)

    async def execute_entry_strategy(self, signal: TradeSignal):
        trade_id = str(uuid.uuid4())[:8]
        self.logger.info(f"[{trade_id}] EXECUTING: {signal.symbol} (Long {signal.long_exchange} / Short {signal.short_exchange})")

        long_client = self.clients.get(signal.long_exchange)
        short_client = self.clients.get(signal.short_exchange)

        if not long_client or not short_client:
            self.logger.error(f"[{trade_id}] Clients not ready.")
            return

        size_amount = 0.0
        try:
            bal_long = await long_client.fetch_balance()
            bal_short = await short_client.fetch_balance()
            
            free_long = float(bal_long['USDT']['free'])
            free_short = float(bal_short['USDT']['free'])
            
            max_deployable = min(free_long, free_short) * PCT_EQUITY_PER_TRADE * LEVERAGE
            raw_size = max_deployable / signal.entry_price_long
            size_amount = float(long_client.amount_to_precision(signal.symbol, raw_size))
            
            self.logger.info(f"[{trade_id}] Size: {size_amount} {signal.symbol} (${max_deployable:.2f})")

        except Exception as e:
            self.logger.error(f"[{trade_id}] Sizing Error: {e}")
            return

        params_maker = {'postOnly': True} 

        try:
            t1 = long_client.create_limit_buy_order(signal.symbol, size_amount, signal.entry_price_long, params_maker)
            t2 = short_client.create_limit_sell_order(signal.symbol, size_amount, signal.entry_price_short, params_maker)
            
            results = await asyncio.gather(t1, t2, return_exceptions=True)
            res_long, res_short = results

            long_filled = not isinstance(res_long, Exception)
            short_filled = not isinstance(res_short, Exception)

            if long_filled and short_filled:
                self.logger.info(f"[{trade_id}] BOTH LEGS FILLED.")
                self._register_trade(trade_id, signal, size_amount, res_long, res_short)
            
            elif long_filled and not short_filled:
                self.logger.critical(f"[{trade_id}] LEG RISK: Long Filled, Short Failed! Error: {res_short}")
                self.logger.critical(f"[{trade_id}] FORCING TAKER SHORT HEDGE...")
                await self._force_hedge(short_client, signal.symbol, 'sell', size_amount)
                self._register_trade(trade_id, signal, size_amount, res_long, {'price': signal.entry_price_short, 'id': 'hedge-forced'})

            elif short_filled and not long_filled:
                self.logger.critical(f"[{trade_id}] LEG RISK: Short Filled, Long Failed! Error: {res_long}")
                self.logger.critical(f"[{trade_id}] FORCING TAKER BUY HEDGE...")
                await self._force_hedge(long_client, signal.symbol, 'buy', size_amount)
                self._register_trade(trade_id, signal, size_amount, {'price': signal.entry_price_long, 'id': 'hedge-forced'}, res_short)

            else:
                self.logger.warning(f"[{trade_id}] Orders failed (Post-Only). Aborting.")

        except Exception as e:
            self.logger.critical(f"[{trade_id}] EXECUTION ERROR: {e}", exc_info=True)

    async def _force_hedge(self, client, symbol, side, amount):
        try:
            if side == 'buy': await client.create_market_buy_order(symbol, amount)
            else: await client.create_market_sell_order(symbol, amount)
            self.logger.info(f"HEDGE SUCCESSFUL for {symbol}")
        except Exception as e:
            self.logger.critical(f"HEDGE FAILED: {e}. MANUAL INTERVENTION REQUIRED.")

    def _register_trade(self, trade_id, signal, size, res_long, res_short):
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

    async def monitor_exits(self):
        """
        Monitors PnL including estimated Taker Fees on exit.
        """
        for t_id, trade in list(self.active_trades.items()):
            if trade.status != "OPEN": continue

            try:
                long_client = self.clients[trade.long_exchange]
                short_client = self.clients[trade.short_exchange]
                
                tick_long = await long_client.fetch_ticker(trade.symbol)
                tick_short = await short_client.fetch_ticker(trade.symbol)
                
                exit_bid_long = tick_long['bid']
                exit_ask_short = tick_short['ask']
                
                pnl_long_pct = (exit_bid_long - trade.entry_price_long) / trade.entry_price_long
                pnl_short_pct = (trade.entry_price_short - exit_ask_short) / trade.entry_price_short
                gross_pnl_bps = (pnl_long_pct + pnl_short_pct) * 10000
                
                exit_fee_bps = 11.0 
                net_pnl_bps = gross_pnl_bps - exit_fee_bps
                
                time_held = time.time() - trade.entry_time
                
                self.logger.debug(f"[{t_id}] Net PnL: {net_pnl_bps:.1f} bps (Gross: {gross_pnl_bps:.1f})")

                if net_pnl_bps > EXIT_NET_PROFIT_TARGET_BPS:
                    self.logger.info(f"[{t_id}] TARGET HIT (Net PnL: {net_pnl_bps:.1f} bps). Closing...")
                    await self.close_trade(trade)
                
                elif time_held > 3000: 
                    self.logger.info(f"[{t_id}] TIME LIMIT. Closing...")
                    await self.close_trade(trade)

            except Exception as e:
                self.logger.error(f"Monitor error {t_id}: {e}")

    async def close_trade(self, trade: ActiveTrade):
        self.logger.info(f"[{trade.trade_id}] Closing Position (Taker)...")
        long_client = self.clients[trade.long_exchange]
        short_client = self.clients[trade.short_exchange]
        params = {'reduceOnly': True}
        
        try:
            t1 = long_client.create_market_sell_order(trade.symbol, trade.size_amount, params)
            t2 = short_client.create_market_buy_order(trade.symbol, trade.size_amount, params)
            await asyncio.gather(t1, t2)
            
            trade.status = "CLOSED"
            del self.active_trades[trade.trade_id]
            self._save_state()
            self._log_to_csv(trade, "EXIT")
            self.logger.info(f"[{trade.trade_id}] CLOSED Successfully.")
            
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
                writer.writerow(['Time', 'ID', 'Action', 'Symbol', 'LongEx', 'ShortEx', 'Size', 'PnL'])
            writer.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), trade.trade_id, action, trade.symbol, trade.long_exchange, trade.short_exchange, trade.size_amount, trade.pnl_realized])