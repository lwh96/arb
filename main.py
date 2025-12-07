import asyncio
import ccxt.pro as ccxt
import logging
import time
from binance_exchange import BinanceExchange
from bybit_exchange import BybitExchange
from bitget_exchange import BitgetExchange
from arbitrage_engine import ArbitrageEngine
from trade_manager import TradeManager
from logger_config import setup_logger

# Initialize Logger
logger = setup_logger("Main")

# Try to use uvloop
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    logger.info("Using uvloop for high performance")
except ImportError:
    pass

async def main():
    logger.info("🤖 Starting Arbitrage Bot System...")

    # 1. Queues
    data_queue = asyncio.Queue()
    exec_queue = asyncio.Queue()

    # 2. Initialize Components
    try:
        binance_exc = BinanceExchange()
        bybit_exc = BybitExchange()
        bitget_exc = BitgetExchange()
        
        engine = ArbitrageEngine(execution_queue=exec_queue)
        trader = TradeManager()
        
        logger.info("All components initialized. Starting concurrent loops...")

        # 3. Run concurrently
        await asyncio.gather(
            binance_exc.start(data_queue),
            bybit_exc.start(data_queue),
            bitget_exc.start(data_queue),
            engine.process_data(data_queue),
            trader.run(exec_queue, data_queue)
        )
    except Exception as e:
        logger.critical("Fatal Crash in Main Loop!", exc_info=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")