import asyncio
import ccxt.pro as ccxt
import logging
import time
from binance_exchange import BinanceExchange
from bybit_exchange import BybitExchange
from bitget_exchange import BitgetExchange
from arbitrage_engine import ArbitrageEngine
from trade_manager import TradeManager

# Try to use uvloop
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

async def main():
    # 1. Queues
    # Data Queue: Scanners -> Engine
    data_queue = asyncio.Queue()
    # Execution Queue: Engine -> TradeManager
    exec_queue = asyncio.Queue()

    # 2. Initialize Components
    binance_exc = BinanceExchange()
    bybit_exc = BybitExchange()
    bitget_exc = BitgetExchange()
    
    # Engine gets the exec_queue to send signals
    engine = ArbitrageEngine(execution_queue=exec_queue)
    
    # Manager gets queues to receive signals
    trader = TradeManager()

    # 3. Run concurrently
    await asyncio.gather(
        binance_exc.start(data_queue),
        bybit_exc.start(data_queue),
        bitget_exc.start(data_queue),
        engine.process_data(data_queue),
        trader.run(exec_queue, data_queue) # Run the trader
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped.")