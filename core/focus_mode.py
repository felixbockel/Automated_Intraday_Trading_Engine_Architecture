# ===============================
# CORE/FOCUS_MODE.PY
# ===============================

import asyncio
from datetime import datetime
from core.ibkr_client import IBKRClient
from strategies.strategy3 import Strategy3  # updated to import class
import time
import pytz

class FocusMode:
    """
    Handles Focus Mode after a BUY:
    - Stops Yahoo scanning & candidate processing
    - Streams 1-min IBKR bars
    - Evaluates Strategy3 for SELL
    - Executes SELL via IBKRClient
    """

    def __init__(self, ibkr_client: IBKRClient, execution_engine, logger):
        self.ibkr_client = ibkr_client
        self.execution_engine = execution_engine
        self.logger = logger
        self.active_symbol = None
        self.running = False
    
        self.strategy3 = Strategy3(logger)
    
        self.last_bar_time = None
    
        # ✅ ADD THESE
        self.first_real_buy_idx_secondary = None
        self.hist_context = None

    # -------------------------------
    # Start focus mode
    # -------------------------------
    # start()
    async def start(
        self,
        symbol: str,
        shutdown_event: asyncio.Event,
        first_real_buy_idx_secondary=None,
        hist_context=None
    ):
        self.active_symbol = symbol
        self.running = True
        self.last_bar_time = None
    
        self.first_real_buy_idx_secondary = first_real_buy_idx_secondary
        self.hist_context = hist_context
    
        self.logger.info(f"Focus Mode activated for {symbol}")
    
        result = await self._monitor_symbol(shutdown_event)
        return result

    # -------------------------------
    # Monitor symbol for exit
    # -------------------------------
    async def _monitor_symbol(self, shutdown_event: asyncio.Event):
        """
        Poll 5-min bars aligned to 5-min boundaries until Strategy3 signals SELL
        """
    
        def seconds_until_next_5min(buffer_seconds: int = 3) -> float:
            from datetime import datetime
    
            now = datetime.now(pytz.timezone("America/New_York"))
    
            # Calculate next 5-min boundary
            next_minute = (now.minute // 5 + 1) * 5
    
            if next_minute >= 60:
                next_time = now.replace(hour=(now.hour + 1) % 24,
                                        minute=0, second=0, microsecond=0)
            else:
                next_time = now.replace(minute=next_minute,
                                        second=0, microsecond=0)
    
            return max((next_time - now).total_seconds() + buffer_seconds, 0)
    
        self.logger.info(f"Focus Mode polling (5-min aligned) for {self.active_symbol}")
    
        while self.running and not shutdown_event.is_set():
    
            cycle_start = time.perf_counter()
    
            try:
                # -------------------------------
                # Pull latest 5-min bars
                # -------------------------------
                bars = await self.ibkr_client.get_5min_bars(self.active_symbol)
    
                if bars is not None and not bars.empty:

                    current_time = bars.index[-1]

                    # -------------------------------
                    # Skip duplicate candle
                    # -------------------------------
                    if self.last_bar_time == current_time:
                        await asyncio.sleep(1)
                        continue
                    
                    self.last_bar_time = current_time
                    
                    latest_bar = bars.iloc[-1].to_dict()
                    latest_bar["Datetime"] = current_time
    
                    # -------------------------------
                    # Run Strategy3
                    # -------------------------------
                    result = await self.strategy3.evaluate(
                        ticker=latest_bar,
                        active_symbol=self.active_symbol,
                        bars=bars,
                        first_real_buy_idx_secondary=self.first_real_buy_idx_secondary,
                        hist_context=self.hist_context
                    )
                    
                    if result and result.get("action") == "SELL":
                        self.logger.info(f"SELL triggered for {self.active_symbol}")
                    
                        exec_result = await self.execution_engine.execute(
                            symbol=self.active_symbol,
                            action="SELL",
                            quantity=100,
                            strategy_data=result
                        )
                    
                        if exec_result and exec_result.get("event") == "SELL_FILLED":
                            self.logger.info(f"Position closed for {self.active_symbol}")
                            self.running = False
                        
                            return {
                                "event": "SELL_FILLED",
                                "symbol": self.active_symbol
                            }
    
            except Exception as e:
                self.logger.warning(f"FocusMode error for {self.active_symbol}: {e}")
    
            # -------------------------------
            # Align to next 5-min boundary
            # -------------------------------
            sleep_time = seconds_until_next_5min(buffer_seconds=3)
    
            self.logger.info(f"NEXT_CYCLE_SLEEP {sleep_time:.2f}s")
    
            await asyncio.sleep(sleep_time)
    
            cycle_duration = time.perf_counter() - cycle_start
            self.logger.info(f"FOCUS_CYCLE_END duration={cycle_duration:.2f}s")

        self.logger.info(f"Focus Mode ended for {self.active_symbol}")
        return {"event": "FOCUS_ENDED", "symbol": self.active_symbol}
