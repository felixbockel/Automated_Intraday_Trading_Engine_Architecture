# ===============================
# STRATEGIES/STRATEGY3
# ===============================

from datetime import datetime, time
import pytz

# 🔥 import Strategy2 runner
from strategies.strategy2 import run_strategy_2


class Strategy3:
    """
    Exit logic (Focus Mode):

    Priority:
    1. Strategy2 SELL trigger (dynamic exit)
    2. End-of-day SELL (15:55 ET fallback)
    """

    def __init__(self, logger):
        self.logger = logger
        self.ny_tz = pytz.timezone("America/New_York")
        self.market_close_candle_time = time(hour=15, minute=55)

    async def evaluate(
        self,
        ticker: dict,
        active_symbol: str,
        bars,
        first_real_buy_idx_secondary=None,
        hist_context=None,
    ):
        """
        Runs every 5-min cycle in Focus Mode
    
        Returns:
            dict | None
        """
    
        # -------------------------------
        # Extract candle data
        # -------------------------------
        candle_open = ticker.get("Open")
        candle_time = ticker.get("Datetime")
    
        if candle_open is None or candle_time is None:
            self.logger.warning(f"Missing candle data for {active_symbol}")
            return None
    
        # -------------------------------
        # Safety check (should never happen)
        # -------------------------------
        if first_real_buy_idx_secondary is None:
            self.logger.warning(
                f"{active_symbol} | Missing first_real_buy_idx_secondary in Strategy3"
            )
            return None
    
        # -------------------------------
        # Run Strategy2 (post-entry tracking)
        # -------------------------------
        try:
            s2_result = run_strategy_2(
                symbol=active_symbol,
                bars=bars,
                # ✅ Use ONLY secondary (your confirmed structure)
                first_real_buy_index_primary=first_real_buy_idx_secondary,
                hist_context=hist_context or {},
                logger=self.logger
            )
    
            self.logger.info(f"{active_symbol} | Strategy2 output: {s2_result}")
    
        except Exception as e:
            self.logger.error(f"Strategy2 error in Strategy3 for {active_symbol}: {e}")
            return None
    
        # -------------------------------
        # Extract SELL trigger
        # -------------------------------
        sell_trigger_list = s2_result.get("sell_trigger", [])
        sell_trigger = any(sell_trigger_list)
    
        # -------------------------------
        # PRIORITY 1: Strategy2 SELL
        # -------------------------------
        if sell_trigger:
            self.logger.info(
                f"STRATEGY2 SELL signal | {active_symbol} | "
                f"candle_time={candle_time}"
            )
    
            return {
                "action": "SELL",
                "reason": "STRATEGY2_TRIGGER",
                "price_reference": candle_open,
                "timestamp": candle_time,
                "s2_context": s2_result
            }
    
        # -------------------------------
        # PRIORITY 2: End-of-day SELL
        # -------------------------------
        if candle_time.hour == 15 and candle_time.minute == 55:
            self.logger.info(
                f"EOD SELL signal | {active_symbol} | "
                f"candle_time={candle_time} | open={candle_open}"
            )
    
            return {
                "action": "SELL",
                "reason": "EOD_1555",
                "price_reference": candle_open,
                "timestamp": candle_time,
                "s2_context": s2_result
            }
    
        # -------------------------------
        # HOLD
        # -------------------------------
        return None
