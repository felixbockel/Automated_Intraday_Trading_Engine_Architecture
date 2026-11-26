# ===============================
# CORE/IBKR_CLIENT.PY
# ===============================

import asyncio
from ib_insync import IB, Stock, MarketOrder,util
from datetime import datetime, timedelta
import pandas as pd

class IBKRClient:
    """
    IBKR connection handler:
    - Historical 5-min bars
    - Streaming 1-min bars
    - Market order execution
    """

    def __init__(self, ib, logger=None):
        self.ib = ib
        self.logger = logger

    # -------------------------------
    # Historical bars
    # -------------------------------
    async def get_historical_bars(self, symbol, interval="5 mins", period="1 D"):

        if self.logger:
            self.logger.info(f"IBKR_HIST_START symbol={symbol}")
            self.logger.info(f"IBKR_CONNECTED={self.ib.isConnected()}")
    
        try:
            contract = Stock(symbol, "SMART", "USD")
    
            if self.logger:
                self.logger.info(f"IBKR_CONTRACT_CREATED symbol={symbol}")
    
            # QUALIFY
            if self.logger:
                self.logger.info(f"IBKR_QUALIFY_START symbol={symbol}")
            
            qualified = await asyncio.wait_for(
                self.ib.qualifyContractsAsync(contract),
                timeout=10
            )
            
            if self.logger:
                self.logger.info(f"IBKR_QUALIFY_DONE symbol={symbol} result={qualified}")
            
            if not qualified:
                if self.logger:
                    self.logger.error(f"IBKR_QUALIFY_FAILED symbol={symbol}")
                raise
    
            # REQUEST DATA
            if self.logger:
                self.logger.info(f"IBKR_REQ_HIST_START symbol={symbol}")
    
            bars = await asyncio.wait_for(
                self.ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime='',
                    durationStr=period,
                    barSizeSetting=interval,
                    whatToShow='TRADES',
                    useRTH=True,
                    formatDate=1
                ),
                timeout=15
            )
    
            if self.logger:
                self.logger.info(
                    f"IBKR_REQ_HIST_DONE symbol={symbol} bars_count={len(bars) if bars else 0}"
                )
    
            if not bars:
                if self.logger:
                    self.logger.warning(f"IBKR_EMPTY_BARS symbol={symbol}")
                return pd.DataFrame()
    
            df = util.df(bars)

            if self.logger:
                self.logger.info(f"IBKR_DF_CREATED symbol={symbol} shape={df.shape}")
    
            df.set_index('date', inplace=True)
            df.index = pd.to_datetime(df.index)
    
            df = df.rename(columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume"
            })
    
            return df
    
        except asyncio.TimeoutError:
            if self.logger:
                self.logger.error(f"IBKR_TIMEOUT symbol={symbol}")
            raise
    
        except Exception as e:
            if self.logger:
                self.logger.error(f"IBKR_ERROR symbol={symbol} error={e}")
            raise

    # -------------------------------
    # Strategy2 helper
    # -------------------------------
    async def get_5min_bars(self, symbol):
        if self.logger:
            self.logger.info(f"IBKR_GET_5MIN_BARS_START symbol={symbol}")
        return await self.get_historical_bars(symbol, "5 mins", "1 D")
    
    
    # -------------------------------
    # Stream live bars (1-min)
    # -------------------------------
    def stream_1min_bars(self, symbol, callback):
        """
        Stream live 1-min bars for a symbol and call callback(bar_dict)
        """
        contract = Stock(symbol, "SMART", "USD")
        self.ib.qualifyContracts(contract)
        ticker = self.ib.reqMktData(contract)

        def on_bar_update(tick):
            if tick.last:
                bar_dict = {
                    "Datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Last": tick.last,
                    "Bid": tick.bid,
                    "Ask": tick.ask
                }
                callback(bar_dict)

        ticker.updateEvent += on_bar_update
        return ticker

    # -------------------------------
    # Place market order
    # -------------------------------
    async def place_order(self, symbol, action="BUY", quantity=1):
    
        if self.logger:
            self.logger.info(f"IBKR_PLACE_ORDER_START symbol={symbol}")
    
        contract = Stock(symbol, "SMART", "USD")
    
        try:
            # -------------------------------
            # Qualify contract (with timeout)
            # -------------------------------
            qualified = await asyncio.wait_for(
                self.ib.qualifyContractsAsync(contract),
                timeout=10
            )
    
            if not qualified:
                if self.logger:
                    self.logger.error(f"IBKR_PLACE_ORDER_QUALIFY_FAILED symbol={symbol}")
                return None
    
            # -------------------------------
            # Place order
            # -------------------------------
            order = MarketOrder(action, quantity)
            trade = self.ib.placeOrder(contract, order)
    
            if self.logger:
                self.logger.info(f"IBKR_ORDER_SUBMITTED orderId={trade.order.orderId}")
    
            # -------------------------------
            # Wait for fill (with timeout)
            # -------------------------------
            start_time = asyncio.get_event_loop().time()
    
            while not trade.isDone():
    
                if self.logger:
                    self.logger.info(
                        f"IBKR_ORDER_WAITING orderId={trade.order.orderId} "
                        f"status={trade.orderStatus.status}"
                    )
    
                if asyncio.get_event_loop().time() - start_time > 30:
                    if self.logger:
                        self.logger.error(f"IBKR_ORDER_TIMEOUT symbol={symbol}")
                    return None
    
                await asyncio.sleep(0.2)
    
            # -------------------------------
            # Return result
            # -------------------------------
            return {
                "status": trade.orderStatus.status,
                "fill_price": trade.orderStatus.avgFillPrice,
                "order_id": trade.order.orderId
            }
    
        except Exception as e:
            if self.logger:
                self.logger.error(f"IBKR_PLACE_ORDER_ERROR symbol={symbol} error={e}")
            return None

    # -------------------------------
    # Disconnect
    # -------------------------------
    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            if self.logger:
                self.logger.info("Disconnected from IBKR")
