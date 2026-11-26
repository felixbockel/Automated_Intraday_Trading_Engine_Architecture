# ===============================
# CORE/EXECUTION_ENGINE
# ===============================

import asyncio
from datetime import datetime
from ib_insync import MarketOrder, Stock
from utils.data_store import data_store

class ExecutionEngine:
    """
    Handles placing market orders via IBKR.
    Logs trades via data_store.
    """

    def __init__(self, ib, contracts=None, logger=None):
        self.ib = ib
        self.contracts = contracts or {}
        self.logger = logger
        self.order_delay = 0.25

    async def execute(self, symbol, action="BUY", quantity=100, strategy_data=None):
        if self.logger:
            self.logger.info(f"EXECUTION_START symbol={symbol} action={action}")
            self.logger.info(f"EXECUTION_IB_CONNECTED={self.ib.isConnected()}")
        
        # -------------------------------
        # Contract handling
        # -------------------------------
        if symbol in self.contracts:
            contract = self.contracts[symbol]
            if self.logger:
                self.logger.info(f"EXECUTION_CONTRACT_CACHE_HIT symbol={symbol}")
        else:
            if self.logger:
                self.logger.info(f"EXECUTION_CONTRACT_CREATE symbol={symbol}")
        
            contract = Stock(symbol, "SMART", "USD")
        
            if self.logger:
                self.logger.info(f"EXECUTION_QUALIFY_START symbol={symbol}")
        
            qualified = await self.ib.qualifyContractsAsync(contract)
        
            if self.logger:
                self.logger.info(f"EXECUTION_QUALIFY_DONE symbol={symbol} result={qualified}")
        
            if not qualified:
                if self.logger:
                    self.logger.error(f"EXECUTION_QUALIFY_FAILED symbol={symbol}")
                return None
        
            self.contracts[symbol] = contract
        
        # -------------------------------
        # Order placement
        # -------------------------------
        await asyncio.sleep(self.order_delay)
        
        if self.logger:
            self.logger.info(f"EXECUTION_PLACE_ORDER symbol={symbol}")
        
        order = MarketOrder(action, quantity)
        trade = self.ib.placeOrder(contract, order)
        
        if self.logger:
            self.logger.info(f"EXECUTION_ORDER_SUBMITTED orderId={trade.order.orderId}")
        
        # -------------------------------
        # Wait for fill (with timeout)
        # -------------------------------
        start_time = asyncio.get_event_loop().time()
        
        while not trade.isDone():
        
            if self.logger:
                self.logger.info(
                    f"EXECUTION_WAITING orderId={trade.order.orderId} "
                    f"status={trade.orderStatus.status}"
                )
        
            if asyncio.get_event_loop().time() - start_time > 30:
                if self.logger:
                    self.logger.error(f"EXECUTION_TIMEOUT orderId={trade.order.orderId}")
                return None
        
            await asyncio.sleep(0.2)
        
        if self.logger:
            self.logger.info(
                f"EXECUTION_FINAL_STATUS orderId={trade.order.orderId} "
                f"status={trade.orderStatus.status}"
            )
        
        if trade.orderStatus.status != "Filled":
            if self.logger:
                self.logger.warning(f"Order not filled: {trade.orderStatus.status}")
            return None

        # Fill details
        fill_price = trade.orderStatus.avgFillPrice
        fill_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order_id = trade.order.orderId

        # Log via data_store
        trade_record = {
            "timestamp": fill_time,
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "price": fill_price,
            "order_id": order_id,
            **(strategy_data or {})
        }
        data_store.save_trade(trade_record)

        if self.logger:
            self.logger.info(f"{action} filled for {symbol} at {fill_price}")

        return {"event": f"{action.upper()}_FILLED", "symbol": symbol, "price": fill_price}
