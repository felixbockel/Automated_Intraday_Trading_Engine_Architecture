# Execution and Position Management

## Broker connection
The IBKR client connects to a locally running IBKR Gateway or Trader Workstation instance, qualifies stock contracts, and requests historical bars. Account-specific connection details are intentionally omitted.

## Entry execution
The candidate worker selects an eligible candidate and calls the execution engine. The engine qualifies a stock contract (SMART routing and USD currency in the described implementation), submits a market order, polls status for a bounded period, records a reported fill, and returns an event containing symbol and fill price. The current candidate-worker entry quantity is 1.

## Position monitoring
After an entry fill is reported, the orchestrator passes the symbol, fill information, indices, and historical context to Focus Mode. Focus Mode requests new five-minute IBKR bars, avoids reprocessing the same latest bar, and evaluates exit logic. When instructed to sell, it submits a market sell order. The described exit quantity is 100.

## Critical quantity mismatch
Entry quantity: **1**. Exit quantity: **100**. This is unresolved and material. Correct it and cover it with tests before end-to-end paper-trading validation. This documentation does not assume which quantity is intended.

## Order-state safety
A bounded polling timeout is not sufficient order-state management. An order may be accepted, partially filled, or filled even if the local process receives no timely status update. Before live operation, the execution layer needs persistent broker order IDs, explicit state transitions, cancel-and-confirm logic, broker reconciliation after timeout/reconnect, idempotent retries, and defined handling of disconnects during submission or fill confirmation.

The current timeout path needs review: absence of a local fill event must not be interpreted as proof that no order exists.

## Position and risk controls
The current flow pauses scanning while a position is active and marks trading finished after a successful exit. This is not a complete risk-management framework. A production-oriented design needs independently tested position/notional limits, daily loss limits, price/liquidity constraints, duplicate-entry prevention, broker/local reconciliation, emergency stop/manual intervention, and handling for partial fills, rejections, and market interruptions.

No claim is made that these controls are fully implemented or validated.
