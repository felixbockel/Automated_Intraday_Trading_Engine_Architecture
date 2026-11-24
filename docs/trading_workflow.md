# Trading Workflow

## Session scheduling
The application uses New York local time and aligns the scan cycle to five-minute boundaries with a short buffer intended to allow candles to complete. Windows below are approximate implementation phases and may change.

## Phase 1 — data collection
Before approximately 11:35 a.m. New York time, the Yahoo scanner collects and normalises five-minute data and persists snapshots. The first strategy stage is disabled during this phase.

## Phase 2 — candidate generation
Approximately 11:35 a.m. to noon, the first-stage screen is enabled. Qualifying symbols are added to the candidate queue and recorded. A processed-symbol set is used to reduce duplicate candidate creation. Proprietary conditions and thresholds are not disclosed here.

## Phase 3 — candidate evaluation and entry
After approximately noon, the worker processes queued candidates when no position is active and trading has not been marked finished. It requests five-minute IBKR bars, runs subsequent evaluation, filters and ranks candidates using an estimated-revenue field, and attempts an entry. The current described entry quantity is one share; this is an implementation fact, not an endorsed sizing approach.

## Position monitoring
After an entry fill is reported, symbol and context are handed to Focus Mode. Scanning and candidate evaluation pause while the position is active. Focus Mode polls five-minute IBKR bars and evaluates exit logic. After an exit fill is reported, the orchestrator marks the position inactive and trading finished for the day.

**Critical discrepancy:** the described Focus Mode exit order quantity is 100 shares, which does not match the entry quantity of one share. This must be resolved before end-to-end trading validation.

## Shutdown
The application is designed to shut down five minutes after the US market close. It also responds to SIGINT/SIGTERM by cancelling tasks and disconnecting from IBKR. Forced termination, Windows updates/reboots, network failure, and broker downtime still require resilience testing.

## Not established
This workflow does not prove reliable partial-fill handling, restart recovery of open positions, broker/local state reconciliation, safe retry behaviour, validated risk limits, or production-grade monitoring.
