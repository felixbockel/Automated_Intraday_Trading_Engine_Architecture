# Development Roadmap

The project remains under development. Completing this roadmap would not, by itself, establish profitability or eliminate trading risk.

## 1. Market-data validation
- Compare Yahoo Finance and IBKR data for selected symbols and sessions.
- Verify five-minute candle completion and timestamp alignment.
- Test New York timezone handling across daylight-saving transitions.
- Test missing, duplicated, delayed, and placeholder candles.
- Verify prior-session context selection and prevent look-ahead.

## 2. Candidate lifecycle and strategy integration
- Audit reset rules for `scanned_symbols`.
- Review cache keys, invalidation, and which outcomes are cached.
- Test candidate deduplication, queue capacity, and worker exceptions.
- Make strategy evaluation deterministic and independently testable.

## 3. Execution safety
- Resolve the entry/exit quantity mismatch.
- Persist order identifiers and state transitions.
- Handle timeouts, partial fills, rejections, and cancellations.
- Reconcile broker positions/open orders after reconnect or restart.
- Prevent duplicate orders after ambiguous responses.
- Add and test position-sizing and risk limits.

## 4. Persistence and observability
- Replace destructive data-directory initialisation.
- Remove/configure the testing-only premarket symbol filter.
- Audit concurrent writes, including trade CSV persistence.
- Define data and log retention policies.
- Add structured event logging and actionable alerts.

## 5. Reliability and recovery
- Test IBKR disconnects, network interruption, and process termination during an active position.
- Test Windows restart/update scenarios and scheduled maintenance.
- Reconcile broker state before enabling trading after restart.
- Ensure shutdown cannot leave an untracked live order or position.

## 6. Verification sequence
1. Unit tests for normalisation, time handling, queue behaviour, candidate selection, and state transitions.
2. Integration tests using recorded or synthetic market data.
3. IBKR connectivity tests without live orders.
4. Paper-trading tests for normal and failure scenarios.
5. Extended monitored paper trading with documented acceptance criteria.
6. Independent code, operational, and risk review before any live-use decision.

Passing tests or completing paper trading does not establish future profitability.

## 7. Operational preparation
Document deployment, monitoring, secrets handling, backups, recovery, manual shutdown, and escalation. Plan controlled operating-system maintenance while continuing to apply security updates.
