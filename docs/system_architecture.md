# System Architecture

## Intent
The application separates market-data collection, screening, broker communication, execution, position monitoring, and persistence into modules coordinated by `main.py`. It uses New York local time and combines asynchronous orchestration with selected threaded operations.

## Components
- **Orchestrator:** configures logging, loads symbols and premarket context, connects to IBKR, creates components, runs recurring scans and Focus Mode, handles signals, cancels tasks, and disconnects.
- **Premarket loader:** retrieves daily and hourly Yahoo Finance context, normalises frames, records failed symbols, and persists context.
- **Yahoo scanner:** downloads five-minute intraday data in batches, processes symbols with a thread pool, normalises New York timestamps, removes duplicates/unusable candles, saves raw snapshots, and applies the first-stage screen when enabled.
- **Candidate queue:** thread-safe deque protected by a lock, supporting enqueue, peek, retrieve, and clear operations.
- **Candidate worker:** requests five-minute IBKR bars, saves broker snapshots, evaluates candidates asynchronously with a semaphore, filters/ranks eligible candidates, and attempts an entry.
- **IBKR client:** broker connectivity, contract qualification, historical bar retrieval, and order-related methods. Although a one-minute streaming helper exists, the current Focus Mode flow uses repeated five-minute historical requests.
- **Execution engine:** qualifies stock contracts, submits market orders, polls order status, records reported fills, and returns fill events.
- **Focus Mode:** takes over after a reported entry fill, polls for new five-minute bars, avoids reprocessing the last bar, evaluates exit logic, and submits an exit order.
- **Data store:** persists raw/normalised data, strategy inputs, candidates, and trades.

## Concurrency
The design combines an `asyncio` loop, background tasks, thread-pool symbol processing, a locked candidate queue, selected persistence locks, and a semaphore for concurrent candidate evaluation. Shared-state ownership and write safety require tests, especially for candidate caches, scanned-symbol tracking, position state, and CSV writes.

## Position lifecycle
`Scanning → Candidate queued → Candidate evaluated → Entry submitted → Entry fill reported → Focus Mode → Exit submitted → Exit fill reported → Daily lockout`

This conceptual lifecycle does not establish complete handling of partial fills, delayed acknowledgements, rejected orders, disconnects, or ambiguous timeouts.

## Known implementation caveats
- Entry quantity is described as 1; exit quantity is described as 100.
- An order timeout is not proof that an order was not accepted or filled.
- DataStore initialisation currently deletes and recreates the data directory.
- Premarket saving contains a testing-symbol restriction.
- Candidate cache/scanned-symbol lifecycle needs audit.
- The `daily_prev` assignment from the latest row of a one-year dataset should be checked against the intended previous completed session.
- Logging cleanup and retention need review.
