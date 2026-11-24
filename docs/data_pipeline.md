# Data Pipeline and Persistence

## Sources
- **Yahoo Finance:** historical daily/hourly context and batched five-minute intraday scanning data.
- **Interactive Brokers:** five-minute historical bars during candidate evaluation and position monitoring, plus order routing.

The sources serve different stages. Their timestamps, session definitions, missing intervals, and prices should not be assumed interchangeable.

## Premarket context
The loader retrieves multiple daily history windows and a shorter hourly window. Normalisation includes flattening multi-index columns, deduplication, standard OHLCV names, numeric conversion, and datetime/timezone handling. Retries and throttling are used for selected failed downloads; failed symbols are recorded separately.

**Validation note:** `daily_prev` is currently assigned from the latest row of a one-year dataset. That row is not necessarily the intended previous completed session for the assessment date and should be verified.

## Yahoo intraday scanner
The scanner downloads five-minute data in batches for a configured universe (approximately 980 symbols in the described settings). Symbol-level processing uses a thread pool. It enforces the regular US session in New York time, normalises timestamps, removes duplicates and missing/placeholder candles, saves raw snapshots, and applies the first-stage screen during its configured phase.

A successful batch request does not prove that every symbol has a complete session history.

## IBKR data
The candidate worker requests and saves five-minute broker bars. Focus Mode repeatedly requests five-minute historical bars and skips a bar already processed. A one-minute streaming helper exists in the client, but it is not the current Focus Mode path described here.

## Representative storage layout
```text
data/
├── intraday/YYYY-MM-DD.parquet
├── yahoo_raw/YYYY-MM-DD.parquet
├── premarket/YYYY-MM-DD.parquet
├── ibkr_raw/YYYY-MM-DD/
├── ibkr_intraday/YYYY-MM-DD/
├── strategy_inputs/
│   ├── strategy1/YYYY-MM-DD/SYMBOL.parquet
│   └── strategy2/YYYY-MM-DD/SYMBOL.parquet
├── candidates.csv
└── trades.csv
```
Parquet stores structured/time-series data; CSV stores selected candidate and trade records.

## Integrity and operational risks
1. DataStore initialisation currently deletes and recreates the whole `data` directory; replace this with non-destructive setup.
2. Premarket persistence currently filters to a testing symbol (`BAX`); it is not a general production save path.
3. Audit locks and concurrent writes, including trade CSV persistence.
4. Preserve source timestamps and timezone provenance; test daylight-saving transitions.
5. Represent missing data explicitly and record failed symbols.
6. Verify prior-session context and prevent look-ahead.
7. Define retention for raw data, derived data, and logs.

## Validation checklist
Compare Yahoo/IBKR timestamps and OHLC; test DST boundaries; test missing, delayed, duplicated, and zero-volume bars; verify candle completion; validate prior-session references; test persistence across repeated runs; and test concurrent/interrupted writes.
