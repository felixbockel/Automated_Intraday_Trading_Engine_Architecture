# ===============================
# CORE/CANDIDATE_WORKER.PY
# ===============================

import asyncio
import time
from datetime import datetime
from .candidate_queue import CandidateQueue
from .ibkr_client import IBKRClient
from .execution_engine import ExecutionEngine
from utils.data_store import data_store
from strategies.strategy2 import run_strategy_2
import pytz

class CandidateWorker:
    """
    Consumes new candidates from the queue, runs Strategy2
    on IBKR data, applies filters, and executes BUY for the
    best candidate (highest estimate_revenue).
    """

    def __init__(self, ibkr: IBKRClient, candidate_queue: CandidateQueue, execution_engine: ExecutionEngine, logger):
        self.ibkr = ibkr
        self.candidate_queue = candidate_queue
        self.execution_engine = execution_engine
        self.logger = logger

        self.active_symbol = None
        self.scanned_symbols = set()   # Per-cycle deduplication
        self.s2_cache = {}             # Persistent cache of Strategy2 results

        # IBKR cycle counter
        self.ibkr_cycle = 0
        self.market_tz = pytz.timezone("America/New_York")

    async def run_once_parallel(self):
        """
        Process all candidates in queue ONCE (no loop, no sleep).
        Called by orchestrator.
        """

        self.logger.info("WORKER_RUN_ONCE")

        # 🔒 GLOBAL LOCK: do not run if a position is active
        if hasattr(self, "is_active") and self.is_active():
            self.logger.info(f"SKIP: CandidateWorker paused (active symbol = {self.active_symbol})")
            return None

        if self.candidate_queue.empty():
            self.logger.info("QUEUE_EMPTY → skipping")
            return

        # Peek all candidates without clearing queue yet
        candidates = self.candidate_queue.peek_all()
        candidates_dict = {
            c.get("symbol"): c for c in candidates if c.get("symbol")
        }
        self.logger.info(f"QUEUE_FETCHED {len(candidates)} candidates")

        self.ibkr_cycle += 1
        cycle_start = time.perf_counter()

        MAX_PARALLEL = 5
        sem = asyncio.Semaphore(MAX_PARALLEL)

        async def process_candidate(candidate):
            async with sem:
                symbol = candidate.get("symbol")
                if not symbol:
                    return None

                # Skip if already processed in this cycle
                if symbol in self.scanned_symbols:
                    return None

                # Skip if cached Strategy2 result exists
                if symbol in self.s2_cache:
                    cached_result = self.s2_cache[symbol]
                    self.logger.info(f"Skipping Strategy2 for {symbol}, cached result exists")
                    return (symbol, cached_result)

                # -------------------------------
                # IBKR data fetch
                # -------------------------------
                try:
                    bars = await asyncio.wait_for(
                        self.ibkr.get_5min_bars(symbol),
                        timeout=20
                    )
                    if bars is None or len(bars) == 0:
                        self.logger.info(f"{symbol} | ❌ No IBKR bars")
                        return None
                except Exception as e:
                    self.logger.warning(f"IBKR fetch failed for {symbol}: {e}")
                    return None

                # -------------------------------
                # Save raw snapshot
                # -------------------------------
                try:
                    raw_df = bars.copy()
                    raw_df["Symbol"] = symbol
                    raw_df["Source"] = "ibkr"
                    data_store.save_ibkr_snapshot(raw_df)
                except Exception as e:
                    self.logger.warning(f"IBKR raw snapshot failed for {symbol}: {e}")

                # -------------------------------
                # Save intraday
                # -------------------------------
                try:
                    ny_tz = pytz.timezone("America/New_York")
                    rows = [
                        {
                            "Symbol": symbol,
                            "Datetime": idx.tz_localize(None).astimezone(ny_tz) if idx.tzinfo is None else idx.astimezone(ny_tz),
                            "Open": row["Open"],
                            "High": row["High"],
                            "Low": row["Low"],
                            "Close": row["Close"],
                            "Volume": row["Volume"]
                        }
                        for idx, row in bars.iterrows()
                    ]
                    data_store.save_ibkr_intraday(rows)
                except Exception as e:
                    self.logger.warning(f"IBKR intraday save failed for {symbol}: {e}")

                # -------------------------------
                # Run Strategy2
                # -------------------------------
                try:
                    s2_result = run_strategy_2(
                        symbol=symbol,
                        bars=bars,
                        first_real_buy_index_primary=candidate.get("first_real_buy_idx"),
                        hist_context=candidate.get("hist_context", {}),
                        logger=self.logger
                    )

                    self.logger.info(f"{symbol} | Strategy2 output: {s2_result}")

                    # Add to scanned_symbols for this cycle
                    self.scanned_symbols.add(symbol)
                    
                    # Cache only Step1-valid results (avoid reprocessing weak candidates)
                    if s2_result and s2_result.get("s2", {}).get("Count_step_until_idx30") not in (0, 1, 2):
                        self.s2_cache[symbol] = s2_result

                except Exception as e:
                    self.logger.exception(f"{symbol} | ❌ Strategy2 error")
                    return None

                if s2_result:
                    return (symbol, s2_result)

                return None

        # -------------------------------
        # Run all candidates concurrently
        # -------------------------------
        results = await asyncio.gather(
            *[process_candidate(c) for c in candidates],
            return_exceptions=True
        )
        
        all_results = [r for r in results if r and not isinstance(r, Exception)]
        
        if not all_results:
            self.logger.info("NO_STRATEGY2_RESULTS")
            return
        # -------------------------
        # Pre-filter: core validity checks
        # -------------------------
        fail_counts = {
            "pattern": 0,
            "ema20": 0,
            "ema50": 0
        }
        
        prefiltered = []
        
        for r in all_results:
            s2 = r[1].get("s2", {})
            
            if s2.get("final_pattern") != "valid":
                fail_counts["pattern"] += 1
                continue
            if s2.get("ema20_good_secondary", 0) <= 0:
                fail_counts["ema20"] += 1
                continue
            if s2.get("ema50_good_secondary", 0) <= 0:
                fail_counts["ema50"] += 1
                continue
        
            prefiltered.append(r)
        
        self.logger.info(
            f"PREFILTER: kept={len(prefiltered)} "
            f"filtered_out={len(all_results) - len(prefiltered)} "
            f"fails={fail_counts}"
        )
  
        # -------------------------
        # Step 1: exclude 0,1,2
        # -------------------------
        step1 = [
            r for r in prefiltered
            if r[1].get("s2", {}).get("Count_step_until_idx30") not in (0, 1, 2)
        ]
        
        if not step1:
            self.logger.info("STEP1_EMPTY")
            return
        
        # -------------------------
        # Step 2: primary condition (2–9)
        # -------------------------
        step2 = [
            r for r in step1
            if 2 <= r[1].get("s2", {}).get("Max_occurrence_count_if_bigger_than_idx30", 0) <= 9
        ]
        
        # -------------------------
        # Step 3: pick max revenue
        # -------------------------
        if step2:
            best_candidate = max(
                step2,
                key=lambda x: x[1]["s2"].get("Daily_revenue_estimated", float("-inf"))
            )
        else:
            # fallback: value == 1
            fallback = [
                r for r in step1
                if r[1]["s2"].get("Max_occurrence_count_if_bigger_than_idx30") == 1
            ]
        
            if not fallback:
                self.logger.info("NO_CANDIDATE_AFTER_FALLBACK")
                return
        
            best_candidate = max(
                fallback,
                key=lambda x: x[1]["s2"].get("Daily_revenue_estimated", float("-inf"))
            )
            
        symbol_to_buy, strategy_data = best_candidate
        
        # -------------------------------
        # Execute trade
        # -------------------------------
        buy_result_info = None
        buy_result = await self.execution_engine.execute(
            symbol=symbol_to_buy,
            action="BUY",
            quantity=1,
            strategy_data=strategy_data
        )

        if buy_result and buy_result.get("event") == "BUY_FILLED":
            self.active_symbol = symbol_to_buy
            self.logger.info(f"BUY_FILLED for {symbol_to_buy} → ready for FocusMode")
        
            buy_result_info = {
                "symbol": symbol_to_buy,
                "price": buy_result.get("price"),

                # ✅ FULL STATE HANDOFF
                "first_real_buy_idx_primary": candidates_dict.get(symbol_to_buy, {}).get("first_real_buy_idx"),
                "first_real_buy_idx_secondary": strategy_data.get("s2", {}).get("first_real_buy_idx_secondary"),
                "hist_context": candidates_dict.get(symbol_to_buy, {}).get("hist_context")
            }
        
            self.candidate_queue.clear()

        cycle_duration = time.perf_counter() - cycle_start
        self.logger.info(f"IBKR_SCAN_END cycle={self.ibkr_cycle} duration={cycle_duration:.3f}s")

        return buy_result_info
