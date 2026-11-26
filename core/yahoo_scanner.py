# ===============================
# CORE/YAHOO_SCANNER
# ===============================

import pandas as pd
import yfinance as yf
from datetime import datetime, time, timedelta
from config.settings import SCAN_INTERVAL_SECONDS, MAX_SYMBOLS, PREMARKET_SLEEP_SECONDS
from utils.data_store import data_store
from strategies.strategy1 import run_strategy_1
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import time as t
import numbers
from pathlib import Path
               

class YahooScanner:
    """
    Continuously scan all symbols using Yahoo Finance 5-min bars,
    run Strategy1, and append new qualifying symbols to candidate CSV.

    Historical context is provided from the premarket loader
    and NOT downloaded during intraday scans.
    """

    def __init__(self, symbols, historical_context, logger, candidate_queue):
        self.candidate_queue = candidate_queue
        self.logger = logger   # <--- assign first
    
        self.symbols = symbols[:MAX_SYMBOLS]  # limit universe
        self.is_active = None
        
        # -------------------------
        # Load failed tickers from premarket pull
        # -------------------------
        data_folder = Path("data")
        failed_file = data_folder / "premarket_failed_symbols.csv"
        
        try:
            failed_df = pd.read_csv(failed_file)
            failed_symbols = set(failed_df["Symbol"])
        except FileNotFoundError:
            failed_symbols = set()
        
        # Filter out failed symbols from scan universe
        original_count = len(self.symbols)
        self.symbols = [s for s in self.symbols if s not in failed_symbols]
        self.failed_symbols = failed_symbols
    
        self.logger.info(
            f"YahooScanner initialized: {len(self.symbols)}/{original_count} symbols to scan, "
            f"{len(failed_symbols)} excluded due to premarket failures"
        )
    
        self.historical_context = historical_context
        self.processed_symbols = set()
        self.yahoo_cycle = 0
        self.full_intraday_saved = False

    # =========================
    # Main continuous scan
    # =========================
    def run_cycle(self, run_strategy1=True):
        """
        Main continuous scan:
        - Checks NY market hours (9:30–16:00)
        - Pulls intraday Yahoo data for all symbols
        - Runs Strategy1 for symbols after 11:35 AM
        """
        
        try:
            # 🚫 STOP ALL SCANNING if position is active
            if self.is_active and self.is_active():
                self.logger.info("YAHOO_PAUSED → Active position detected")
                return []
            
            # -------------------------
            # Current NY time
            # -------------------------
            market_tz = pytz.timezone("America/New_York")
            now_ny = datetime.now(pytz.UTC).astimezone(market_tz)
    
            # -------------------------
            # Market hours
            # -------------------------
            market_open = time(hour=9, minute=30)
            market_close = time(hour=16, minute=0)
    
            if now_ny.time() < market_open:
                self.logger.info(
                    f"Yahoo intraday scan paused — waiting for market open (current NY time {now_ny.time()})"
                )
                return []
    
            if now_ny.time() > market_close:
                self.logger.info(
                    f"Yahoo intraday scan paused — market closed (current NY time {now_ny.time()})"
                )
                return []
    
            self.yahoo_cycle += 1
            cycle_start = t.perf_counter()
    
            self.logger.info(
                f"[TIME CHECK] NY={now_ny.strftime('%Y-%m-%d %H:%M:%S %Z')} | "
                f"Hour={now_ny.hour} Min={now_ny.minute}"
            )
    
            # -------------------------
            # 1️⃣ Batch Intraday Pull
            # -------------------------
            intraday_data = yf.download(
                tickers=self.symbols,
                interval="5m",
                period="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True
            )
    
            data_store.save_yahoo_snapshot(intraday_data)
    
            new_candidates = []
    
            # -------------------------
            # 2️⃣ Parallel symbol processing
            # -------------------------
            with ThreadPoolExecutor(max_workers=16) as executor:
                futures = {
                    executor.submit(
                        self.process_symbol,
                        symbol,
                        intraday_data,
                        run_strategy1,
                        now_ny   # 👈 ADD THIS
                    ): symbol
                    for symbol in self.symbols
                }
    
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            new_candidates.append(result)
                    except Exception as e:
                        self.logger.warning(f"Yahoo scan error: {e}")
    
            # -------------------------
            # 3️⃣ Logging behaviour
            # -------------------------
            if run_strategy1:
                if new_candidates:
                    safe_candidates = [
                        {
                            k: v
                            for k, v in c.items()
                            if k != "hist_context"
                        }
                        for c in new_candidates
                    ]
                
                    self.logger.info(f"New Strategy1 candidates: {safe_candidates}")
                else:
                    self.logger.info("No new candidates found in this cycle.")
    
            cycle_duration = t.perf_counter() - cycle_start
    
            # -------------------------
            # Mark full intraday save completed
            # -------------------------
            if now_ny.time() >= time(hour=11, minute=35) and not self.full_intraday_saved:
                self.logger.info("FULL_INTRADAY_DATASET_SAVED")
                self.full_intraday_saved = True
    
            return new_candidates
    
        except Exception as e:
            cycle_duration = t.perf_counter() - cycle_start
            self.logger.warning(f"Yahoo batch scan failed: {e}")
            self.logger.info(
                f"YAHOO_SCAN_END cycle={self.yahoo_cycle} duration={cycle_duration:.3f}s"
            )
            return []

    # =========================
    # Symbol Processing
    # =========================
    def process_symbol(self, symbol, intraday_data, run_strategy1, now_ny):
        """
        Process a single symbol:
        - Normalize intraday data
        - Skip if before 11:35 AM NY time
        - Save intraday data
        - Load historical context from premarket cache
        - Run Strategy1
        """

        try:

            try:
                df = intraday_data[symbol]
            except KeyError:
                return None

            # Normalize DataFrame
            df = self._normalize(df)

            if df is None or df.empty:
                return None
                               
            # -------------------------
            # Intraday storage logic
            # -------------------------

            # Save FULL dataset once
            if not self.full_intraday_saved:

                rows = []

                rows = [
                    {
                        "Symbol": symbol,
                        "Datetime": idx.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"),
                        "Open": r.Open,
                        "High": r.High,
                        "Low": r.Low,
                        "Close": r.Close,
                        "Volume": r.Volume
                    }
                    for idx, r in zip(df.index, df.itertuples(index=False))
                ]
                
                if rows:
                    data_store.save_intraday_rows(rows)


            # After full save → append only newest candle
            else:

                latest_idx = df.index[-1].tz_localize(None)
                latest_row = df.iloc[-1]

                data_store.save_intraday({
                    "Symbol": symbol,
                    "Datetime": latest_idx.strftime("%Y-%m-%d %H:%M:%S"),
                    "Open": latest_row["Open"],
                    "High": latest_row["High"],
                    "Low": latest_row["Low"],
                    "Close": latest_row["Close"],
                    "Volume": latest_row["Volume"]
                })

            # =========================
            # 🚫 Phase 1: Skip Strategy1 entirely
            # =========================
            if not run_strategy1:
                # Only store data, skip ALL strategy logic
                return None

            # -------------------------
            # Historical context
            # -------------------------
            context = self.historical_context.get(symbol)

            if context is None:
                self.logger.warning(f"No historical context for {symbol}")
                return None

            daily = context.get("daily")
            hist6m = context.get("hist6m")
            hist1y = context.get("hist1y")
            hist_short = context.get("hist_short")
            daily_prev = context.get("daily_prev")

            # -------------------------
            # Premarket finished log
            # -------------------------
            if not getattr(self, "_premarket_logged", False):

                self.logger.info("Premarket download finished for all symbols.")
                self._premarket_logged = True

            # -------------------------
            # Run Strategy1
            # -------------------------
            data_store.save_strategy1_input(
                symbol,
                df,
                daily,
                hist6m,
                hist1y,
                hist_short,
                daily_prev
            )
            
            strategy_start = t.perf_counter()

            self.logger.info(f"[STRATEGY1] Running for {symbol}")
            
            s1_result = run_strategy_1(
                symbol=symbol,
                intraday=df,
                daily=daily,
                hist6m=hist6m,
                hist1y=hist1y,
                hist_short=hist_short,
                daily_prev=daily_prev,
                logger=self.logger
            )

            strategy_time = t.perf_counter() - strategy_start

            self.logger.info(
                f"STRATEGY1_COMPLETE symbol={symbol} duration={strategy_time:.4f}s"
            )

            if not s1_result:
                return None
            
            first_idx = s1_result.get("first_real_buy_idx")

            # Candidate condition: first_real_buy_idx must be numeric and not NaN
            if (
                first_idx is not None
                and isinstance(first_idx, numbers.Number)
                and not pd.isna(first_idx)
                and symbol not in self.processed_symbols
            ):
                
                candidate = {
                    "symbol": symbol,
                    "timestamp": now_ny.isoformat(),
                    "first_real_buy_idx": first_idx,
                    "strategy": "strategy1",
                
                    # 🔥 FIX — PASS CONTEXT FOR STRATEGY2
                    "hist_context": context
                }

                # Save (optional but good for audit)
                data_store.save_candidate(candidate)
                
                # 🔥 CRITICAL: push to pipeline
                self.candidate_queue.enqueue(candidate)
                
                self.processed_symbols.add(symbol)

                self.logger.info(
                    f"[CANDIDATE] {symbol} PASSED Strategy1 (first_idx={first_idx})"
                )
                                
                return candidate

        except Exception as e:

            self.logger.exception(f"{symbol} scan error")

        return None

    # =========================
    # Normalize DataFrame
    # =========================
    def _normalize(self, df):
        """
        Capitalize columns, flatten MultiIndex if necessary,
        and convert timestamps to NY market timezone.
        """

        if df is None or df.empty:
            return None

        # If input is Series, convert to DataFrame
        if isinstance(df, pd.Series):
            df = df.to_frame().T

        # Flatten MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Capitalize column names
        df.columns = [str(c).capitalize() for c in df.columns]

        # -------------------------
        # Convert timezone (UTC → NY)
        # -------------------------
        try:

            ny_tz = pytz.timezone("America/New_York")

            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")

            df.index = df.index.tz_convert(ny_tz)

        except Exception as e:

            self.logger.warning(f"Timezone normalization failed: {e}")

        # Remove duplicate timestamps (Yahoo bug)
        df = df[~df.index.duplicated(keep="last")]

        # -------------------------
        # Remove empty Yahoo candles
        # -------------------------
        try:

            # Drop rows where all price fields are NaN
            df = df.dropna(subset=["Open", "High", "Low", "Close"], how="all")

            # Drop rows where Volume is NaN
            if "Volume" in df.columns:
                df = df[df["Volume"].notna()]

            # Optional: remove rows where volume == 0 AND prices identical
            # (common Yahoo placeholder candle)
            df = df[
                ~(
                    (df["Volume"] == 0) &
                    (df["Open"] == df["High"]) &
                    (df["High"] == df["Low"]) &
                    (df["Low"] == df["Close"])
                )
            ]

        except Exception as e:
            self.logger.warning(f"Data cleaning failed: {e}")

        if df.empty:
            return None

        return df
