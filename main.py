# ===============================
# MAIN.PY – SYSTEM ORCHESTRATOR
# ===============================

import asyncio
import signal
import traceback
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from ib_insync import IB,util
from pathlib import Path
from config.settings import SCAN_INTERVAL_SECONDS
from config.symbols import get_cleaned_symbols
from utils.logging_config import setup_logging
from utils.data_store import data_store

from core.ibkr_client import IBKRClient
from core.execution_engine import ExecutionEngine
from core.yahoo_scanner import YahooScanner
from core.candidate_queue import CandidateQueue
from core.candidate_worker import CandidateWorker
from core.focus_mode import FocusMode

import time
from pandas.errors import EmptyDataError
import re

from datetime import datetime, timedelta
import pytz
import os

util.patchAsyncio()

# ===============================
# wait +2–3 seconds so Yahoo candle is fully formed
# ===============================
NY_TZ = pytz.timezone("America/New_York")

def seconds_until_next_5min(buffer_seconds=3):
    now = datetime.now(pytz.UTC).astimezone(NY_TZ)

    # Next 5-min boundary
    next_minute = (now.minute // 5 + 1) * 5

    next_run = now.replace(second=0, microsecond=0)

    if next_minute >= 60:
        next_run = next_run.replace(minute=0) + timedelta(hours=1)
    else:
        next_run = next_run.replace(minute=next_minute)

    # Add buffer (wait for candle close)
    next_run += timedelta(seconds=buffer_seconds)

    return max((next_run - now).total_seconds(), 0)

# ===============================
# PREMARKET HISTORICAL LOADER
# ===============================

def normalize_yf_df(df):
    """
    Convert yfinance dataframe to flat, numeric-only structure.
    Handles MultiIndex columns, duplicates, and ensures float64.
    """

    if df is None or df.empty:
        return None

    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()

    # Flatten MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    # Deduplicate columns by keeping first
    df = df.loc[:, ~df.columns.duplicated()]

    # Capitalize
    df.columns = [str(c).capitalize() for c in df.columns]

    # Force numeric columns to float64 safely
    numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
    for col in numeric_cols:
        if col in df.columns:
            df.loc[:, col] = (
                pd.to_numeric(df[col], errors="coerce")
                .round(6)
            )

    return df



# ===============================
# load_premarket_context_safe.py
# ===============================

async def load_premarket_context(symbols, logger, batch_size=10, delay_between_batches=5):
    logger.info("PREMARKET_PULL_START")
    start_time = datetime.now()
    historical_context = {}
    failed_symbols = []

    # ----------------------
    # Helper: sanitize tickers for Yahoo
    # ----------------------
    def sanitize_symbol(symbol):
        # BRK/A → BRK-B, BF/B → BF-B, etc.
        return re.sub(r"[/.]", "-", symbol)

   
    # ----------------------
    # Core download per symbol
    # ----------------------
    def download_symbol(symbol, max_retries=5, delay_between_retries=5):
        y_symbol = sanitize_symbol(symbol)
        
        for attempt in range(1, max_retries + 1):
            try:    
                # --------------------------
                # STEP 2: CLEAN DOWNLOAD (Ticker.history)
                # --------------------------
                def yf_download_clean(symbol, period, interval):
                    data = yf.download(
                        symbol,
                        period=period,
                        interval=interval,
                        auto_adjust=True,
                        progress=False,
                        threads=False
                    )
                
                    if data is None or data.empty:
                        return None
                
                    # Flatten MultiIndex if present
                    if isinstance(data.columns, pd.MultiIndex):
                        data.columns = data.columns.get_level_values(0)
                
                    # Ensure datetime index
                    data.index = pd.to_datetime(data.index)
                
                    # Drop bad rows
                    data.dropna(inplace=True)
                
                    # Normalize column names
                    data.columns = [str(c).capitalize() for c in data.columns]
                
                    # -----------------------------
                    # FORCE FLOAT64 ON ALL NUMERIC COLUMNS
                    # -----------------------------
                    for col in data.select_dtypes(include=['number']).columns:
                        data[col] = data[col].astype(float)
                
                    return normalize_yf_df(data)

                daily = yf_download_clean(y_symbol, "60d", "1d")
                hist6m = yf_download_clean(y_symbol, "6mo", "1d")
                hist1y = yf_download_clean(y_symbol, "1y", "1d")
                hist_short = yf_download_clean(y_symbol, "6d", "1h")
                
                # --------------------------
                # BASIC VALIDATION (minimal & fast)
                # --------------------------
                if hist6m is None or hist6m.empty or hist1y is None or hist1y.empty:
                
                    logger.warning(f"{symbol}: missing critical historical data → skipping")
                
                    failed_symbols.append({
                        "Symbol": symbol,
                        "Reason": "Missing hist6m or hist1y"
                    })
                
                    return symbol, None
                    
                # --------------------------
                # PREVIOUS DAY
                # --------------------------
                daily_prev = hist1y.iloc[[-1]] if hist1y is not None and not hist1y.empty else None
    
                # --------------------------
                # TIMEZONE NORMALIZATION
                # --------------------------
                for df in [daily, hist6m, hist1y, hist_short]:
                    if df is not None and not df.empty:
                        if df.index.tz is None:
                            df.index = df.index.tz_localize("UTC")
                        df.index = df.index.tz_convert("America/New_York")
    
                # --------------------------
                # SAVE ROWS
                # --------------------------
                rows = []
    
                def append_rows(df, bar_type, sym):
                    if df is None or df.empty:
                        return
    
                    for idx, row in df.iterrows():
                        rows.append({
                            "Symbol": sym,
                            "Datetime": idx,
                            "Open": row.get("Open"),
                            "High": row.get("High"),
                            "Low": row.get("Low"),
                            "Close": row.get("Close"),
                            "Volume": row.get("Volume"),
                            "BarType": bar_type
                        })
    
                append_rows(hist6m, "DAILY_6M", symbol)
                append_rows(hist1y, "DAILY_1Y", symbol)
                append_rows(hist_short, "HOURLY_7D", symbol)
                append_rows(daily_prev, "PREV_DAY", symbol)
    
                if rows:
                    data_store.save_premarket_rows(rows)
    
                # --------------------------
                # THROTTLE (IMPORTANT)
                # --------------------------
                time.sleep(1.5)
    
                return symbol, {
                    "daily": daily,
                    "hist6m": hist6m,
                    "hist1y": hist1y,
                    "hist_short": hist_short,
                    "daily_prev": daily_prev
                }
    
            except Exception as e:
                msg = str(e).lower()
    
                if "too many requests" in msg:
                    logger.warning(
                        f"Rate limited for {symbol}. "
                        f"Retrying in {delay_between_retries}s (attempt {attempt})"
                    )
                    time.sleep(delay_between_retries)
    
                else:
                    failed_symbols.append({"Symbol": symbol, "Reason": str(e)})
                    logger.warning(f"Premarket download failed for {symbol}: {e}")
                    return symbol, None
    
        failed_symbols.append({"Symbol": symbol, "Reason": f"Failed after {max_retries} attempts"})
        logger.warning(f"Premarket download failed for {symbol} after {max_retries} attempts")
    
        return symbol, None
    # ----------------------
    # Sequential download (no batching)
    # ----------------------
    total_symbols = len(symbols)
    
    for idx, symbol in enumerate(symbols, start=1):
    
        logger.info(f"Downloading {idx}/{total_symbols}: {symbol}")
    
        symbol, result = download_symbol(symbol)
    
        if result:
            historical_context[symbol] = result

    # ----------------------
    # Summary logging
    # ----------------------
    elapsed = (datetime.now() - start_time).total_seconds()
    success_count = len(historical_context)
    failed_count = len(failed_symbols)
    logger.info(f"PREMARKET_PULL_END duration={elapsed:.2f}s")
    logger.info(f"Premarket download success: {success_count}/{total_symbols}")
    if failed_symbols:
        logger.warning(f"Premarket download failed for {failed_count} tickers")
        failed_df = pd.DataFrame(failed_symbols)
        data_folder = Path("data")
        data_folder.mkdir(exist_ok=True)
        failed_file = data_folder / "premarket_failed_symbols.csv"
        failed_df.to_csv(failed_file, index=False)
        logger.warning(f"Failed tickers and reasons saved to: {failed_file}")

    return historical_context


# ===============================
# MAIN ENGINE
# ===============================

async def main():

    logger = setup_logging()

    logger.info("Starting Intraday Trading Engine (Option B)")

    # -----------------------------
    # Load universe
    # -----------------------------
    symbols = get_cleaned_symbols()
    logger.info(f"Loaded {len(symbols)} symbols for scanning")

    # -----------------------------
    # Premarket historical pull
    # -----------------------------
    historical_context = await load_premarket_context(symbols, logger)
    
    logger.info(f"Premarket context loaded for {len(historical_context)} symbols")

    # -----------------------------
    # Setup IBKR
    # -----------------------------
    ib = IB()
    await ib.connectAsync("127.0.0.1", 4002, clientId=1)
    logger.info(f"IBKR_CONNECTED_STATUS {ib.isConnected()}")
    logger.info(f"IBKR_CONNECTION_OBJECT {ib}")

    # -----------------------------
    # Setup system components
    # -----------------------------

    ibkr = IBKRClient(ib, logger=logger)
    
    execution_engine = ExecutionEngine(ibkr.ib, logger=logger)
    
    # ✅ Create queue FIRST
    candidate_queue = CandidateQueue(logger=logger)
    
    # ✅ Pass it into scanner
    yahoo_scanner = YahooScanner(
        symbols,
        historical_context,
        logger,
        candidate_queue
    )

    candidate_worker = CandidateWorker(
        ibkr,
        candidate_queue,
        execution_engine,
        logger=logger
    )

    focus_mode = FocusMode(
        ibkr,
        execution_engine,
        logger
    )

    active_position = None   # (already exists later in orchestrator, but OK)
    
    yahoo_scanner.is_active = lambda: active_position is not None
    candidate_worker.is_active = lambda: active_position is not None
    
    # -----------------------------
    # Shutdown event handling
    # -----------------------------
    shutdown_event = asyncio.Event()

    def shutdown_handler(signum, frame):

        logger.info("Shutdown signal received")

        shutdown_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    async def schedule_autoctrlc():
        now = datetime.now(NY_TZ)
        market_close_dt = now.replace(hour=16, minute=0, second=0, microsecond=0)
        delay = (market_close_dt - now).total_seconds() + 5 * 60
    
        if delay > 0:
            await asyncio.sleep(delay)
    
        logger.info("Auto-triggering Ctrl+C (SIGINT) 5min after market close")
        os.kill(os.getpid(), signal.SIGINT)
    
    # -----------------------------
    # Run tasks
    # -----------------------------
    tasks = []
    
    async def orchestrator_loop():
        cycle = 0
        
        # ✅ ADD THIS
        active_position = None
        trading_finished = False
    
        while not shutdown_event.is_set():
            cycle += 1
            cycle_start = time.perf_counter()
    
            current_time = datetime.now(NY_TZ).time()
    
            t_1135 = datetime.strptime("11:35", "%H:%M").time()
            t_1200 = datetime.strptime("12:00", "%H:%M").time()
    
            logger.info(f"CYCLE_START {cycle}")
    
            # =========================
            # 🟢 PHASE 1: Before 11:35
            # =========================
            if current_time < t_1135:
                logger.info("PHASE_1: Yahoo scan ONLY (no Strategy1)")
    
                # Only fetch/store data, no signals
                yahoo_scanner.run_cycle(run_strategy1=False)
    
            # =========================
            # 🟡 PHASE 2: 11:35–11:55
            # =========================
            elif current_time < t_1200:
                logger.info("PHASE_2: Yahoo + Strategy1 (build candidates)")
    
                new_candidates = yahoo_scanner.run_cycle(run_strategy1=True)
    
                logger.info(f"CANDIDATES_BUILT {len(new_candidates)}")
    
            # =========================
            # 🔴 PHASE 3: After 11:55
            # =========================
            else:
                logger.info("PHASE_3")
            
                # ---------------------------------
                # 🚫 HARD STOP: trading finished
                # ---------------------------------
                if trading_finished:
                    logger.info("TRADING_FINISHED → No further actions")
                
                # ---------------------------------
                # 🟢 NO POSITION → normal behavior
                # ---------------------------------
                elif active_position is None:
            
                    logger.info("NO ACTIVE POSITION → Running CandidateWorker")
            
                    buy_result = await candidate_worker.run_once_parallel()
            
                    if buy_result:
                        active_position = buy_result
                        active_symbol = buy_result["symbol"]

            
                        logger.info(f"BUY LOCKED → {active_symbol}")
                        logger.info(f"Starting FocusMode monitoring for {active_symbol}")
            
                        async def run_focus_mode():
                            nonlocal active_position, trading_finished
                        
                            logger.info(f"FOCUS_MODE_TASK_STARTED → {active_symbol}")
                        
                            result = await focus_mode.start(
                                symbol=active_symbol,
                                shutdown_event=shutdown_event,
                                first_real_buy_idx_secondary=buy_result.get("first_real_buy_idx_secondary"),
                                hist_context=buy_result.get("hist_context")
                            )
                        
                            logger.info(f"FOCUS_MODE_EXIT → {active_symbol} result={result}")
                        
                            # 🔴 POSITION CLOSED → lock system for rest of day
                            active_position = None
                            trading_finished = True
                        
                            logger.info("TRADING_FINISHED set to TRUE")
                        
                        # Run in background (non-blocking)
                        asyncio.create_task(run_focus_mode())
            
                    else:
                        logger.info("No BUY executed this cycle")
            
                    # Only generate new candidates if STILL no position
                    if active_position is None:
                        new_candidates = yahoo_scanner.run_cycle(run_strategy1=True)
                        logger.info(f"YAHOO_CANDIDATES_PRODUCED {len(new_candidates)}")
            
                # ---------------------------------
                # 🔵 POSITION ACTIVE → ONLY FOCUS MODE
                # ---------------------------------
                else:
                    active_symbol = active_position["symbol"]

                    logger.info(f"ACTIVE POSITION → {active_symbol}")
                    logger.info("SCANNER + STRATEGY1 + STRATEGY2 PAUSED")
            
                    # ❗ ONLY IBKR should run for this symbol (handled inside FocusMode)
                    # DO NOTHING ELSE HERE
    
    
            # =========================
            # ⏱ Align to next 5-min candle
            # =========================
            sleep_time = seconds_until_next_5min(buffer_seconds=3)
    
            logger.info(f"NEXT_CYCLE_SLEEP {sleep_time:.2f}s")
    
            await asyncio.sleep(sleep_time)
    
            cycle_duration = time.perf_counter() - cycle_start
            logger.info(f"CYCLE_END {cycle} duration={cycle_duration:.2f}s")
    
    # Start Auto exist 5min after market is close
    tasks.append(asyncio.create_task(schedule_autoctrlc()))
    
    # Start orchestrator
    tasks.append(asyncio.create_task(orchestrator_loop()))

    # -----------------------------
    # Wait for shutdown
    # -----------------------------
    await shutdown_event.wait()

    logger.info("Shutdown initiated. Cancelling tasks...")

    for task in tasks:

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

    ib.disconnect()

    logger.info("Intraday Trading Engine stopped cleanly.")


# ===============================
# WINDOWS ENTRY POINT
# ===============================
if __name__ == "__main__":

    import multiprocessing

    multiprocessing.freeze_support()
    multiprocessing.set_start_method("spawn", force=True)

    asyncio.run(main())
