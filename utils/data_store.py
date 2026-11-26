# ===============================
# utils/data_store.py
# ===============================

import pandas as pd
from pathlib import Path
from threading import Lock
from datetime import datetime
import pytz
from shutil import rmtree
import os
from pandas.errors import EmptyDataError

class DataStore:

    # Only used in save_premarket_rows for debugging
    symbol_testing = ["BAX"]
    
    def __init__(self):
        self.lock = Lock()
        self.base = Path("data")

        # Remove previous run data
        if self.base.exists():
            rmtree(self.base)

        self.intraday_dir = self.base / "intraday"
        self.yahoo_raw_dir = self.base / "yahoo_raw"
        self.premarket_dir = self.base / "premarket"
        self.strategy1_dir = self.base / "strategy_inputs" / "strategy1"
        self.strategy2_dir = self.base / "strategy_inputs" / "strategy2"
        self.ibkr_raw_dir = self.base / "ibkr_raw"
        self.ibkr_intraday_dir = self.base / "ibkr_intraday"

        self.candidates_file = self.base / "candidates.csv"
        self.trades_file = self.base / "trades.csv"

        # Recreate folders
        for d in [
            self.intraday_dir,
            self.yahoo_raw_dir,
            self.premarket_dir,
            self.strategy1_dir,
            self.strategy2_dir,
            self.ibkr_raw_dir,
            self.ibkr_intraday_dir
        ]:
            d.mkdir(parents=True, exist_ok=True)

    # =========================
    # Intraday storage (multiple rows)
    # =========================
    def save_intraday_rows(self, rows):

        if not rows:
            return

        with self.lock:

            df = pd.DataFrame(rows)

            date = pd.to_datetime(df["Datetime"].iloc[0]).strftime("%Y-%m-%d")

            file = self.intraday_dir / f"{date}.parquet"

            if file.exists():
                existing = pd.read_parquet(file)
                df = pd.concat([existing, df], ignore_index=True)

            df.to_parquet(file, index=False)

    # =========================
    # Intraday storage (single row)
    # =========================
    def save_intraday(self, row):
        """
        Save a single intraday row (dict) safely.
        Wraps save_intraday_rows to reuse existing logic.
        """
        if not row:
            return

        # Wrap the single row in a list and call save_intraday_rows
        self.save_intraday_rows([row])

    # =========================
    # Yahoo raw snapshot
    # =========================

    def save_yahoo_snapshot(self, df):

        with self.lock:

            ny = pytz.timezone("America/New_York")

            date = datetime.now(ny).strftime("%Y-%m-%d")

            file = self.yahoo_raw_dir / f"{date}.parquet"

            df.to_parquet(file)

    # =========================
    # Premarket historical
    # =========================

    def save_premarket_rows(self, rows):

        if not rows:
            return
    
        with self.lock:
    
            df = pd.DataFrame(rows)

            # ==========================
            # DEBUG STEP 3: SAVE CHECK
            # ==========================
            unique_symbols = df["Symbol"].unique()
            
            if "BAX" in unique_symbols:
            
                print("\n[DEBUG SAVE] Symbols in this batch:", unique_symbols)
            
                bax_df = df[df["Symbol"] == "BAX"]
            
                print("[DEBUG SAVE] BAX sample:")
                print(bax_df[["Close", "BarType"]].head(5))
            
                print("[DEBUG SAVE] BAX max close:", bax_df["Close"].max())

            
            # Filter only the testing symbols
            df = df[df["Symbol"].isin(self.symbol_testing)]
            # If nothing left, skip saving
            if df.empty:
                return
    
            date = df["Datetime"].iloc[0].strftime("%Y-%m-%d")
            file = self.premarket_dir / f"{date}.parquet"
    
            df.to_parquet(file, index=False)

    # =========================
    # Candidates
    # =========================
    def save_candidate(self, candidate):
        """
        Save a new candidate to candidates.csv.
        Drops the 'hist_context' field to keep CSV clean.
        Appends only if the candidate 
        """    
        with self.lock:
    
            candidate_to_save = candidate.copy()
            candidate_to_save.pop("hist_context", None)
    
            df = pd.DataFrame([candidate_to_save])
    
            write_header = not self.candidates_file.exists()
    
            df.to_csv(
                self.candidates_file,
                mode="a",
                header=write_header,
                index=False
            )

    # =========================
    # Trades
    # =========================

    def save_trade(self, trade):

        df = pd.DataFrame([trade])

        if self.trades_file.exists():

            existing = pd.read_csv(self.trades_file)

            df = pd.concat([existing, df], ignore_index=True)

        df.to_csv(self.trades_file, index=False)

    # =========================
    # IBKR raw snapshot
    # =========================
    def save_ibkr_snapshot(self, df):

        if df is None or df.empty:
            return
    
        symbol = df["Symbol"].iloc[0]
        day = datetime.utcnow().strftime("%Y-%m-%d")
    
        folder = self.ibkr_raw_dir / day
        folder.mkdir(parents=True, exist_ok=True)
    
        file = folder / f"{symbol}.parquet"
    
        with self.lock:
    
            if file.exists():
                existing = pd.read_parquet(file)
                df = pd.concat([existing, df], ignore_index=True)
    
            df.to_parquet(file, engine="pyarrow", compression="snappy", index=False)
    
    
    # =========================
    # IBKR intraday dataset
    # =========================
    def save_ibkr_intraday(self, rows):

        if not rows:
            return
    
        df = pd.DataFrame(rows)
    
        # Extract metadata
        symbol = df["Symbol"].iloc[0]
        day = datetime.utcnow().strftime("%Y-%m-%d")
    
        # Folder: data/ibkr_intraday/YYYY-MM-DD/
        folder = self.ibkr_intraday_dir / day
        folder.mkdir(parents=True, exist_ok=True)
    
        file = folder / f"{symbol}.parquet"
    
        with self.lock:
    
            if file.exists():
                existing = pd.read_parquet(file)
                df = pd.concat([existing, df], ignore_index=True)
    
            df.to_parquet(file, engine="pyarrow", compression="snappy", index=False)

    # =========================
    # Strategy1 input snapshot
    # =========================
    
    def save_strategy1_input(self, symbol, intraday, daily, hist6m, hist1y, hist_short, daily_prev):
    
        with self.lock:
    
            ny = pytz.timezone("America/New_York")
            date = datetime.now(ny).strftime("%Y-%m-%d")
    
            folder = self.strategy1_dir / date
            folder.mkdir(exist_ok=True)
    
            file = folder / f"{symbol}.parquet"
    
            data = {
                "intraday": intraday,
                "daily": daily,
                "hist6m": hist6m,
                "hist1y": hist1y,
                "hist_short": hist_short,
                "daily_prev": daily_prev
            }
    
            combined = []
    
            for key, df in data.items():
    
                if df is None or df.empty:
                    continue
    
                temp = df.copy()
                temp["dataset"] = key
                temp["symbol"] = symbol
    
                combined.append(temp)
    
            if combined:
    
                result = pd.concat(combined)
                result.to_parquet(file)


    # =========================
    # Strategy2 input snapshot
    # =========================
    
    def save_strategy2_input(self, symbol, bars, hist_context):
    
        with self.lock:
    
            ny = pytz.timezone("America/New_York")
            date = datetime.now(ny).strftime("%Y-%m-%d")
    
            folder = self.strategy2_dir / date
            folder.mkdir(exist_ok=True)
    
            file = folder / f"{symbol}.parquet"
    
            combined = []
    
            if bars is not None and not bars.empty:
    
                temp = bars.copy()
                temp["dataset"] = "bars"
                temp["symbol"] = symbol
    
                combined.append(temp)
    
            for key, df in hist_context.items():
    
                if df is None or df.empty:
                    continue
    
                temp = df.copy()
                temp["dataset"] = key
                temp["symbol"] = symbol
    
                combined.append(temp)
    
            if combined:
    
                result = pd.concat(combined)
                result.to_parquet(file)

data_store = DataStore()
