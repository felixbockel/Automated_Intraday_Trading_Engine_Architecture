# ===============================
# CONFIG/SETTINGS
# ===============================

from pathlib import Path
from datetime import datetime

# ---------------------------------------------------
# MARKET UNIVERSE FILE
# ---------------------------------------------------
SYMBOL_UNIVERSE_FILE = "data/symbol_universe.csv"

def load_symbol_universe():
    import pandas as pd
    df = pd.read_csv(SYMBOL_UNIVERSE_FILE)
    return df["symbol"].tolist()

# ---------------------------------------------------
# SCANNER SETTINGS
# ---------------------------------------------------
SCAN_INTERVAL_SECONDS = 300  # 5 minutes
MAX_SYMBOLS = 980

# ---------------------------------------------------
# IBKR SETTINGS
# ---------------------------------------------------
IBKR_HOST = "127.0.0.1"
IBKR_PORT = 4002
IBKR_CLIENT_ID = 101  # default clientId for hybrid engine

# ---------------------------------------------------
# DATA DIRECTORY
# ---------------------------------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

TODAY = datetime.now().strftime("%Y%m%d")

# Intraday 5-min bars from Yahoo
INTRADAY_YAHOO_FILE = DATA_DIR / f"intraday_yahoo_5min_{TODAY}.csv"

# Candidates CSV (grows dynamically during day)
CANDIDATES_FILE = DATA_DIR / f"candidates_{TODAY}.csv"

# Trades CSV log
TRADES_FILE = DATA_DIR / f"trades_{TODAY}.csv"

# Pre-market context (historical support/resistance)
PREMARKET_CONTEXT_FILE = DATA_DIR / f"premarket_context_{TODAY}.csv"

# ---------------------------------------------------
# QUEUE SETTINGS
# ---------------------------------------------------
CANDIDATE_QUEUE_SIZE = 1000

# ---------------------------------------------------
# TRADING RULES
# ---------------------------------------------------
MAX_TRADES_PER_DAY = 1
POSITION_SIZE = 1  # can be adjusted to number of contracts/shares

# ---------------------------------------------------
# HISTORICAL LOOKBACK SETTINGS
# ---------------------------------------------------
HIST_6M_DAYS = 126    # ~6 months trading days
HIST_1Y_DAYS = 252    # ~1 year trading days
SHORT_TERM_DAYS = 7   # 1-hour bars, 7-day rolling for flexible S/R
SHORT_TERM_WINDOW = 12 # Rolling lookback for highs/lows

# ---------------------------------------------------
# ROLLING BARS & BATCH SETTINGS
# ---------------------------------------------------
ROLLING_BARS = 30        # for strategy comparison
MAX_WORKERS = 24         # CPU-bound batch execution
STREAM_SECONDS = 20      # focus mode streaming pacing

# ---------------------------------------------------
# PREMARKET DOWNLOAD SETTINGS
# ---------------------------------------------------
PREMARKET_SLEEP_SECONDS = 2      # pause between Yahoo premarket downloads (seconds)
