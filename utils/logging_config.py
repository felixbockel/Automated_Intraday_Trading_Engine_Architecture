# ===============================
# UTILS/LOGGING_CONFIG
# ===============================

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from datetime import datetime
from shutil import rmtree  # add this import at the top if not already

# -----------------------------
# Paths
# -----------------------------
LOG_DIR = Path("logs")
# -----------------------------
# Clear previous logs
# -----------------------------
if LOG_DIR.exists():
    rmtree(LOG_DIR)  # remove all old logs
LOG_DIR.mkdir(parents=True, exist_ok=True)  # recreate empty folder

TODAY = datetime.now().strftime("%Y%m%d")
LOG_FILE = LOG_DIR / f"hybrid_engine_{TODAY}.log"

# -----------------------------
# Logger Setup
# -----------------------------
def setup_logging(name="HybridEngineLogger"):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():  # ← only add handlers once
        logger.setLevel(logging.INFO)

        # Console Handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        ch.setFormatter(ch_formatter)
        logger.addHandler(ch)

        # File Handler
        fh = TimedRotatingFileHandler(
            filename=LOG_FILE,
            when="midnight",
            backupCount=7,
            encoding="utf-8"
        )
        fh.setLevel(logging.INFO)
        fh_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        fh.setFormatter(fh_formatter)
        logger.addHandler(fh)

        logger.info("Logger initialized")
    return logger
