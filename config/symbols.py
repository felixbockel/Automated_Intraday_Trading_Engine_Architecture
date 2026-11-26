# ===============================
# CONFIG/SYMBOLS
# ===============================

symbols = ['AAPL',...] # hundreds of tickers

# Optional: helper to filter or clean symbols dynamically
def get_cleaned_symbols(exclude_symbols=None):
    exclude_symbols = exclude_symbols or set()
    return [s for s in symbols if s not in exclude_symbols]
