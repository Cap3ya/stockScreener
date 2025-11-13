import requests
import pandas as pd
from io import StringIO
from .config import Config

class TickerDataSource:
    """Fetches active stock tickers from Nasdaq Trader."""

    def __init__(self, logger):
        self.logger = logger
        self.session = requests.Session()

    def _fetch_symbols(self, url, symbol_col, name_col):
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text), sep="|")[:-1]
            df[symbol_col] = df[symbol_col].astype(str).str.strip()
            df[name_col] = df[name_col].astype(str).str.strip()
            filtered = df[df[name_col].str.endswith(Config.VALID_SUFFIXES, na=False)]
            return filtered[symbol_col].tolist()
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return []

    def get_active_tickers(self):
        self.logger.info("Fetching active US tickers from NasdaqTrader...")
        nasdaq_url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
        other_url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

        tickers = (
            self._fetch_symbols(nasdaq_url, "Symbol", "Security Name") +
            self._fetch_symbols(other_url, "ACT Symbol", "Security Name")
        )
        self.logger.info(f"Retrieved {len(tickers)} active tickers.")
        return tickers
