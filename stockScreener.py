import requests
import yfinance as yf
import pandas as pd
import numpy as np
import logging
from io import StringIO
from tqdm import tqdm
import os

# ================================
# Configuration
# ================================
class Config:
    VALID_SUFFIXES = (" Stock", " Shares")
    OUTPUT_DIR = "/home/capeya/stockScreener"
    LOG_FILE = os.path.join(OUTPUT_DIR, "stockScreener_log.log")
    CSV_FILE = os.path.join(OUTPUT_DIR, "stockScreener_Output.csv")
    BAD_TICKERS_FILE = os.path.join(OUTPUT_DIR, "stockScreener_badTickers.txt")

# ================================
# Logging setup
# ================================
class Logger:
    @staticmethod
    def setup():
        os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(Config.LOG_FILE, mode='w')
            ]
        )
        return logging.getLogger(__name__)


# ================================
# Bad ticker handling
# ================================
class BadTickerManager:
    def __init__(self, file_path):
        self.file_path = file_path
        self.bad_tickers = set()

    def load(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, "r") as f:
                self.bad_tickers = set(line.strip() for line in f if line.strip())
        return self.bad_tickers

    def save(self, new_bad_tickers):
        self.bad_tickers.update(new_bad_tickers)
        with open(self.file_path, "w") as f:
            for ticker in sorted(self.bad_tickers):
                f.write(f"{ticker}\n")


# ================================
# Fetching tickers from NasdaqTrader
# ================================
class TickerDataSource:
    def __init__(self):
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
            logger.error(f"Error fetching {url}: {e}")
            return []

    def get_active_tickers(self):
        logger.info("Fetching active US tickers from NasdaqTrader...")
        nasdaq_url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
        other_url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

        tickers = (
            self._fetch_symbols(nasdaq_url, "Symbol", "Security Name") +
            self._fetch_symbols(other_url, "ACT Symbol", "Security Name")
        )
        logger.info(f"Retrieved {len(tickers)} active tickers.")
        return tickers


# ================================
# Analyzer
# ================================
class TickerAnalyzer:
    @staticmethod
    def analyze(ticker):
        try:
            dat = yf.Ticker(ticker)
            df = dat.history(period="1y", interval="1d", auto_adjust=True)

            if df.empty or not {'Close', 'Volume', "High", "Low"}.issubset(df.columns):
                raise ValueError("No valid OHLCV data")

            # --- Price Volume ---
            df['DollarVolume'] = df['Volume'] * df['Close']
            dollar_volume = df['DollarVolume'].iloc[-1]

            # --- Sharpe Ratio ---
            df['Returns'] = np.log(df['Close'] / df['Close'].shift(1))
            mean_return = df['Returns'].mean()
            std_return = df['Returns'].std()

            sharpe = 0 if std_return == 0 else mean_return / std_return

            # --- Parkinson Volatility ---
            df["Parkinson_Var"] = (np.log(df["High"] / df["Low"]) ** 2) / (4.0 * np.log(2.0))
            df["Parkinson_Vol_Daily"] = np.sqrt(df["Parkinson_Var"])
            # 21-day rolling average variance, then annualize
            df["AvgVar_21"] = df["Parkinson_Var"].rolling(window=21).mean()
            df["Parkinson_Vol_21d_Annualized"] = np.sqrt(df["AvgVar_21"] * 365)
            # Current annualized Parkinson volatility
            current_parkinson_vol = df["Parkinson_Vol_21d_Annualized"].iloc[-1]
            # Percentile of current volatility vs historical daily volatilities
            vol_percentile = df["Parkinson_Vol_21d_Annualized"].rank(pct=True).iloc[-1].round(2)            

            return {
                'Ticker': ticker,
                '$Volume': dollar_volume,
                'Sharpe': sharpe,
                'Parkinson': round(current_parkinson_vol, 2), 
                'ParkinsonPctl': vol_percentile,
            }

        except Exception as e:
            logger.error(f"{ticker}: Failed ({e})")
            return None

# ================================
# Orchestrator
# ================================
class AnalysisOrchestrator:
    def __init__(self):
        self.bad_ticker_manager = BadTickerManager(Config.BAD_TICKERS_FILE)
        self.ticker_source = TickerDataSource()
        self.results = []
        self.new_bad_tickers = set()

    def _process_ticker(self, ticker): 
        result = TickerAnalyzer.analyze(ticker) 
        if result is None: 
            self.new_bad_tickers.add(ticker) 
        else: 
            self.results.append(result)
            
    def run(self):
        bad_tickers = self.bad_ticker_manager.load()
        tickers = [t for t in self.ticker_source.get_active_tickers() if t not in bad_tickers]

        logger.info(f"Processing {len(tickers)} tickers...")
        for ticker in tqdm(tickers, desc="Tickers", unit="ticker"):
            self._process_ticker(ticker)

        if self.new_bad_tickers:
            self.bad_ticker_manager.save(self.new_bad_tickers)

        if not self.results:
            logger.warning("No valid tickers found.")
            return

        df = pd.DataFrame(self.results)
        # --- Compute cross-sectional percentile ---
        df['$VolumePctl'] = df['$Volume'].rank(pct=True).round(3)
        df['SharpePctl'] = df['Sharpe'].rank(pct=True).round(3)
        # --- Remove raw column to clean up output ---
        df.drop(columns=['$Volume'], inplace=True)
        df.drop(columns=['Sharpe'], inplace=True)
        # --- Composite metrics ---
        df['$VolxSharpe'] = (df['$VolumePctl'] * df['SharpePctl']).round(3)
        # --- Sort by the composite metric ---
        df = df.sort_values(by='$VolxSharpe', ascending=False)
        # --- Reorder columns ---
        desired_order = [
            'Ticker',
            '$VolumePctl',
            'SharpePctl',
            '$VolxSharpe',
            'Parkinson',
            'ParkinsonPctl',
        ]
        df = df[[col for col in desired_order if col in df.columns]]

        logger.info("\nTop 50 tickers by composite percentile:")
        logger.info(f"\n{df.head(50).to_string(index=False)}")

        df.to_csv(Config.CSV_FILE, index=False)
        logger.info(f"Results saved to {Config.CSV_FILE}")

        if self.new_bad_tickers:
            logger.warning(f"{len(self.new_bad_tickers)} new tickers failed. Added to bad tickers list.")

# ================================
# Main
# ================================
if __name__ == "__main__":
    logger = Logger.setup()
    orchestrator = AnalysisOrchestrator()
    orchestrator.run()
