import pandas as pd
from tqdm import tqdm
from .config import Config
from .bad_ticker_manager import BadTickerManager
from .ticker_data_source import TickerDataSource
from .ticker_analyzer import TickerAnalyzer

class AnalysisOrchestrator:
    """Coordinates fetching, analysis, and saving of ticker data."""

    def __init__(self, logger):
        self.logger = logger
        self.bad_ticker_manager = BadTickerManager(Config.BAD_TICKERS_FILE)
        self.ticker_source = TickerDataSource(logger)
        self.analyzer = TickerAnalyzer(logger)
        self.results = []
        self.new_bad_tickers = set()

    def _process_ticker(self, ticker):
        result = self.analyzer.analyze(ticker)
        if result is None:
            self.new_bad_tickers.add(ticker)
        elif result == "SKIP":
            return
        else:
            self.results.append(result)

    def run(self):
        bad_tickers = self.bad_ticker_manager.load()
        tickers = [t for t in self.ticker_source.get_active_tickers() if t not in bad_tickers][:100]

        self.logger.info(f"Processing {len(tickers)} tickers...")
        for ticker in tqdm(tickers, desc="Tickers", unit="ticker"):
            self._process_ticker(ticker)

        if self.new_bad_tickers:
            self.bad_ticker_manager.save(self.new_bad_tickers)

        if not self.results:
            self.logger.warning("No valid tickers found.")
            return

        df = pd.DataFrame(self.results)
        df["$Volume(M)"] = (df["$Volume"] / 1_000_000).round(2)  # Convert to millions
        df['$VolumePctl'] = df['$Volume'].rank(pct=True).round(3)
        df['SharpePctl'] = df['Sharpe'].rank(pct=True).round(3)
        # df.drop(columns=['$Volume', 'Sharpe'], inplace=True)
        df['$VolxSharpe'] = (df['$VolumePctl'] * df['SharpePctl']).round(3)

        df = df.sort_values(by='StreakCount', ascending=False)
        # desired_order = [
        #     'Ticker', '$VolumePctl', 'SharpePctl', '$VolxSharpe',
        #     'Parkinson', 'ParkinsonPctl',
        # ]
        desired_order = [
            'Ticker', 'StreakCount', 'StreakSource', '$Volume(M)',
        ]
        df = df[[col for col in desired_order if col in df.columns]]

        self.logger.info("\nTop 50 tickers by composite percentile:")
        self.logger.info(f"\n{df.head(50).to_string(index=False)}")

        df.to_csv(Config.CSV_FILE, index=False)
        self.logger.info(f"Results saved to {Config.CSV_FILE}")

        if self.new_bad_tickers:
            self.logger.warning(f"{len(self.new_bad_tickers)} new tickers failed. Added to bad tickers list.")
