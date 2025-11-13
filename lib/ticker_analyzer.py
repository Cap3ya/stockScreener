import yfinance as yf
import numpy as np
import pandas as pd

class TickerAnalyzer:
    """Analyzes a single ticker using price and volatility metrics."""

    def __init__(self, logger):
        self.logger = logger

    def analyze(self, ticker: str):
        try:
            dat = yf.Ticker(ticker)
            df = dat.history(period="1y", interval="1d", auto_adjust=True)

            if df.empty or not {'Close', 'Volume', "High", "Low"}.issubset(df.columns):
                raise ValueError("No valid OHLCV data")

            df['DollarVolume'] = df['Volume'] * df['Close']
            dollar_volume = df['DollarVolume'].iloc[-1]

            df['Returns'] = np.log(df['Close'] / df['Close'].shift(1))
            mean_return = df['Returns'].mean()
            std_return = df['Returns'].std()
            sharpe = 0 if std_return == 0 else mean_return / std_return

            df["Parkinson_Var"] = (np.log(df["High"] / df["Low"]) ** 2) / (4.0 * np.log(2.0))
            df["Parkinson_Vol"] = np.sqrt(df["Parkinson_Var"] * 365)
            df["AvgVol_21"] = df["Parkinson_Vol"].ewm(span=min(len(df), 21)).mean()

            current_parkinson_vol = df["AvgVol_21"].iloc[-1].round(2)
            vol_percentile = df["AvgVol_21"].rank(pct=True).iloc[-1].round(2)

            return {
                'Ticker': ticker,
                '$Volume': dollar_volume,
                'Sharpe': sharpe,
                'Parkinson': current_parkinson_vol, 
                'ParkinsonPctl': vol_percentile,
            }

        except Exception as e:
            self.logger.error(f"{ticker}: Failed ({e})")
            return None
