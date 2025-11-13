import yfinance as yf
import numpy as np
import pandas as pd
from .config import Config

class TickerAnalyzer:
    """Analyzes a single ticker using price and volatility metrics."""

    def __init__(self, logger):
        self.logger = logger

    def analyze(self, ticker: str):
        try:
            dat = yf.Ticker(ticker)
            df = dat.history(
                period=f"{Config.PERIOD_DAYS}d", 
                interval="1d", 
                auto_adjust=True, 
                # progress=False
            )

            print(df.head())

            if df.empty or not {'Open', 'High', 'Low', 'Close', 'Volume'}.issubset(df.columns):
                raise ValueError("No valid OHLCV data")
            
            # --- Price Volume ---
            df['DollarVolume'] = df['Volume'] * df['Close']
            dollar_volume = df['DollarVolume'].iloc[-1]
            # --- Skip illiquid tickers ---
            if dollar_volume < Config.MIN_DOLLAR_VOLUME:
                self.logger.info(f"{ticker}: Skipped (low liquidity, ${dollar_volume:,.0f})")
                return "SKIP"

            # --- Sharpe Ratio ---
            df['Returns'] = np.log(df['Close'] / df['Close'].shift(1))
            mean_return = df['Returns'].mean()
            std_return = df['Returns'].std()
            sharpe = 0 if std_return == 0 else mean_return / std_return

            # --- Parkinson Volatility ---
            df["Parkinson_Var"] = (np.log(df["High"] / df["Low"]) ** 2) / (4.0 * np.log(2.0))
            df["Parkinson_Vol"] = np.sqrt(df["Parkinson_Var"] * 365)
            df["AvgVol_21"] = df["Parkinson_Vol"].ewm(span=min(len(df), 21)).mean()

            current_parkinson_vol = df["AvgVol_21"].iloc[-1].round(2)
            vol_percentile = df["AvgVol_21"].rank(pct=True).iloc[-1].round(2)

            # --- Consecutive New Highs / Lows ---
            df['Prev_High'] = df['High'].shift(1)
            df['Prev_Low'] = df['Low'].shift(1)

            # Compute boolean conditions
            df['IsNewHigh'] = df['High'] > df['Prev_High']
            df['IsNewLow'] = df['Low'] < df['Prev_Low']

            # Running counts for consecutive highs/lows
            df['CountHigh'] = (
                df['IsNewHigh']
                .astype(int)
                .groupby((~df['IsNewHigh']).cumsum())
                .cumsum()
            )
            df['CountLow'] = (
                df['IsNewLow']
                .astype(int)
                .groupby((~df['IsNewLow']).cumsum())
                .cumsum()
            )

            last_high_streak = int(df['CountHigh'].iloc[-1])
            last_low_streak = int(df['CountLow'].iloc[-1])

            if last_high_streak > last_low_streak:
                streak_source = "High"
                streak_count = last_high_streak
            else:
                streak_source = "Low"
                streak_count = last_low_streak

            # --- Return Summary ---
            return {
                'Ticker': ticker,
                '$Volume': dollar_volume,
                'Sharpe': sharpe,
                'Parkinson': current_parkinson_vol,
                'ParkinsonPctl': vol_percentile,
                'StreakCount': streak_count,
                'StreakSource': streak_source,
            }

        except Exception as e:
            self.logger.error(f"{ticker}: Failed ({e})")
            return None