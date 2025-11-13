import os
from typing import Set

class BadTickerManager:
    """Handles reading and writing of bad ticker symbols."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.bad_tickers: Set[str] = set()

    def load(self) -> Set[str]:
        if os.path.exists(self.file_path):
            with open(self.file_path, "r") as f:
                self.bad_tickers = {line.strip() for line in f if line.strip()}
        return self.bad_tickers

    def save(self, new_bad_tickers: Set[str]):
        self.bad_tickers.update(new_bad_tickers)
        with open(self.file_path, "w") as f:
            for ticker in sorted(self.bad_tickers):
                f.write(f"{ticker}\n")
