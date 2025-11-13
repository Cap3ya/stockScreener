import os

class Config:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # path to lib/
    PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))  # one level up
    OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

    VALID_SUFFIXES = (" Stock", " Shares")
    LOG_FILE = os.path.join(OUTPUT_DIR, "stockScreener.log")
    CSV_FILE = os.path.join(OUTPUT_DIR, "stockScreener.csv")
    BAD_TICKERS_FILE = os.path.join(OUTPUT_DIR, "stockScreener_badTickers.txt")

    PERIOD_DAYS = 63
    MIN_DOLLAR_VOLUME = 1_000_000  

    @classmethod
    def ensure_output_dir(cls):
        os.makedirs(cls.OUTPUT_DIR, exist_ok=True)
