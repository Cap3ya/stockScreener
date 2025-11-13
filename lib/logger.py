import logging
from .config import Config

class Logger:
    @staticmethod
    def setup() -> logging.Logger:
        Config.ensure_output_dir()
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(Config.LOG_FILE, mode='w')
            ]
        )
        return logging.getLogger("stock_screener")
