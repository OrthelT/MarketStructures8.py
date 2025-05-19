import os
import logging
from logging.handlers import RotatingFileHandler

def configure_logging(
    log_name: str = "mkt_structures",
    log_file: str = "log_file/mkt_structures.log",
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 5,
    ) -> logging.Logger:
    # create logger (or get existing)
    logger = logging.getLogger(log_name)

    logger.setLevel(logging.INFO)

    # common formatter
    fmt = "%(asctime)s - %(name)s - %(levelname)s %(funcName)s:%(lineno)d > %(message)s"
    formatter = logging.Formatter(fmt)

    # rotating file handler
    rotating_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )
    rotating_handler.setLevel(logging.INFO)
    rotating_handler.setFormatter(formatter)
    logger.addHandler(rotating_handler)

    # console handler (errors only)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
