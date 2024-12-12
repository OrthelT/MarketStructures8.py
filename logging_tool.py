import logging
from logging.handlers import RotatingFileHandler


def configure_logging(
        log_name: str = "report_log", log_file: str = "log_report.log"
) -> None:
    # Create logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

    # Configure rotating log handler
    handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)
    logging.basicConfig(
        handlers=[handler],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
